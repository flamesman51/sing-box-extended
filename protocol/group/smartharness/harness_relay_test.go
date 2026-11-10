//go:build linux && with_wireguard && with_gvisor

package smartharness_test

import (
	"context"
	"math/rand/v2"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type relayFaults struct {
	bytesPerSecond int
	delay          time.Duration
	loss           float64
	blackhole      bool
	maxDatagram    int
	downstream     *sharedDownstream
}

type relayPacket struct {
	body *[65536]byte
	size int
	to   netip.AddrPort
	at   time.Time
}

// A single queueing clock accounts for downstream packets from every member.
type sharedDownstream struct {
	mu             sync.Mutex
	bytesPerSecond int
	next           time.Time
}

func (s *sharedDownstream) reserve(size int) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.next = maxTime(s.next, now).Add(time.Duration(float64(size) / float64(s.bytesPerSecond) * float64(time.Second)))
	return s.next.Sub(now)
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// Each direction has an independent token bucket, as on a full-duplex link.
type faultRelay struct {
	conn      *net.UDPConn
	mu        sync.RWMutex
	faults    relayFaults
	workers   sync.WaitGroup
	forwarded atomic.Int64
	dropped   atomic.Int64
	buffers   sync.Pool
}

func newFaultRelay(t *testing.T, server *net.UDPAddr, faults relayFaults) *faultRelay {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &faultRelay{conn: conn, faults: faults}
	r.buffers.New = func() any { return new([65536]byte) }
	serverAddress := netip.AddrPortFrom(server.AddrPort().Addr().Unmap(), uint16(server.Port))
	// Bound FIFO delay to about 220 ms at the slowest fixture rate.
	up, down := make(chan relayPacket, 32), make(chan relayPacket, 32)
	r.workers.Add(3)
	go r.forward(ctx, up, 1, false)
	go r.forward(ctx, down, 2, true)
	go func() {
		defer r.workers.Done()
		var client netip.AddrPort
		for {
			buffer := r.buffers.Get().(*[65536]byte)
			n, from, err := conn.ReadFromUDPAddrPort(buffer[:])
			if err != nil {
				r.buffers.Put(buffer)
				return
			}
			queue, to := up, serverAddress
			if from == serverAddress {
				queue, to = down, client
			} else {
				client = from
			}
			if !to.IsValid() {
				r.buffers.Put(buffer)
				continue
			}
			packet := relayPacket{body: buffer, size: n, to: to, at: time.Now()}
			select {
			case queue <- packet:
			default:
				r.buffers.Put(buffer)
				r.dropped.Add(1)
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		_ = conn.Close()
		r.workers.Wait()
	})
	return r
}

func (r *faultRelay) set(faults relayFaults) {
	r.mu.Lock()
	r.faults = faults
	r.mu.Unlock()
}

func (r *faultRelay) current() relayFaults {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.faults
}

func (r *faultRelay) forward(ctx context.Context, packets <-chan relayPacket, seed int64, downstream bool) {
	defer r.workers.Done()
	random := rand.New(rand.NewPCG(uint64(seed), 0))
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	if !timer.Stop() {
		<-timer.C
	}
	last, tokens := time.Now(), float64(16*1024)
	for {
		select {
		case <-ctx.Done():
			return
		case packet := <-packets:
			faults := r.current()
			if faults.blackhole || faults.maxDatagram > 0 && packet.size > faults.maxDatagram || random.Float64() < faults.loss {
				r.dropped.Add(1)
				r.buffers.Put(packet.body)
				continue
			}
			wait := max(time.Duration(0), time.Until(packet.at.Add(faults.delay)))
			if faults.bytesPerSecond > 0 {
				now := time.Now()
				tokens = min(float64(16*1024), tokens+now.Sub(last).Seconds()*float64(faults.bytesPerSecond))
				last = now
				tokens -= float64(packet.size)
				if tokens < 0 {
					wait = max(wait, time.Duration(-tokens/float64(faults.bytesPerSecond)*float64(time.Second)))
				}
			}
			if downstream && faults.downstream != nil {
				wait = max(wait, faults.downstream.reserve(packet.size))
			}
			if wait > 0 {
				timer.Reset(wait)
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
				}
			}
			if r.current().blackhole {
				r.dropped.Add(1)
				r.buffers.Put(packet.body)
				continue
			}
			if n, err := r.conn.WriteToUDPAddrPort(packet.body[:packet.size], packet.to); err == nil {
				r.forwarded.Add(int64(n))
			}
			r.buffers.Put(packet.body)
		}
	}
}

func TestSmartHarnessRelayFaults(t *testing.T) {
	echo, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		var buffer [65536]byte
		for {
			n, from, readErr := echo.ReadFromUDP(buffer[:])
			if readErr != nil {
				return
			}
			_, _ = echo.WriteToUDP(buffer[:n], from)
		}
	}()
	t.Cleanup(func() { _ = echo.Close(); <-done })
	relay := newFaultRelay(t, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: echo.LocalAddr().(*net.UDPAddr).Port}, relayFaults{})
	client, err := net.DialUDP("udp4", nil, relay.conn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for _, test := range []struct {
		name    string
		faults  relayFaults
		size    int
		blocked bool
		minimum time.Duration
	}{
		{name: "pass", size: 32},
		{name: "delay", faults: relayFaults{delay: 20 * time.Millisecond}, size: 32, minimum: 40 * time.Millisecond},
		{name: "rate", faults: relayFaults{bytesPerSecond: 100_000}, size: 30_000, minimum: 100 * time.Millisecond},
		{name: "loss", faults: relayFaults{loss: 1}, size: 32, blocked: true},
		{name: "blackhole", faults: relayFaults{blackhole: true}, size: 32, blocked: true},
		{name: "mtu", faults: relayFaults{maxDatagram: 31}, size: 32, blocked: true},
		{name: "restored", size: 32},
	} {
		t.Run(test.name, func(t *testing.T) {
			relay.set(test.faults)
			_ = client.SetDeadline(time.Now().Add(500 * time.Millisecond))
			start := time.Now()
			if _, err := client.Write(make([]byte, test.size)); err != nil {
				t.Fatal(err)
			}
			buffer := make([]byte, test.size)
			n, err := client.Read(buffer)
			if test.blocked {
				if err == nil {
					t.Fatal("fault did not drop datagram")
				}
			} else if err != nil || n != test.size || time.Since(start) < test.minimum {
				t.Fatalf("size=%d error=%v elapsed=%v minimum=%v", n, err, time.Since(start), test.minimum)
			}
		})
	}
}
