package group

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/control"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"

	mDNS "github.com/miekg/dns"
)

type smartProbeTestDialer struct {
	failResolve           bool
	blockResolve          bool
	badFirst              bool
	block                 bool
	failUDP, failFirstUDP bool
	packetStarted         chan struct{}
	resolves              atomic.Int32
	packets               atomic.Int32
	httpDials             atomic.Int32
	httpHandler           func(net.Conn)
}

func (d *smartProbeTestDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if destination.Port != 53 {
		d.httpDials.Add(1)
		if d.httpHandler == nil {
			return nil, errors.New("no TCP endpoint")
		}
		client, server := net.Pipe()
		go func() { defer server.Close(); d.httpHandler(server) }()
		return client, nil
	}
	d.resolves.Add(1)
	if d.blockResolve {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if d.failResolve {
		return nil, errors.New("bootstrap DNS blocked")
	}
	if destination.IsDomain() {
		return nil, errors.New("profile resolver used")
	}
	client, server := net.Pipe()
	go func() {
		defer server.Close()
		conn := &mDNS.Conn{Conn: server}
		for range 2 {
			request, err := conn.ReadMsg()
			if err != nil {
				return
			}
			response := new(mDNS.Msg).SetReply(request)
			if request.Question[0].Qtype == mDNS.TypeA {
				response.Answer = []mDNS.RR{&mDNS.A{Hdr: mDNS.RR_Header{Name: request.Question[0].Name, Rrtype: mDNS.TypeA, Class: mDNS.ClassINET, Ttl: 60}, A: net.IPv4(203, 0, 113, 1)}}
			}
			if err = conn.WriteMsg(response); err != nil {
				return
			}
		}
	}()
	return client, nil
}

func (d *smartProbeTestDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	count := d.packets.Add(1)
	if d.packetStarted != nil {
		close(d.packetStarted)
	}
	return &smartProbeTestPacket{destination: destination, packets: make(chan []byte, 2), done: make(chan struct{}), badFirst: d.badFirst, block: d.block, fail: d.failUDP || d.failFirstUDP && count == 1}, nil
}

type smartProbeTestPacket struct {
	destination     M.Socksaddr
	packets         chan []byte
	done            chan struct{}
	once            sync.Once
	badFirst, block bool
	fail            bool
}

func (p *smartProbeTestPacket) ReadFrom(b []byte) (int, net.Addr, error) {
	if p.fail {
		return 0, nil, errors.New("UDP blocked")
	}
	select {
	case packet := <-p.packets:
		return copy(b, packet), p.destination, nil
	case <-p.done:
		return 0, nil, net.ErrClosed
	}
}

func (p *smartProbeTestPacket) WriteTo(b []byte, _ net.Addr) (int, error) {
	if p.block {
		return len(b), nil
	}
	var reply []byte
	if len(b) == 1200 {
		reply = make([]byte, 27)
		reply[0], reply[5], reply[14] = 0x80, 8, 8
		copy(reply[6:14], b[15:23])
		copy(reply[15:23], b[6:14])
		binary.BigEndian.PutUint32(reply[23:27], 1)
	} else {
		reply = make([]byte, 32)
		copy(reply, b)
		binary.BigEndian.PutUint16(reply[:2], 0x0101)
		binary.BigEndian.PutUint16(reply[2:4], 12)
		binary.BigEndian.PutUint16(reply[20:22], 0x0020)
		binary.BigEndian.PutUint16(reply[22:24], 8)
		reply[25] = 1
	}
	if p.badFirst {
		bad := append([]byte(nil), reply...)
		bad[8] ^= 0xff
		p.packets <- bad
	}
	p.packets <- reply
	return len(b), nil
}

func (p *smartProbeTestPacket) Close() error                     { p.once.Do(func() { close(p.done) }); return nil }
func (p *smartProbeTestPacket) LocalAddr() net.Addr              { return &net.UDPAddr{} }
func (p *smartProbeTestPacket) SetDeadline(time.Time) error      { return nil }
func (p *smartProbeTestPacket) SetReadDeadline(time.Time) error  { return nil }
func (p *smartProbeTestPacket) SetWriteDeadline(time.Time) error { return nil }

func TestSmartTCPResponseExpectations(t *testing.T) {
	cases := []struct {
		operator, status int
		location, body   string
		ok               bool
	}{
		{0, 200, "", "h=1.1.1.1\n", true},
		{0, 200, "", "h=1.1.1.10\n", false},
		{0, 302, "", "h=1.1.1.1\n", false},
		{1, 302, "https://dns.google/", "", true},
		{1, 200, "https://dns.google/", "", false},
		{1, 302, "https://captive.portal/", "", false},
		{2, 404, "", "", true},
	}
	for index, test := range cases {
		t.Run(strconv.Itoa(index), func(t *testing.T) {
			response := &http.Response{StatusCode: test.status, Header: make(http.Header)}
			response.Header.Set("Location", test.location)
			err := smartValidateTCPResponse(test.operator, "1.1.1.1", response, []byte(test.body))
			if (err == nil) != test.ok {
				t.Fatalf("response validation: %v", err)
			}
		})
	}
	client := smartProbeHTTPClient(&smartProbeTestDialer{})
	if err := client.CheckRedirect(nil, nil); err != http.ErrUseLastResponse {
		t.Fatal("probe redirects enabled")
	}
	if transport := client.Transport.(*http.Transport); transport.TLSClientConfig != nil && transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("TLS validation disabled")
	}
}

func TestSmartUDPValidatesIDsAndIndependentResolution(t *testing.T) {
	for _, quic := range []bool{false, true} {
		dialer := &smartProbeTestDialer{badFirst: true}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := smartUDPProbe(ctx, dialer, quic)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if dialer.resolves.Load() != 1 || dialer.packets.Load() != 1 {
			t.Fatal("probe bypassed member DNS or failed to ignore wrong IDs")
		}
	}
}

func TestSmartResolverFailuresAndCancellationAreNeutral(t *testing.T) {
	dialer := &smartProbeTestDialer{failResolve: true}
	if _, err := smartUDPProbe(context.Background(), dialer, false); !errors.Is(err, errSmartResolve) {
		t.Fatalf("resolution not tagged neutral: %v", err)
	}
	if dialer.packets.Load() != 0 || dialer.resolves.Load() != 6 {
		t.Fatal("resolution skipped independent operators")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := smartTCPProbe(ctx, dialer, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("TCP cancellation: %v", err)
	}
	if _, err := smartUDPProbe(ctx, dialer, true); !errors.Is(err, context.Canceled) {
		t.Fatalf("UDP cancellation: %v", err)
	}
	if _, err := smartDirectProbe(ctx, "RU", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("direct cancellation: %v", err)
	}
}

func TestSmartUDPResolverDeadlineRemainsNeutral(t *testing.T) {
	for _, test := range []struct {
		kind string
		quic bool
	}{{"udp", false}, {"udp443", true}} {
		t.Run(test.kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			dialer := &smartProbeTestDialer{blockResolve: true}
			_, err := smartUDPProbe(ctx, dialer, test.quic)
			if !errors.Is(err, errSmartResolve) || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("resolver deadline lost neutral classification: %v", err)
			}
			if dialer.resolves.Load() == 0 || dialer.packets.Load() != 0 {
				t.Fatal("resolver deadline did not remain in DNS")
			}
			s, clock := smartTestGroup()
			s.applyResultLocked(smartResult{member: s.primary, generation: s.generation, memberGeneration: s.primary.generation, configGeneration: s.configGeneration, kind: test.kind, err: err}, clock.now)
			if s.primary.udp.value(clock.now) != "unknown" || s.primary.udp443.value(clock.now) != "unknown" {
				t.Fatal("resolver deadline produced negative UDP evidence")
			}
		})
	}
}

func TestSmartUDPDatagramTimeoutsRemainNegative(t *testing.T) {
	for _, test := range []struct {
		kind string
		quic bool
	}{{"udp", false}, {"udp443", true}} {
		t.Run(test.kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			dialer := &smartProbeTestDialer{block: true}
			_, err := smartUDPProbe(ctx, dialer, test.quic)
			if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errSmartResolve) {
				t.Fatalf("datagram timeout lost negative classification: %v", err)
			}
			if dialer.packets.Load() != 2 {
				t.Fatal("datagram timeout was not corroborated")
			}
			s, clock := smartTestGroup()
			s.applyResultLocked(smartResult{member: s.primary, generation: s.generation, memberGeneration: s.primary.generation, configGeneration: s.configGeneration, kind: test.kind, err: err}, clock.now)
			evidence := s.primary.udp
			if test.quic {
				evidence = s.primary.udp443
			}
			if evidence.value(clock.now) != "failed" {
				t.Fatal("corroborated datagram timeout did not produce negative evidence")
			}
		})
	}
}

func TestSmartUDPClosesBlockedSocketOnCancellation(t *testing.T) {
	dialer := &smartProbeTestDialer{block: true, packetStarted: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := smartUDPExchange(ctx, dialer, M.ParseSocksaddr("203.0.113.1:443"), true)
		done <- err
	}()
	<-dialer.packetStarted
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled probe kept socket open")
	}
}

func TestSmartUDPNegativeRequiresBothEndpoints(t *testing.T) {
	for _, quic := range []bool{false, true} {
		oneFailed := &smartProbeTestDialer{failFirstUDP: true}
		if _, err := smartUDPProbe(context.Background(), oneFailed, quic); err != nil {
			t.Fatalf("single endpoint failure became negative: %v", err)
		}
		if oneFailed.packets.Load() != 2 {
			t.Fatal("fallback endpoint not checked")
		}
		bothFailed := &smartProbeTestDialer{failUDP: true}
		if _, err := smartUDPProbe(context.Background(), bothFailed, quic); err == nil || errors.Is(err, errSmartResolve) {
			t.Fatalf("corroborated failure not exposed: %v", err)
		}
		if bothFailed.packets.Load() != 2 {
			t.Fatal("negative without corroboration")
		}
	}
}

func TestSmartDNSRejectsWrongQuestionAndID(t *testing.T) {
	request := new(mDNS.Msg).SetQuestion("probe.example.", mDNS.TypeA)
	response := new(mDNS.Msg).SetReply(request)
	response.Id++
	if _, err := smartDNSAddresses(request, response); err == nil {
		t.Fatal("wrong ID accepted")
	}
	response.Id = request.Id
	response.Question = []mDNS.Question{{Name: "other.example.", Qtype: mDNS.TypeA, Qclass: mDNS.ClassINET}}
	if _, err := smartDNSAddresses(request, response); err == nil {
		t.Fatal("wrong question accepted")
	}
}

func TestSmartMeasureCountsInnerBytesAndCapsOverhead(t *testing.T) {
	dialer := &smartProbeTestDialer{httpHandler: func(conn net.Conn) {
		var buffer [4096]byte
		_, _ = conn.Read(buffer[:])
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 2000000\r\nConnection: close\r\n\r\n")
		_, _ = io.CopyN(conn, smartZeroReader{}, 2_000_000)
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	download, _, charged, err := smartMeasure(ctx, dialer, "http://203.0.113.1/down?bytes={bytes}", "", 1_100_000, false)
	if err != nil || download <= 0 {
		t.Fatalf("measurement failed: %v (%f Mbps)", err, download)
	}
	if charged <= 0 || charged > 1_210_000 {
		t.Fatalf("invalid overhead accounting: %d", charged)
	}
	if dialer.httpDials.Load() > 2 {
		t.Fatal("download connection limit exceeded")
	}
}

type smartProbeDisappearingNetworkManager struct {
	adapter.NetworkManager
	iface *adapter.NetworkInterface
	reads int
}

func (m *smartProbeDisappearingNetworkManager) DefaultNetworkInterface() *adapter.NetworkInterface {
	m.reads++
	iface := m.iface
	m.iface = nil
	return iface
}

func TestSmartDirectProbeSnapshotsDefaultInterface(t *testing.T) {
	manager := &smartProbeDisappearingNetworkManager{iface: &adapter.NetworkInterface{Interface: control.Interface{Name: "wlan0"}}}
	ctx := service.ContextWith[adapter.NetworkManager](context.Background(), manager)
	if _, err := smartDirectProbe(ctx, "", nil); !errors.Is(err, errSmartResolve) {
		t.Fatalf("probe did not advance to bootstrap resolution: %v", err)
	}
	if manager.reads != 1 {
		t.Fatalf("default interface read %d times", manager.reads)
	}
}

func TestSmartDirectProbeMissingInterfaceIsNeutral(t *testing.T) {
	for name, ctx := range map[string]context.Context{
		"missing manager":   context.Background(),
		"missing interface": service.ContextWith[adapter.NetworkManager](context.Background(), &smartProbeDisappearingNetworkManager{}),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := smartDirectProbe(ctx, "", nil)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("link loss not tagged neutral: %v", err)
			}
			s, clock := smartTestGroup()
			s.applyResultLocked(smartResult{generation: s.generation, configGeneration: s.configGeneration, kind: "direct", err: err}, clock.now)
			if s.direct.value(clock.now) != "unknown" || !s.ambiguousSince.IsZero() {
				t.Fatal("missing interface produced negative direct evidence")
			}
		})
	}
}

func TestSmartDirectListsAndCoarseResolution(t *testing.T) {
	if got := smartDirectURLs("RU"); len(got) != 8 || got[0] != "https://ya.ru/" {
		t.Fatal("RU list changed")
	}
	for _, country := range []string{"RU", "CN", "IR", ""} {
		urls := smartDirectURLs(country)
		if len(urls) < 2 {
			t.Fatal("cannot corroborate direct failures")
		}
		for _, url := range urls {
			if !strings.HasPrefix(url, "https://") {
				t.Fatal("direct TLS omitted")
			}
		}
	}
	addresses, err := smartResolveMember(context.Background(), &smartProbeTestDialer{failResolve: true}, "1.1.1.1")
	if err != nil || len(addresses) != 1 || addresses[0] != netip.MustParseAddr("1.1.1.1") {
		t.Fatal("IP literal resolved through DNS")
	}
}

func TestSmartMeasureRampsAndUploadsOneMB(t *testing.T) {
	var downloaded, uploaded atomic.Int64
	var firstDownload atomic.Int64
	dialer := &smartProbeTestDialer{httpHandler: func(conn net.Conn) {
		reader := bufio.NewReader(conn)
		for {
			request, err := http.ReadRequest(reader)
			if err != nil {
				return
			}
			if request.Method == http.MethodPost {
				n, copyErr := io.Copy(io.Discard, request.Body)
				_ = request.Body.Close()
				if copyErr != nil {
					return
				}
				uploaded.Add(n)
				_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
				continue
			}
			size, parseErr := strconv.ParseInt(request.URL.Query().Get("bytes"), 10, 64)
			if parseErr != nil || size <= 0 {
				return
			}
			firstDownload.CompareAndSwap(0, size)
			downloaded.Add(size)
			if _, err = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: "+strconv.FormatInt(size, 10)+"\r\n\r\n"); err != nil {
				return
			}
			if _, err = io.CopyN(conn, smartZeroReader{}, size); err != nil {
				return
			}
		}
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	download, upload, charged, err := smartMeasure(ctx, dialer, "http://203.0.113.1/down?bytes={bytes}", "http://203.0.113.1/up", 4_000_000, true)
	if err != nil || download <= 0 || upload <= 0 {
		t.Fatalf("measurement failed: %v (%f/%f Mbps)", err, download, upload)
	}
	if firstDownload.Load() != 1_000_000 || downloaded.Load() <= 1_000_000 || uploaded.Load() != 1_000_000 {
		t.Fatalf("ramp/upload sizes: %d/%d/%d", firstDownload.Load(), downloaded.Load(), uploaded.Load())
	}
	if charged < (downloaded.Load()+uploaded.Load())*11/10 || charged > 4_400_000 {
		t.Fatalf("inner byte accounting: %d", charged)
	}
	if dialer.httpDials.Load() > 2 {
		t.Fatal("unnecessary download connections")
	}
}
