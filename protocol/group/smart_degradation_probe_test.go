package group

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

// Shared by ledger fixtures; transport measurements no longer use quota clocks.
func smartDegradationTestClock() *smartFakeClock {
	return &smartFakeClock{now: time.Unix(1800000000, 0), changed: make(chan struct{}, 8)}
}

type smartMeasurementEdgeConn struct {
	net.Conn
	read  func([]byte) (int, error)
	write func([]byte) (int, error)
}

func (c smartMeasurementEdgeConn) Read(p []byte) (int, error)  { return c.read(p) }
func (c smartMeasurementEdgeConn) Write(p []byte) (int, error) { return c.write(p) }
func (c smartMeasurementEdgeConn) Close() error                { return nil }

func TestSmartMeasurementConnectionMeterEdges(t *testing.T) {
	t.Run("empty operations do not exhaust allowance", func(t *testing.T) {
		meter := newSmartMeasureMeter(0)
		conn := &smartMeasureConn{meter: meter, ctx: context.Background()}
		if n, err := conn.Read(nil); n != 0 || err != nil {
			t.Fatalf("empty read: %d %v", n, err)
		}
		if n, err := conn.Write(nil); n != 0 || err != nil {
			t.Fatalf("empty write: %d %v", n, err)
		}
		if meter.tripped {
			t.Fatal("empty I/O tripped budget")
		}
	})
	t.Run("short read refunds unused reservation", func(t *testing.T) {
		meter := newSmartMeasureMeter(110)
		conn := &smartMeasureConn{meter: meter, ctx: context.Background(), Conn: smartMeasurementEdgeConn{read: func([]byte) (int, error) { return 3, io.EOF }}}
		if n, err := conn.Read(make([]byte, 50)); n != 3 || err != io.EOF {
			t.Fatalf("read: %d %v", n, err)
		}
		if meter.available() != 97 || meter.chargedBytes() != 4 {
			t.Fatalf("refund: raw=%d charged=%d", meter.available(), meter.chargedBytes())
		}
	})
	t.Run("budget short write is never success", func(t *testing.T) {
		meter := newSmartMeasureMeter(11)
		conn := &smartMeasureConn{meter: meter, ctx: context.Background(), Conn: smartMeasurementEdgeConn{write: func(p []byte) (int, error) { return len(p), nil }}}
		if n, err := conn.Write(make([]byte, 11)); n != 10 || !errors.Is(err, errSmartMeasureCap) {
			t.Fatalf("short budget write: %d %v", n, err)
		}
		if meter.chargedBytes() != 11 || !meter.exhausted() {
			t.Fatal("exact charged boundary lost")
		}
	})
	t.Run("underlying short write remains an I/O error", func(t *testing.T) {
		meter := newSmartMeasureMeter(110)
		conn := &smartMeasureConn{meter: meter, ctx: context.Background(), Conn: smartMeasurementEdgeConn{write: func([]byte) (int, error) { return 2, nil }}}
		if n, err := conn.Write(make([]byte, 10)); n != 2 || !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("short I/O write: %d %v", n, err)
		}
		if meter.available() != 98 || meter.chargedBytes() != 3 {
			t.Fatal("short write failed to settle actual bytes")
		}
	})
	t.Run("panic refunds pending allowance", func(t *testing.T) {
		meter := newSmartMeasureMeter(110)
		conn := &smartMeasureConn{meter: meter, ctx: context.Background(), Conn: smartMeasurementEdgeConn{read: func([]byte) (int, error) { panic("meter read") }}}
		if _, err := conn.Read(make([]byte, 50)); !errors.Is(err, errSmartContained) {
			t.Fatalf("panic escaped: %v", err)
		}
		if meter.available() != 100 || meter.chargedBytes() != 0 {
			t.Fatal("panic leaked pending reservation")
		}
	})
}

func TestSmartMeasurementSharedAllowanceChargedBoundaries(t *testing.T) {
	for _, charged := range []int64{1, 10, 11, 12, 50_000_000, 300_000_000, 1_000_000_000, 5_000_000_000} {
		meter := newSmartMeasureMeter(charged)
		var workers sync.WaitGroup
		for range 16 {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for {
					allowed := meter.allowance(1_000_000)
					if allowed == 0 {
						return
					}
					meter.account(allowed, allowed)
				}
			}()
		}
		workers.Wait()
		if meter.chargedBytes() > charged || meter.available() != 0 || !meter.exhausted() {
			t.Fatalf("charged=%d actual=%d raw remaining=%d", charged, meter.chargedBytes(), meter.available())
		}
		if charged >= 11 && meter.chargedBytes() < charged-1 {
			t.Fatalf("allowance cut off early: %d of %d", meter.chargedBytes(), charged)
		}
	}
}

type smartDegradationPanicDialer struct{ *smartProbeTestDialer }

func (d smartDegradationPanicDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	conn, err := d.smartProbeTestDialer.DialContext(ctx, network, destination)
	if err != nil {
		return nil, err
	}
	return &smartDegradationPanicConn{Conn: conn, remaining: 1_100_000}, nil
}

type smartDegradationPanicConn struct {
	net.Conn
	remaining int64
}

func (c *smartDegradationPanicConn) Read(p []byte) (int, error) {
	if c.remaining == 0 {
		panic("measurement payload read")
	}
	n, err := c.Conn.Read(p[:min(int64(len(p)), c.remaining)])
	c.remaining -= int64(n)
	return n, err
}

func TestSmartMeasurementContainedReadSettlesBytes(t *testing.T) {
	dialer := smartDegradationPanicDialer{&smartProbeTestDialer{httpHandler: func(conn net.Conn) {
		reader := bufio.NewReader(conn)
		for {
			request, err := http.ReadRequest(reader)
			if err != nil {
				return
			}
			_ = request.Body.Close()
			_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\n")
			if _, err = io.CopyN(conn, smartZeroReader{}, 1_000_000); err != nil {
				return
			}
		}
	}}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	meter := newSmartMeasureMeter(10_000_000)
	result, err := smartMeasure(ctx, dialer, "http://203.0.113.1/down?bytes={bytes}", "", meter, smartMeasurementPlan{Download: smartTestPhase(1)}, smartRealClock{})
	if result.Download.Valid || !errors.Is(err, errSmartContained) || meter.chargedBytes() < 1_210_000 || meter.chargedBytes() > 10_000_000 {
		t.Fatalf("contained read lost settlement: %+v charged=%d err=%v", result, meter.chargedBytes(), err)
	}
}

func TestSmartMeasurementBudgetAbortDiscardsCompletedDownload(t *testing.T) {
	t.Parallel()
	server := &smartMeasurementServer{downRate: func(time.Duration) float64 { return 20 }, upRate: func(int, int) float64 { return 1 }}
	meter := newSmartMeasureMeter(17_000_000)
	plan := smartMeasurementPlan{Download: smartTestPhase(1), Upload: smartTestPhase(1)}
	ctx, cancel := context.WithTimeout(context.Background(), plan.Deadline())
	defer cancel()
	result, err := smartMeasure(ctx, server.dialer(), "http://203.0.113.1/down?bytes={bytes}", "http://203.0.113.1/up?bytes={bytes}", meter, plan, smartRealClock{})
	if !errors.Is(err, errSmartMeasureCap) || result.Download.Valid || result.Upload.Valid || !meter.exhausted() || meter.chargedBytes() > 17_000_000 {
		t.Fatalf("budget abort published partial plan: %+v charged=%d err=%v", result, meter.chargedBytes(), err)
	}
}
