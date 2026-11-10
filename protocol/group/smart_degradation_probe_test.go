package group

import (
	"bufio"
	"context"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

func TestSmartDegradationConfirmationCap(t *testing.T) {
	for _, test := range []struct {
		target float64
		want   int64
	}{
		{25, 10_000_000},
		{1, 2_000_000},
		{50, 19_000_000},
		{100, 25_000_000},
		{0, 0},
		{-1, 0},
		{math.NaN(), 0},
	} {
		if got := smartDegradationConfirmationCap(test.target); got != test.want {
			t.Fatalf("cap(%v) = %d, want %d", test.target, got, test.want)
		}
	}
}

type smartDegradationTimedReader struct {
	clock              *smartFakeClock
	warmup             int64
	warmupElapsed      time.Duration
	read               int64
	elapsed            time.Duration
	intervals          []time.Duration
	passive            *int64
	perRead            int64
	passivePerInterval []int64
	failAt             int64
	failure            error
}

func (r *smartDegradationTimedReader) Read(p []byte) (int, error) {
	if r.failure != nil && r.read >= r.failAt {
		return 0, r.failure
	}
	n := len(p)
	if r.warmup > 0 {
		n = int(min(int64(n), r.warmup))
		r.warmup -= int64(n)
		r.clock.Advance(time.Duration(n) * r.warmupElapsed / 1_000_000)
	} else {
		elapsed := r.elapsed
		if len(r.intervals) > 0 {
			elapsed = r.intervals[(r.read-1_000_000)/1_953_125]
		}
		r.clock.Advance(time.Duration(n) * elapsed / 1_953_125)
		if r.passive != nil {
			multiplier := r.perRead
			if len(r.passivePerInterval) > 0 {
				multiplier = r.passivePerInterval[(r.read-1_000_000)/1_953_125]
			}
			*r.passive += int64(n) * multiplier
		}
	}
	r.read += int64(n)
	return n, nil
}

func smartDegradationTestClock() *smartFakeClock {
	return &smartFakeClock{now: time.Unix(1800000000, 0), changed: make(chan struct{}, 8)}
}

func TestSmartDegradationBurstDoesNotHideSlowPayload(t *testing.T) {
	clock := smartDegradationTestClock()
	reader := &smartDegradationTimedReader{clock: clock, warmup: 1_000_000, elapsed: 5208333333 * time.Nanosecond}
	rate, err := smartMeasureDegradationBody(context.Background(), reader, 25, 25*.5, nil, clock)
	if err != nil || math.Abs(rate-3) > 0.001 {
		t.Fatalf("startup burst hid 3 Mbps payload: rate=%f err=%v", rate, err)
	}
	if reader.read >= 2_953_125 || clock.Now().Sub(time.Unix(1800000000, 0)) > 2100*time.Millisecond {
		t.Fatalf("slow proof waited for a complete fast quota: bytes=%d elapsed=%s", reader.read, clock.Now().Sub(time.Unix(1800000000, 0)))
	}
}

func TestSmartDegradationSharedLinkUsesIdenticalIntervals(t *testing.T) {
	clock := smartDegradationTestClock()
	passive := int64(900_000_000)
	reader := &smartDegradationTimedReader{clock: clock, warmup: 1_000_000, elapsed: time.Second, passive: &passive, perRead: 1}
	start := clock.Now()
	rate, err := smartMeasureDegradationBody(context.Background(), reader, 25, 25*.5, func() int64 { return passive }, clock)
	if err != nil || math.Abs(rate-31.25) > 0.001 {
		t.Fatalf("same-interval correction: rate=%f err=%v", rate, err)
	}
	if elapsed := clock.Now().Sub(start); elapsed < 3999*time.Millisecond || elapsed > 4*time.Second {
		t.Fatalf("wrong evidence duration: %v", elapsed)
	}
	if reader.read != 8_812_500 {
		t.Fatalf("wrong proof payload: %d", reader.read)
	}
}

func TestSmartDegradationFastProofWaitsTwoSeconds(t *testing.T) {
	clock := smartDegradationTestClock()
	reader := &smartDegradationTimedReader{clock: clock, warmup: 1_000_000}
	type result struct {
		rate float64
		err  error
	}
	done := make(chan result, 1)
	start := clock.Now()
	go func() {
		rate, err := smartMeasureDegradationBody(context.Background(), reader, 25, 25*.5, nil, clock)
		done <- result{rate, err}
	}()
	for interval := range 4 {
		select {
		case <-clock.changed:
		case <-time.After(time.Second):
			t.Fatal("proof did not create minimum-duration timer")
		}
		select {
		case got := <-done:
			t.Fatalf("fast result before interval %d finished: %+v", interval, got)
		default:
		}
		clock.Advance(499 * time.Millisecond)
		select {
		case got := <-done:
			t.Fatalf("fast result before minimum duration: %+v", got)
		default:
		}
		clock.Advance(time.Millisecond)
	}
	select {
	case got := <-done:
		if got.err != nil || got.rate != 31.25 || clock.Now().Sub(start) != 2*time.Second {
			t.Fatalf("invalid sustained proof: %+v, elapsed=%v", got, clock.Now().Sub(start))
		}
	case <-time.After(time.Second):
		t.Fatal("completed proof did not return")
	}
}

func TestSmartDegradationIncompleteEvidenceIsNoData(t *testing.T) {
	for _, failure := range []error{io.EOF, errSmartMeasureCap, errSmartContained} {
		clock := smartDegradationTestClock()
		reader := &smartDegradationTimedReader{clock: clock, warmup: 1_000_000, elapsed: time.Second, failAt: 1_100_000, failure: failure}
		rate, err := smartMeasureDegradationBody(context.Background(), reader, 25, 25*.5, nil, clock)
		if rate != 0 || !errors.Is(err, errSmartDegradationNoData) || !errors.Is(err, failure) {
			t.Fatalf("partial proof became evidence: rate=%f err=%v", rate, err)
		}
	}
}

func TestSmartDegradationCancelMinimumDurationWait(t *testing.T) {
	clock := smartDegradationTestClock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := &smartDegradationTimedReader{clock: clock, warmup: 1_000_000}
	done := make(chan error, 1)
	go func() {
		_, err := smartMeasureDegradationBody(ctx, reader, 25, 25*.5, nil, clock)
		done <- err
	}()
	select {
	case <-clock.changed:
	case <-time.After(time.Second):
		t.Fatal("proof did not start cancellable wait")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("wait cancellation lost: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled proof kept waiting")
	}
}

type smartDegradationAutoClock struct{ *smartFakeClock }

func (c smartDegradationAutoClock) NewTimer(d time.Duration) smartTimer {
	timer := c.smartFakeClock.NewTimer(d)
	c.Advance(d)
	return timer
}

func TestSmartDegradationHTTPProofAndAccounting(t *testing.T) {
	dialer := &smartProbeTestDialer{httpHandler: func(conn net.Conn) {
		defer conn.Close()
		request, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			return
		}
		if request.Method != http.MethodGet || request.URL.Query().Get("bytes") != "8812500" {
			t.Errorf("wrong degradation request: %s %s", request.Method, request.URL)
			return
		}
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 8812500\r\nConnection: close\r\n\r\n")
		_, _ = io.CopyN(conn, smartZeroReader{}, 8_812_500)
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	clock := smartDegradationAutoClock{smartDegradationTestClock()}
	rate, charged, err := smartMeasureDegradation(ctx, dialer, "http://203.0.113.1/down?bytes={bytes}", 10_000_000, 25, 25*.5, nil, clock)
	if err != nil || rate != 31.25 {
		t.Fatalf("HTTP proof failed: rate=%f charged=%d err=%v", rate, charged, err)
	}
	if charged <= 9_693_750 || charged > 11_000_000 || dialer.httpDials.Load() != 1 {
		t.Fatalf("wrong HTTP accounting or request count: charged=%d dials=%d", charged, dialer.httpDials.Load())
	}
}

func TestSmartDegradationHTTPIncompleteAndCancelledAccounting(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(strconv.FormatBool(cancelled), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			dialer := &smartProbeTestDialer{httpHandler: func(conn net.Conn) {
				defer conn.Close()
				if _, err := http.ReadRequest(bufio.NewReader(conn)); err != nil {
					return
				}
				_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 8812500\r\nConnection: close\r\n\r\n")
				_, _ = io.CopyN(conn, smartZeroReader{}, 1_100_000)
				if cancelled {
					cancel()
				}
			}}
			rate, charged, err := smartMeasureDegradation(ctx, dialer, "http://203.0.113.1/down?bytes={bytes}", 10_000_000, 25, 25*.5, nil, smartDegradationAutoClock{smartDegradationTestClock()})
			if rate != 0 || charged <= 0 || charged > 11_000_000 {
				t.Fatalf("partial HTTP evidence/accounting: rate=%f charged=%d err=%v", rate, charged, err)
			}
			if cancelled && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation lost: %v", err)
			}
			if !cancelled && !errors.Is(err, errSmartDegradationNoData) {
				t.Fatalf("truncated proof was not neutral: %v", err)
			}
		})
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
		panic("degradation payload read")
	}
	n, err := c.Conn.Read(p[:min(int64(len(p)), c.remaining)])
	c.remaining -= int64(n)
	return n, err
}

func TestSmartDegradationContainedReadSettlesBytes(t *testing.T) {
	dialer := smartDegradationPanicDialer{&smartProbeTestDialer{httpHandler: func(conn net.Conn) {
		defer conn.Close()
		if _, err := http.ReadRequest(bufio.NewReader(conn)); err != nil {
			return
		}
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 8812500\r\nConnection: close\r\n\r\n")
		_, _ = io.CopyN(conn, smartZeroReader{}, 8_812_500)
	}}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rate, charged, err := smartMeasureDegradation(ctx, dialer, "http://203.0.113.1/down?bytes={bytes}", 10_000_000, 25, 25*.5, nil, smartDegradationAutoClock{smartDegradationTestClock()})
	if rate != 0 || !errors.Is(err, errSmartContained) || charged < 1_210_000 || charged > 11_000_000 {
		t.Fatalf("contained read lost settlement: rate=%f charged=%d err=%v", rate, charged, err)
	}
}

func TestSmartDegradationRefusesUndersizedProof(t *testing.T) {
	dialer := &smartProbeTestDialer{}
	for _, test := range []struct {
		cap    int64
		target float64
	}{{8_000_000, 25}, {25_000_000, 100}} {
		rate, charged, err := smartMeasureDegradation(context.Background(), dialer, "http://203.0.113.1/down?bytes={bytes}", test.cap, test.target, test.target*.5, nil, smartDegradationTestClock())
		if rate != 0 || charged != 0 || !errors.Is(err, errSmartDegradationNoData) {
			t.Fatalf("undersized proof was shrunk: rate=%f charged=%d err=%v", rate, charged, err)
		}
	}
	if dialer.httpDials.Load() != 0 {
		t.Fatal("infeasible proof spent network bytes")
	}
}

func TestSmartDegradationLaterSlowIntervalCannotHideBehindBurst(t *testing.T) {
	clock := smartDegradationTestClock()
	reader := &smartDegradationTimedReader{
		clock: clock, warmup: 1_000_000,
		intervals: []time.Duration{500 * time.Millisecond, 500 * time.Millisecond, 500 * time.Millisecond, time.Second},
	}
	rate, err := smartMeasureDegradationBody(context.Background(), reader, 25, 25*.5, nil, smartDegradationAutoClock{clock})
	if err != nil || math.Abs(rate-15.625) > 0.001 || reader.read != 8_812_500 {
		t.Fatalf("earlier intervals hid later slowness: rate=%f read=%d err=%v", rate, reader.read, err)
	}
}

func TestSmartDegradationFramingCapIsNoDataAndSettlesBytes(t *testing.T) {
	dialer := &smartProbeTestDialer{httpHandler: func(conn net.Conn) {
		defer conn.Close()
		if _, err := http.ReadRequest(bufio.NewReader(conn)); err != nil {
			return
		}
		if _, err := io.WriteString(conn, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"); err != nil {
			return
		}
		chunks := strings.Repeat("1\r\nx\r\n", 1024)
		for {
			if _, err := io.WriteString(conn, chunks); err != nil {
				return
			}
		}
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rate, charged, err := smartMeasureDegradation(ctx, dialer, "http://203.0.113.1/down?bytes={bytes}", 2_000_000, 1, 1*.5, nil, smartDegradationAutoClock{smartDegradationTestClock()})
	if rate != 0 || charged != 2_200_000 || !errors.Is(err, errSmartDegradationNoData) || !errors.Is(err, errSmartMeasureCap) {
		t.Fatalf("framing cap became evidence or lost bytes: rate=%f charged=%d err=%v", rate, charged, err)
	}
}

func TestSmartDegradationDoesNotReuseEarlierPassiveBytes(t *testing.T) {
	clock := smartDegradationTestClock()
	passive := int64(900_000_000)
	reader := &smartDegradationTimedReader{
		clock: clock, warmup: 1_000_000, elapsed: time.Second, passive: &passive,
		passivePerInterval: []int64{3, 1, 1, 0},
	}
	rate, err := smartMeasureDegradationBody(context.Background(), reader, 25, 25*.5, func() int64 { return passive }, clock)
	if err != nil || math.Abs(rate-15.625) > 0.001 {
		t.Fatalf("earlier passive bytes hid final slow interval: rate=%f err=%v", rate, err)
	}
}

type smartDegradationDeadlineReader struct {
	reader   io.Reader
	clock    *smartFakeClock
	deadline time.Time
	cancel   context.CancelFunc
}

func (r *smartDegradationDeadlineReader) Read(p []byte) (int, error) {
	if !r.clock.Now().Before(r.deadline) {
		r.cancel()
		return 0, context.Canceled
	}
	return r.reader.Read(p)
}

func TestSmartDegradationVerySlowConfirmsBeforeDeadline(t *testing.T) {
	clock := smartDegradationTestClock()
	start := clock.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	timed := &smartDegradationTimedReader{clock: clock, warmup: 1_000_000, warmupElapsed: 16 * time.Second, elapsed: 31250 * time.Millisecond}
	reader := &smartDegradationDeadlineReader{reader: timed, clock: clock, deadline: start.Add(30 * time.Second), cancel: cancel}
	rate, err := smartMeasureDegradationBody(ctx, reader, 25, 25*.5, nil, clock)
	if err != nil || math.Abs(rate-.5) > .001 {
		t.Fatalf("0.5 Mbps primary did not confirm before timeout: rate=%f elapsed=%s err=%v", rate, clock.Now().Sub(start), err)
	}
	if clock.Now().Sub(start) > 19*time.Second || timed.read >= 2_953_125 {
		t.Fatalf("slow proof waited for full fast quota: elapsed=%s bytes=%d", clock.Now().Sub(start), timed.read)
	}
}

type smartDegradationExpiredContext struct{ context.Context }

func (c smartDegradationExpiredContext) Err() error {
	if errors.Is(context.Cause(c.Context), context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return c.Context.Err()
}

type smartDegradationWarmupDeadlineReader struct {
	remaining int
	clock     *smartFakeClock
	cancel    context.CancelCauseFunc
	failure   error
}

func (r *smartDegradationWarmupDeadlineReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		r.clock.Advance(30 * time.Second)
		r.cancel(r.failure)
		return 0, r.failure
	}
	n := min(len(p), r.remaining)
	r.remaining -= n
	return n, nil
}

func TestSmartDegradationWarmupDeadlineSlowEvidence(t *testing.T) {
	for _, scenario := range []struct {
		name    string
		bytes   int
		passive int64
		failure error
		slow    bool
	}{
		{name: "145_KB_in_30_seconds", bytes: 145_000, failure: context.DeadlineExceeded, slow: true},
		{name: "zero_bytes", failure: context.DeadlineExceeded},
		{name: "cancelled", bytes: 145_000, failure: context.Canceled},
		{name: "truncated", bytes: 145_000, failure: io.ErrUnexpectedEOF},
		{name: "fast_shared_primary", bytes: 145_000, passive: 100_000_000, failure: context.DeadlineExceeded},
		{name: "half_target_boundary", bytes: 145_000, passive: 46_730_000, failure: context.DeadlineExceeded},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			clock := smartDegradationTestClock()
			base, cancel := context.WithCancelCause(context.Background())
			defer cancel(context.Canceled)
			ctx := smartDegradationExpiredContext{base}
			reader := &smartDegradationWarmupDeadlineReader{remaining: scenario.bytes, clock: clock, cancel: cancel, failure: scenario.failure}
			start := clock.Now()
			passive := func() int64 {
				if clock.Now().After(start) {
					return scenario.passive
				}
				return 0
			}
			rate, err := smartMeasureDegradationBody(ctx, reader, 25, 25*.5, passive, clock)
			if scenario.slow {
				if err != nil || math.Abs(rate-0.0386666667) > 0.000001 {
					t.Fatalf("145 KB/30 s was not slow evidence: rate=%f err=%v", rate, err)
				}
			} else if rate != 0 || !errors.Is(err, errSmartDegradationNoData) || !errors.Is(err, scenario.failure) {
				t.Fatalf("insufficient or cancelled proof became evidence: rate=%f err=%v", rate, err)
			}
		})
	}
}

func TestSmartDegradationHTTPWarmupDeadlinePreservesSlowEvidence(t *testing.T) {
	clock := smartDegradationTestClock()
	base, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	ctx := smartDegradationExpiredContext{base}
	started := make(chan struct{})
	finished := make(chan struct{})
	var once sync.Once
	dialer := &smartProbeTestDialer{httpHandler: func(conn net.Conn) {
		defer close(finished)
		defer conn.Close()
		if _, err := http.ReadRequest(bufio.NewReader(conn)); err != nil {
			return
		}
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 8812500\r\nConnection: close\r\n\r\n")
		select {
		case <-started:
		case <-ctx.Done():
			return
		}
		_, _ = io.CopyN(conn, smartZeroReader{}, 145_000)
		clock.Advance(30 * time.Second)
		cancel(context.DeadlineExceeded)
	}}
	passive := func() int64 { once.Do(func() { close(started) }); return 0 }
	rate, charged, err := smartMeasureDegradation(ctx, dialer, "http://203.0.113.1/down?bytes={bytes}", 10_000_000, 25, 25*.5, passive, clock)
	<-finished
	if err != nil || math.Abs(rate-0.0386666667) > 0.000001 || charged < 159_500 || charged > 170_000 {
		t.Fatalf("HTTP deadline discarded slow proof/accounting: rate=%f charged=%d err=%v", rate, charged, err)
	}
}

func TestSmartDegradationLateHeadersDoNotConfirmSlow(t *testing.T) {
	clock := smartDegradationTestClock()
	base, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	ctx := smartDegradationExpiredContext{base}
	started := make(chan struct{})
	finished := make(chan struct{})
	var once sync.Once
	dialer := &smartProbeTestDialer{httpHandler: func(conn net.Conn) {
		defer close(finished)
		defer conn.Close()
		if _, err := http.ReadRequest(bufio.NewReader(conn)); err != nil {
			return
		}
		clock.Advance(29_900 * time.Millisecond)
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 8812500\r\nConnection: close\r\n\r\n")
		select {
		case <-started:
		case <-ctx.Done():
			return
		}
		_, _ = io.CopyN(conn, smartZeroReader{}, 1_000)
		clock.Advance(100 * time.Millisecond)
		cancel(context.DeadlineExceeded)
	}}
	passive := func() int64 { once.Do(func() { close(started) }); return 0 }
	rate, charged, err := smartMeasureDegradation(ctx, dialer, "http://203.0.113.1/down?bytes={bytes}", 10_000_000, 25, 25*.5, passive, clock)
	<-finished
	if rate != 0 || charged <= 0 || !errors.Is(err, errSmartDegradationNoData) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("100 ms body after 29.9 s startup became slow evidence: rate=%f charged=%d err=%v", rate, charged, err)
	}
}

func TestSmartDegradationProfileSlowProofUsesThreshold(t *testing.T) {
	clock := smartDegradationTestClock()
	passive := int64(900_000_000)
	reader := &smartDegradationTimedReader{
		clock: clock, warmup: 1_000_000, elapsed: 12500 * time.Millisecond,
		passive: &passive, perRead: 13,
	}
	start := clock.Now()
	rate, err := smartMeasureDegradationBody(context.Background(), reader, 25, 20, func() int64 { return passive }, clock)
	if err != nil || math.Abs(rate-17.5) > .001 {
		t.Fatalf("profile threshold rejected same-interval slow proof: rate=%f err=%v", rate, err)
	}
	if reader.read >= 2_953_125 || clock.Now().Sub(start) > 2100*time.Millisecond {
		t.Fatalf("profile slow proof waited for a full target quota: bytes=%d elapsed=%s", reader.read, clock.Now().Sub(start))
	}
}

func TestSmartDegradationWorkerProfileDeadlineUsesSameIntervalPassive(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(strconv.FormatBool(legacy), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, clock := smartProfileThroughputTestGroup(t)
				s.preference.legacy = legacy
				// Align the fixture with synctest's clock before the worker creates its deadline.
				time.Sleep(time.Until(clock.Now()))
				s.clock = smartRealClock{}
				s.options.Throughput.DownloadURL = "http://localhost/down?bytes={bytes}"
				primary := s.primary
				primary.passiveRead = 900_000_000
				episode := smartTestDegradationEpisode(s, s.clock.Now())
				primary.queued["throughput"] = true
				primary.degradationQueued = episode
				finished := make(chan struct{})
				dialer := &smartProbeTestDialer{httpHandler: func(conn net.Conn) {
					defer close(finished)
					defer conn.Close()
					request, err := http.ReadRequest(bufio.NewReader(conn))
					if err != nil {
						return
					}
					if request.URL.Query().Get("bytes") != "8812500" {
						t.Errorf("profile threshold changed target quota: %s", request.URL)
						return
					}
					_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 8812500\r\nConnection: close\r\n\r\n")
					_, _ = io.CopyN(conn, smartZeroReader{}, 145_000)
					_, _ = io.Copy(io.Discard, conn)
				}}
				primary.outbound = &smartConfirmationTestOutbound{smartFakeOutbound: &smartFakeOutbound{tag: primary.tag}, dialer: dialer}
				job := smartJob{
					ctx: episode.ctx, timeout: 30 * time.Second, member: primary, kind: "throughput",
					capBytes: 10_000_000, purpose: smartDegradationConfirmation, episode: episode,
					generation: episode.generation, configGeneration: episode.configGeneration, memberGeneration: episode.memberGeneration,
				}
				done := make(chan smartResult, 1)
				start := s.clock.Now()
				go func() { done <- s.runOperation(job) }()
				synctest.Wait()
				select {
				case result := <-done:
					t.Fatalf("worker returned before its body deadline: %+v", result)
				default:
				}
				for range 29 {
					time.Sleep(time.Second)
					s.mu.Lock()
					primary.passiveRead += 2_250_000
					s.mu.Unlock()
				}
				result := <-done
				synctest.Wait()
				select {
				case <-finished:
				default:
					t.Fatal("deadline did not close the HTTP transfer")
				}
				if elapsed := s.clock.Now().Sub(start); elapsed != 30*time.Second || result.bytes <= 0 || result.bytes > 170_000 {
					t.Fatalf("worker did not settle deadline-limited transfer: result=%+v elapsed=%s", result, elapsed)
				}
				if legacy {
					if result.download != 0 || !errors.Is(result.err, errSmartDegradationNoData) || !errors.Is(result.err, context.DeadlineExceeded) {
						t.Fatalf("legacy accepted above-half-target incomplete proof: %+v", result)
					}
				} else if result.err != nil || math.Abs(result.download-17.4386666667) > .000001 {
					t.Fatalf("profile discarded deadline proof with same-interval passive bytes: %+v", result)
				}
				s.applyResultLocked(result, s.clock.Now())
				if legacy {
					if primary.floorDegraded || primary.throughput.Download != 50 || s.degradation.episode != nil {
						t.Fatal("inconclusive legacy transfer changed primary credit")
					}
				} else if !primary.floorDegraded || primary.throughput.Download != result.download || episode.confirmed != result.download || episode.challenger == nil {
					t.Fatal("confirmed profile transfer did not remove floor credit and retain challenger exploration")
				}
			})
		})
	}
}
