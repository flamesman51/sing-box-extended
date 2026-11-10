package group

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

var errSmartMeasureCap = errors.New("smart measurement byte allowance exhausted")

type smartMeasurementPhase struct {
	Streams int           `json:"streams"`
	Window  time.Duration `json:"window"`
	Warmup  time.Duration `json:"warmup"`
}

type smartMeasurementPlan struct {
	Download       *smartMeasurementPhase `json:"download,omitempty"`
	Upload         *smartMeasurementPhase `json:"upload,omitempty"`
	DownloadPhases int                    `json:"download_phases,omitempty"`
	STUNBursts     int                    `json:"stun_bursts,omitempty"`
}

func (p smartMeasurementPlan) Deadline() time.Duration {
	bound := 30 * time.Second
	if p.Download != nil {
		bound += time.Duration(max(1, p.DownloadPhases)) * (15*time.Second + p.Download.Warmup + p.Download.Window)
	}
	if p.Upload != nil {
		bound += 15*time.Second + p.Upload.Warmup + p.Upload.Window
	}
	if p.STUNBursts > 0 {
		bound += time.Duration(p.STUNBursts)*5*time.Second + time.Duration(p.STUNBursts-1)*time.Second
	}
	return bound
}

type smartDirectionResult struct {
	Value   float64       `json:"value"`
	P10     float64       `json:"p10,omitempty"`
	Mean    float64       `json:"mean,omitempty"`
	PostMin float64       `json:"post_min,omitempty"`
	Streams int           `json:"streams"`
	Window  time.Duration `json:"window"`
	Source  string        `json:"source"`
	At      time.Time     `json:"at"`
	Valid   bool          `json:"valid"`
}

type smartUploadObservation struct {
	Start, End time.Time
	Bytes      int64
	Verified   bool
}

type smartMeasurementResult struct {
	Download, Upload smartDirectionResult
	UploadUnverified bool
	DownloadBins     []int64
	UploadPosts      []smartUploadObservation
}

// The allowance is reserved before I/O, including framing and TLS traffic.
// Holding the mutex only around reservations keeps concurrent streams independent.
type smartMeasureMeter struct {
	mu              sync.Mutex
	remaining, used int64
	pending         int
	settlement      *sync.Cond
	tripped         bool
	stop            chan struct{}
}

func newSmartMeasureMeter(chargedRemaining int64) *smartMeasureMeter {
	return &smartMeasureMeter{remaining: max(0, chargedRemaining/11*10+chargedRemaining%11*10/11), stop: make(chan struct{})}
}

func (m *smartMeasureMeter) abortLocked() {
	if !m.tripped {
		m.tripped = true
		if m.stop != nil {
			close(m.stop)
		}
	}
}

func (m *smartMeasureMeter) allowance(size int) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.tripped {
		return 0
	}
	size = int(min(int64(size), m.remaining))
	m.remaining -= int64(size)
	if size == 0 {
		m.abortLocked()
	} else {
		m.pending++
	}
	return size
}

func (m *smartMeasureMeter) account(allowed, actual int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.remaining += int64(allowed - actual)
	m.used += int64(actual)
	m.pending--
	if m.pending == 0 && m.settlement != nil {
		m.settlement.Broadcast()
	}
}

func (m *smartMeasureMeter) available() int64 { m.mu.Lock(); defer m.mu.Unlock(); return m.remaining }
func (m *smartMeasureMeter) chargedBytes() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.used + m.used/10 + (m.used%10+9)/10
}
func (m *smartMeasureMeter) exhausted() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tripped || m.remaining == 0
}

func (m *smartMeasureMeter) settle() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pending == 0 {
		return
	}
	if m.settlement == nil {
		m.settlement = sync.NewCond(&m.mu)
	}
	for m.pending != 0 {
		m.settlement.Wait()
	}
}

func (m *smartMeasureMeter) stopped() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stop == nil {
		m.stop = make(chan struct{})
		if m.tripped {
			close(m.stop)
		}
	}
	return m.stop
}

type smartMeasureConn struct {
	net.Conn
	ctx    context.Context
	meter  *smartMeasureMeter
	upload *smartUploadTracker
}

func (c *smartMeasureConn) Read(p []byte) (n int, err error) {
	defer smartContextRecover(c.ctx, "throughput-read", &err)
	if len(p) == 0 {
		return 0, nil
	}
	allowed := c.meter.allowance(len(p))
	if allowed == 0 {
		return 0, errSmartMeasureCap
	}
	defer func() { c.meter.account(allowed, n) }()
	return c.Conn.Read(p[:allowed])
}

func (c *smartMeasureConn) Write(p []byte) (n int, err error) {
	defer smartContextRecover(c.ctx, "throughput-write", &err)
	if len(p) == 0 {
		return 0, nil
	}
	allowed := c.meter.allowance(len(p))
	if allowed == 0 {
		return 0, errSmartMeasureCap
	}
	defer func() { c.meter.account(allowed, n) }()
	if c.upload != nil {
		c.upload.writing()
	}
	n, err = c.Conn.Write(p[:allowed])
	if err == nil && n < len(p) {
		if allowed < len(p) {
			c.meter.mu.Lock()
			c.meter.abortLocked()
			c.meter.mu.Unlock()
			err = errSmartMeasureCap
		} else {
			err = io.ErrShortWrite
		}
	}
	return
}

func (c *smartMeasureConn) Close() (err error) {
	defer smartContextRecover(c.ctx, "throughput-close", &err)
	return c.Conn.Close()
}

type smartMeasureDialer struct {
	N.Dialer
	meter  *smartMeasureMeter
	upload *smartUploadTracker
}

func (d *smartMeasureDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (connection net.Conn, err error) {
	defer smartContextRecover(ctx, "throughput-dial", &err)
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := d.Dialer.DialContext(dialCtx, network, destination)
	if err != nil {
		return nil, err
	}
	return &smartMeasureConn{Conn: conn, ctx: ctx, meter: d.meter, upload: d.upload}, nil
}

type smartZeroReader struct{}

func (smartZeroReader) Read(p []byte) (int, error) { clear(p); return len(p), nil }

func smartMeasurementClient(dialer N.Dialer, meter *smartMeasureMeter, upload *smartUploadTracker) *http.Client {
	metered := &smartMeasureDialer{Dialer: dialer, meter: meter, upload: upload}
	resolved := &smartResolvingDialer{Dialer: metered, resolve: func(ctx context.Context, host string) ([]netip.Addr, error) {
		return smartResolveMember(ctx, metered, host)
	}}
	return smartProbeHTTPClient(resolved)
}

func smartMeasure(ctx context.Context, dialer N.Dialer, downloadURL, uploadURL string, meter *smartMeasureMeter, plan smartMeasurementPlan, clock smartClock) (result smartMeasurementResult, err error) {
	defer func() {
		if meter != nil {
			meter.settle()
		}
		if meter != nil && meter.exhausted() {
			err = errSmartMeasureCap
		}
		if ctx.Err() != nil && !errors.Is(err, errSmartContained) {
			err = ctx.Err()
		}
		if err != nil {
			result = smartMeasurementResult{}
		}
	}()
	defer smartContextRecover(ctx, "throughput-measure", &err)
	if err = ctx.Err(); err != nil {
		return
	}
	if meter == nil {
		return result, errors.New("smart measurement requires a shared meter")
	}
	if clock == nil {
		clock = smartRealClock{}
	}
	// Copy phase values: execution never mutates the admission plan.
	var download, upload smartMeasurementPhase
	if plan.Download != nil {
		download = *plan.Download
	}
	if plan.Upload != nil {
		upload = *plan.Upload
	}
	for _, phase := range []*smartMeasurementPhase{plan.Download, plan.Upload} {
		if phase != nil && (phase.Streams < 1 || phase.Window < 5*time.Second || phase.Warmup < time.Second) {
			return result, errors.New("invalid smart measurement phase")
		}
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-meter.stopped():
			cancel()
		case <-workCtx.Done():
		}
	}()
	if plan.Download != nil {
		if !strings.Contains(downloadURL, "{bytes}") {
			return result, errors.New("smart download URL lacks {bytes}")
		}
		result.Download, result.DownloadBins, err = smartMeasureDownload(workCtx, dialer, downloadURL, meter, download, clock)
		if err != nil {
			return
		}
	}
	if plan.Upload != nil {
		result.Upload, result.UploadPosts, result.UploadUnverified, err = smartMeasureUpload(workCtx, dialer, uploadURL, meter, upload, clock)
	}
	return
}

func smartAdaptiveSize(bytes int64, elapsed time.Duration, interval time.Duration, minimum int64) int64 {
	if elapsed <= 0 {
		return minimum
	}
	return int64(math.Max(float64(minimum), math.Min(25_000_000, float64(bytes)*float64(interval)/float64(elapsed))))
}

type smartDownloadWindow struct {
	mu         sync.Mutex
	start, end time.Time
	bins       []int64
	bytes      int64
	delivered  []bool
	ready      chan struct{}
	phase      smartMeasurementPhase
}

func (w *smartDownloadWindow) headers(now time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.start.IsZero() {
		w.start = now.Add(w.phase.Warmup)
		w.end = w.start.Add(w.phase.Window)
		close(w.ready)
	}
}

func (w *smartDownloadWindow) record(stream, n int, now time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.start.IsZero() || now.Before(w.start) || !now.Before(w.end) {
		return
	}
	w.bytes += int64(n)
	w.delivered[stream] = w.delivered[stream] || n > 0
	bin := int(now.Sub(w.start) / time.Second)
	if bin < len(w.bins) {
		w.bins[bin] += int64(n)
	}
}

func smartMeasureDownload(ctx context.Context, dialer N.Dialer, url string, meter *smartMeasureMeter, phase smartMeasurementPhase, clock smartClock) (result smartDirectionResult, bins []int64, err error) {
	phaseCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	w := smartDownloadWindow{phase: phase, bins: make([]int64, int(phase.Window/time.Second)), delivered: make([]bool, phase.Streams), ready: make(chan struct{})}
	errorsC := make(chan error, phase.Streams)
	var workers sync.WaitGroup
	for stream := range phase.Streams {
		workers.Add(1)
		go func() {
			defer workers.Done()
			var streamErr error
			defer func() { errorsC <- streamErr }()
			defer smartContextRecover(phaseCtx, "throughput-download", &streamErr)
			client := smartMeasurementClient(dialer, meter, nil)
			defer client.CloseIdleConnections()
			size := int64(1_000_000)
			buffer := make([]byte, 32*1024)
			for phaseCtx.Err() == nil {
				w.mu.Lock()
				finished := !w.end.IsZero() && !clock.Now().Before(w.end)
				w.mu.Unlock()
				if finished {
					return
				}
				request, requestErr := http.NewRequestWithContext(phaseCtx, http.MethodGet, strings.ReplaceAll(url, "{bytes}", strconv.FormatInt(size, 10)), nil)
				if requestErr != nil {
					streamErr = requestErr
					return
				}
				started := clock.Now()
				response, requestErr := client.Do(request)
				if requestErr != nil {
					streamErr = requestErr
					return
				}
				w.headers(clock.Now())
				if response.StatusCode < 200 || response.StatusCode >= 300 {
					_ = response.Body.Close()
					return
				}
				var received int64
				for {
					n, readErr := response.Body.Read(buffer)
					w.record(stream, n, clock.Now())
					received += int64(n)
					if readErr != nil {
						closeErr := response.Body.Close()
						if readErr != io.EOF {
							streamErr = readErr
							return
						}
						if closeErr != nil {
							streamErr = closeErr
							return
						}
						break
					}
				}
				size = smartAdaptiveSize(received, clock.Now().Sub(started), 2*time.Second, 1_000_000)
			}
		}()
	}
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	dialTimer := clock.NewTimer(10 * time.Second)
	select {
	case <-w.ready:
	case <-done:
	case <-ctx.Done():
	case <-dialTimer.C():
	}
	dialTimer.Stop()
	w.mu.Lock()
	end := w.end
	w.mu.Unlock()
	completed := false
	if !end.IsZero() {
		timer := clock.NewTimer(max(0, end.Sub(clock.Now())))
		select {
		case <-timer.C():
			completed = true
		case <-done:
			completed = !clock.Now().Before(end)
		case <-ctx.Done():
		}
		timer.Stop()
	}
	cancel()
	<-done
	close(errorsC)
	for streamErr := range errorsC {
		if errors.Is(streamErr, errSmartContained) {
			return result, nil, streamErr
		}
	}
	if ctx.Err() != nil {
		return result, nil, ctx.Err()
	}
	if !completed || len(w.bins) < 5 || slices.Contains(w.delivered, false) {
		return result, w.bins, nil
	}
	bins = w.bins
	sorted := slices.Clone(bins)
	slices.Sort(sorted)
	median := float64(sorted[len(sorted)/2])
	if len(sorted)%2 == 0 {
		median = (float64(sorted[len(sorted)/2-1]) + median) / 2
	}
	return smartDirectionResult{Value: median * 8 / 1_000_000, P10: float64(sorted[(len(sorted)+9)/10-1]) * 8 / 1_000_000, Mean: float64(w.bytes) * 8 / phase.Window.Seconds() / 1_000_000, Streams: phase.Streams, Window: phase.Window, Source: "active", At: clock.Now(), Valid: true}, bins, nil
}

type smartUploadTracker struct {
	mu    sync.Mutex
	clock smartClock
	armed bool
	start time.Time
	first func(time.Time)
}

func (t *smartUploadTracker) writing() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.armed && t.start.IsZero() {
		t.start = t.clock.Now()
		t.first(t.start)
	}
}

type smartUploadBody struct {
	tracker   *smartUploadTracker
	remaining int64
}

func (b *smartUploadBody) Read(p []byte) (int, error) {
	if b.remaining == 0 {
		return 0, io.EOF
	}
	b.tracker.mu.Lock()
	b.tracker.armed = true
	b.tracker.mu.Unlock()
	n := int(min(int64(len(p)), b.remaining))
	clear(p[:n])
	b.remaining -= int64(n)
	return n, nil
}

func smartMeasureUpload(ctx context.Context, dialer N.Dialer, url string, meter *smartMeasureMeter, phase smartMeasurementPhase, clock smartClock) (result smartDirectionResult, posts []smartUploadObservation, unverified bool, err error) {
	phaseCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex
	var start, end time.Time
	ready := make(chan struct{})
	first := func(now time.Time) {
		mu.Lock()
		defer mu.Unlock()
		if start.IsZero() {
			start = now.Add(phase.Warmup)
			end = start.Add(phase.Window)
			close(ready)
		}
	}
	errorsC := make(chan error, phase.Streams)
	var workers sync.WaitGroup
	for range phase.Streams {
		workers.Add(1)
		go func() {
			defer workers.Done()
			var streamErr error
			defer func() { errorsC <- streamErr }()
			defer smartContextRecover(phaseCtx, "throughput-upload", &streamErr)
			tracker := &smartUploadTracker{clock: clock, first: first}
			client := smartMeasurementClient(dialer, meter, tracker)
			defer client.CloseIdleConnections()
			size := int64(65_536)
			for phaseCtx.Err() == nil {
				mu.Lock()
				finished := !end.IsZero() && !clock.Now().Before(end)
				mu.Unlock()
				if finished {
					return
				}
				tracker.mu.Lock()
				tracker.start = time.Time{}
				tracker.armed = false
				tracker.mu.Unlock()
				body := &smartUploadBody{tracker: tracker, remaining: size}
				request, requestErr := http.NewRequestWithContext(phaseCtx, http.MethodPost, strings.ReplaceAll(url, "{bytes}", strconv.FormatInt(size, 10)), body)
				if requestErr != nil {
					streamErr = requestErr
					return
				}
				request.ContentLength = size
				request.Header.Set("Content-Type", "application/octet-stream")
				written := make(chan error, 1)
				request = request.WithContext(httptrace.WithClientTrace(request.Context(), &httptrace.ClientTrace{
					WroteRequest: func(info httptrace.WroteRequestInfo) { written <- info.Err },
				}))
				response, requestErr := client.Do(request)
				ended := clock.Now()
				tracker.mu.Lock()
				began := tracker.start
				tracker.mu.Unlock()
				observation := smartUploadObservation{Start: began, End: ended, Bytes: size}
				missingReceipt := false
				if requestErr == nil {
					receipt := response.Header.Get("cf-meta-upload-bytes")
					missingReceipt = receipt == ""
					received, parseErr := strconv.ParseInt(receipt, 10, 64)
					observation.Verified = response.StatusCode >= 200 && response.StatusCode < 300 && parseErr == nil && received == size && !began.IsZero()
					if observation.Verified {
						select {
						case writeErr := <-written:
							observation.Verified = writeErr == nil
						case <-phaseCtx.Done():
							observation.Verified = false
						}
					}
					_, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, smartProbeBodyLimit+1))
					closeErr := response.Body.Close()
					if readErr != nil || closeErr != nil {
						observation.Verified = false
					}
				}
				mu.Lock()
				if !began.IsZero() && !began.Before(start) && began.Before(end) {
					posts = append(posts, observation)
				}
				unverified = unverified || missingReceipt
				mu.Unlock()
				if !observation.Verified {
					streamErr = requestErr
					cancel()
					return
				}
				size = smartAdaptiveSize(size, ended.Sub(began), time.Second, 65_536)
			}
		}()
	}
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	dialTimer := clock.NewTimer(10 * time.Second)
	select {
	case <-ready:
	case <-done:
	case <-ctx.Done():
	case <-dialTimer.C():
	}
	dialTimer.Stop()
	mu.Lock()
	windowEnd := end
	mu.Unlock()
	completed := false
	if !windowEnd.IsZero() {
		timer := clock.NewTimer(max(0, windowEnd.Sub(clock.Now())))
		select {
		case <-timer.C():
			tail := clock.NewTimer(max(0, windowEnd.Add(5*time.Second).Sub(clock.Now())))
			select {
			case <-done:
				completed = !clock.Now().After(windowEnd.Add(5 * time.Second))
			case <-tail.C():
			case <-ctx.Done():
			}
			tail.Stop()
		case <-done:
			completed = !clock.Now().Before(windowEnd) && !clock.Now().After(windowEnd.Add(5*time.Second))
		case <-ctx.Done():
		}
		timer.Stop()
	}
	cancel()
	<-done
	close(errorsC)
	for streamErr := range errorsC {
		if errors.Is(streamErr, errSmartContained) {
			return result, posts, unverified, streamErr
		}
	}
	if ctx.Err() != nil {
		return result, posts, unverified, ctx.Err()
	}
	if !completed || len(posts) < 3 {
		return result, posts, unverified, nil
	}
	var earliest, latest time.Time
	var bytes int64
	minimum := math.Inf(1)
	for _, post := range posts {
		if !post.Verified || !post.End.After(post.Start) {
			return result, posts, unverified, nil
		}
		if earliest.IsZero() || post.Start.Before(earliest) {
			earliest = post.Start
		}
		if post.End.After(latest) {
			latest = post.End
		}
		bytes += post.Bytes
		minimum = min(minimum, float64(post.Bytes)*8/post.End.Sub(post.Start).Seconds()/1_000_000)
	}
	return smartDirectionResult{Value: float64(bytes) * 8 / latest.Sub(earliest).Seconds() / 1_000_000, PostMin: minimum * float64(phase.Streams), Streams: phase.Streams, Window: phase.Window, Source: "active", At: clock.Now(), Valid: true}, posts, unverified, nil
}
