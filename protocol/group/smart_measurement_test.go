package group

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type smartMeasurementServer struct {
	mu            sync.Mutex
	started       time.Time
	requests      []int64
	posts         []smartUploadObservation
	connections   atomic.Int32
	downRate      func(time.Duration) float64
	upRate        func(int, int) float64
	delay         func(int, int) time.Duration
	receipt       string
	closeAfterEOF bool
	stopAfter     int
}

func (s *smartMeasurementServer) dialer() *smartProbeTestDialer {
	return &smartProbeTestDialer{httpHandler: s.serve}
}

func (s *smartMeasurementServer) serve(conn net.Conn) {
	id := int(s.connections.Add(1))
	if s.stopAfter > 0 && id > 1 {
		return
	}
	reader := bufio.NewReader(conn)
	for ordinal := 1; ; ordinal++ {
		request, err := http.ReadRequest(reader)
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.started.IsZero() {
			s.started = time.Now()
		}
		started := s.started
		s.mu.Unlock()
		if s.stopAfter > 0 && ordinal > s.stopAfter {
			return
		}
		if request.Method == http.MethodGet {
			size, _ := strconv.ParseInt(request.URL.Query().Get("bytes"), 10, 64)
			s.mu.Lock()
			s.requests = append(s.requests, size)
			s.mu.Unlock()
			connection := ""
			if s.closeAfterEOF {
				connection = "Connection: close\r\n"
			}
			if _, err = fmt.Fprintf(conn, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\n%s\r\n", size, connection); err != nil {
				return
			}
			if err = smartPacedTransfer(conn, nil, size, func() float64 { return s.downRate(time.Since(started)) }); err != nil {
				return
			}
			if s.closeAfterEOF {
				return
			}
			continue
		}
		if s.receipt == "early" {
			_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
			// Closing with unread body bytes sends a RST, which on Windows
			// can discard the response before the client reads it.
			_, _ = io.Copy(io.Discard, request.Body)
			return
		}
		size := request.ContentLength
		began := time.Now()
		if err = smartPacedTransfer(nil, request.Body, size, func() float64 { return s.upRate(id, ordinal) }); err != nil {
			return
		}
		_ = request.Body.Close()
		if s.delay != nil {
			time.Sleep(s.delay(id, ordinal))
		}
		header := ""
		switch s.receipt {
		case "missing":
		case "mismatch":
			header = "cf-meta-upload-bytes: 1\r\n"
		default:
			header = fmt.Sprintf("cf-meta-upload-bytes: %d\r\n", size)
		}
		s.mu.Lock()
		s.posts = append(s.posts, smartUploadObservation{Start: began, End: time.Now(), Bytes: size, Verified: true})
		s.mu.Unlock()
		if _, err = fmt.Fprintf(conn, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n%s\r\n", header); err != nil {
			return
		}
	}
}

func smartPacedTransfer(writer io.Writer, reader io.Reader, bytes int64, rate func() float64) error {
	var buffer [32 * 1024]byte
	target := time.Now()
	var transferred int64
	for transferred < bytes {
		// Small chunks expose raw delivery times rather than buffered bursts.
		bps := rate() * 1_000_000 / 8
		chunk := min(int64(len(buffer)), max(int64(1), int64(bps*.01)), bytes-transferred)
		target = target.Add(time.Duration(float64(chunk) / bps * float64(time.Second)))
		if delay := time.Until(target); delay > 0 {
			time.Sleep(delay)
		}
		if writer != nil {
			n, err := writer.Write(buffer[:chunk])
			transferred += int64(n)
			if err != nil {
				return err
			}
		} else {
			n, err := io.ReadFull(reader, buffer[:chunk])
			transferred += int64(n)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func smartRunMeasurement(t *testing.T, server *smartMeasurementServer, plan smartMeasurementPlan) (smartMeasurementResult, *smartMeasureMeter, *smartProbeTestDialer) {
	t.Helper()
	dialer := server.dialer()
	meter := newSmartMeasureMeter(1_000_000_000)
	ctx, cancel := context.WithTimeout(context.Background(), plan.Deadline())
	defer cancel()
	result, err := smartMeasure(ctx, dialer, "http://203.0.113.1/down?bytes={bytes}", "http://203.0.113.1/up?bytes={bytes}", meter, plan, smartRealClock{})
	if err != nil {
		t.Fatalf("measurement: %v", err)
	}
	return result, meter, dialer
}

func smartTestPhase(streams int) *smartMeasurementPhase {
	return &smartMeasurementPhase{Streams: streams, Warmup: time.Second, Window: 5 * time.Second}
}

func smartTestUploadPhase(streams int) *smartMeasurementPhase {
	phase := smartTestPhase(streams)
	phase.Window = 8 * time.Second
	return phase
}

func TestSmartAdaptiveSize(t *testing.T) {
	for _, direction := range []struct {
		name    string
		target  time.Duration
		minimum int64
		maximum int64
		want    int64
		cap     int64
	}{
		{"download", smartDownloadRequestTarget, 1_000_000, smartDownloadRequestLimit, 4_000_000, 100_000_000},
		{"upload", smartUploadRequestTarget, 65_536, smartUploadRequestLimit, 2_000_000, 25_000_000},
	} {
		t.Run(direction.name, func(t *testing.T) {
			for _, test := range []struct {
				name    string
				bytes   int64
				elapsed time.Duration
				want    int64
			}{
				{"target", 1_000_000, time.Second, direction.want},
				{"cap", 100_000_000, time.Millisecond, direction.cap},
				{"minimum", 1, time.Second, direction.minimum},
				{"zero_elapsed", 1_000_000, 0, direction.minimum},
				{"negative_elapsed", 1_000_000, -time.Second, direction.minimum},
			} {
				t.Run(test.name, func(t *testing.T) {
					if got := smartAdaptiveSize(test.bytes, test.elapsed, direction.target, direction.minimum, direction.maximum); got != test.want {
						t.Fatalf("adaptive size = %d, want %d", got, test.want)
					}
				})
			}
		})
	}
}

func TestSmartMeasurementPacedDownloadTwentyMbps(t *testing.T) {
	t.Parallel()
	server := &smartMeasurementServer{downRate: func(time.Duration) float64 { return 20 }}
	phase := smartTestPhase(1)
	original := *phase
	result, meter, dialer := smartRunMeasurement(t, server, smartMeasurementPlan{Download: phase})
	if !result.Download.Valid || math.Abs(result.Download.Value-20) > 1 || math.Abs(result.Download.Mean-20) > 1 {
		t.Fatalf("paced 20 Mbps: %+v bins=%v", result.Download, result.DownloadBins)
	}
	if *phase != original {
		t.Fatal("execution mutated admission plan")
	}
	if dialer.httpDials.Load() != 1 {
		t.Fatalf("EOF connection not reused: %d dials", dialer.httpDials.Load())
	}
	if meter.chargedBytes() <= 15_000_000 {
		t.Fatal("warmup/framing bytes were not charged")
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if len(server.requests) < 2 || server.requests[0] != 1_000_000 || server.requests[1] < 9_000_000 || server.requests[1] > 10_500_000 {
		t.Fatalf("adaptive ~4 s requests: %v", server.requests)
	}
}

func TestSmartMeasurementWarmupExcluded(t *testing.T) {
	t.Parallel()
	server := &smartMeasurementServer{downRate: func(elapsed time.Duration) float64 {
		if elapsed < time.Second {
			return 100
		}
		return 20
	}}
	result, _, _ := smartRunMeasurement(t, server, smartMeasurementPlan{Download: smartTestPhase(1)})
	if !result.Download.Valid || math.Abs(result.Download.Value-20) > 1 {
		t.Fatalf("warmup leaked into bins: %+v %v", result.Download, result.DownloadBins)
	}
}

func TestSmartMeasurementStreamsAggregate(t *testing.T) {
	for _, streams := range []int{1, 4} {
		t.Run(strconv.Itoa(streams), func(t *testing.T) {
			t.Parallel()
			server := &smartMeasurementServer{downRate: func(time.Duration) float64 { return 5 }}
			result, _, _ := smartRunMeasurement(t, server, smartMeasurementPlan{Download: smartTestPhase(streams)})
			want := float64(streams) * 5
			if !result.Download.Valid || math.Abs(result.Download.Value-want) > want*.05 {
				t.Fatalf("%d streams: %+v", streams, result.Download)
			}
		})
	}
}

func TestSmartMeasurementSlowFourStreamWindowAndTimeout(t *testing.T) {
	t.Parallel()
	server := &smartMeasurementServer{downRate: func(time.Duration) float64 { return .5 }}
	started := time.Now()
	result, _, _ := smartRunMeasurement(t, server, smartMeasurementPlan{Download: smartTestPhase(4)})
	if !result.Download.Valid || math.Abs(result.Download.Value-2) > .1 || time.Since(started) > 8*time.Second {
		t.Fatalf("slow link did not finish its window: %+v elapsed=%s", result.Download, time.Since(started))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	result, err := smartMeasure(ctx, server.dialer(), "http://203.0.113.1/down?bytes={bytes}", "", newSmartMeasureMeter(100_000_000), smartMeasurementPlan{Download: smartTestPhase(4)}, smartRealClock{})
	if !errors.Is(err, context.DeadlineExceeded) || result.Download.Valid {
		t.Fatalf("deadline became success: %+v %v", result, err)
	}
}

func TestSmartMeasurementServerClosedConnectionRedials(t *testing.T) {
	t.Parallel()
	server := &smartMeasurementServer{downRate: func(time.Duration) float64 { return 20 }, closeAfterEOF: true}
	result, _, dialer := smartRunMeasurement(t, server, smartMeasurementPlan{Download: smartTestPhase(1)})
	if !result.Download.Valid || dialer.httpDials.Load() < 2 {
		t.Fatalf("closed EOF not redialed: %+v dials=%d", result.Download, dialer.httpDials.Load())
	}
}

func TestSmartMeasurementPartialBinsExcluded(t *testing.T) {
	t.Parallel()
	phase := smartTestPhase(1)
	phase.Window = 5500 * time.Millisecond
	server := &smartMeasurementServer{downRate: func(time.Duration) float64 { return 20 }}
	result, _, _ := smartRunMeasurement(t, server, smartMeasurementPlan{Download: phase})
	if !result.Download.Valid || len(result.DownloadBins) != 5 || math.Abs(result.Download.Value-20) > 1 || math.Abs(result.Download.Mean-20) > 1 {
		t.Fatalf("partial bin included: %+v bins=%v", result.Download, result.DownloadBins)
	}
}

func TestSmartMeasurementFewerThanFiveBinsInconclusive(t *testing.T) {
	t.Parallel()
	server := &smartMeasurementServer{downRate: func(time.Duration) float64 { return 20 }, stopAfter: 1}
	result, meter, _ := smartRunMeasurement(t, server, smartMeasurementPlan{Download: smartTestPhase(1)})
	if result.Download.Valid || meter.chargedBytes() == 0 {
		t.Fatalf("short phase became success or lost accounting: %+v charged=%d", result.Download, meter.chargedBytes())
	}
	phase := smartTestPhase(1)
	phase.Window = 4 * time.Second
	_, err := smartMeasure(context.Background(), server.dialer(), "http://203.0.113.1/down?bytes={bytes}", "", newSmartMeasureMeter(100_000_000), smartMeasurementPlan{Download: phase}, smartRealClock{})
	if err == nil {
		t.Fatal("sub-five-second plan accepted")
	}
}

func TestSmartMeasurementUploadRejectsEarlyAndMismatchedReceipts(t *testing.T) {
	for _, receipt := range []string{"early", "missing", "mismatch"} {
		t.Run(receipt, func(t *testing.T) {
			t.Parallel()
			server := &smartMeasurementServer{receipt: receipt, upRate: func(int, int) float64 { return 10 }}
			result, meter, _ := smartRunMeasurement(t, server, smartMeasurementPlan{Upload: smartTestUploadPhase(1)})
			if result.Upload.Valid || result.UploadUnverified != (receipt != "mismatch") || meter.chargedBytes() == 0 {
				t.Fatalf("bad receipt became evidence: %+v charged=%d", result, meter.chargedBytes())
			}
		})
	}
}

func TestSmartMeasurementUnequalUploadWallClockAggregate(t *testing.T) {
	t.Parallel()
	server := &smartMeasurementServer{upRate: func(id, _ int) float64 { return float64(id) * .5 }}
	result, _, _ := smartRunMeasurement(t, server, smartMeasurementPlan{Upload: smartTestUploadPhase(4)})
	if !result.Upload.Valid || len(result.UploadPosts) < 3 {
		t.Fatalf("no verified cohort: %+v posts=%v", result.Upload, result.UploadPosts)
	}
	var bytes int64
	first, last := result.UploadPosts[0].Start, result.UploadPosts[0].End
	minimum := math.Inf(1)
	for _, post := range result.UploadPosts {
		if !post.Verified {
			t.Fatal("unverified post included")
		}
		bytes += post.Bytes
		if post.Start.Before(first) {
			first = post.Start
		}
		if post.End.After(last) {
			last = post.End
		}
		minimum = min(minimum, float64(post.Bytes)*8/post.End.Sub(post.Start).Seconds()/1_000_000)
	}
	want := float64(bytes) * 8 / last.Sub(first).Seconds() / 1_000_000
	if math.Abs(result.Upload.Value-want) > 1e-9 || math.Abs(result.Upload.PostMin-minimum*4) > 1e-9 || result.Upload.P10 != 0 || result.Upload.Mean != 0 {
		t.Fatalf("not a raw wall-clock aggregate: %+v want=%f", result.Upload, want)
	}
	if result.Upload.Value < 3.5 || result.Upload.Value > 5.25 {
		t.Fatalf("unequal 5 Mbps aggregate implausible: %f", result.Upload.Value)
	}
}

func TestSmartMeasurementDelayedWarmupPostExcludedAndConservative(t *testing.T) {
	t.Parallel()
	server := &smartMeasurementServer{upRate: func(int, int) float64 { return 4 }, delay: func(int, int) time.Duration { return 1400 * time.Millisecond }}
	result, _, _ := smartRunMeasurement(t, server, smartMeasurementPlan{Upload: smartTestUploadPhase(1)})
	server.mu.Lock()
	defer server.mu.Unlock()
	if !result.Upload.Valid || len(server.posts) < 4 || len(result.UploadPosts) != len(server.posts)-1 {
		t.Fatalf("warmup set: %+v measured=%v server=%v", result.Upload, result.UploadPosts, server.posts)
	}
	if result.UploadPosts[0].Start.Before(server.posts[0].End) || result.Upload.Value >= 2 {
		t.Fatalf("delayed response inflated rate or warmup counted: %+v", result.Upload)
	}
}

func TestSmartMeasurementUploadRateDropPastTailInconclusive(t *testing.T) {
	t.Parallel()
	server := &smartMeasurementServer{upRate: func(_, ordinal int) float64 {
		if ordinal > 5 {
			return .02
		}
		return 1
	}}
	started := time.Now()
	result, meter, _ := smartRunMeasurement(t, server, smartMeasurementPlan{Upload: smartTestUploadPhase(1)})
	verified := 0
	for _, post := range result.UploadPosts {
		if post.Verified {
			verified++
		}
	}
	if result.Upload.Valid || verified < 3 || meter.chargedBytes() == 0 || time.Since(started) > 16*time.Second {
		t.Fatalf("tail truncation became success: %+v posts=%v elapsed=%s", result.Upload, result.UploadPosts, time.Since(started))
	}
	if !slices.ContainsFunc(result.UploadPosts, func(post smartUploadObservation) bool { return !post.Verified }) {
		t.Fatal("cancelled measured POST missing from raw observations")
	}
}

func TestSmartMeasurementAsymmetricUploadBootstrapsIndependently(t *testing.T) {
	t.Parallel()
	server := &smartMeasurementServer{downRate: func(time.Duration) float64 { return 100 }, upRate: func(int, int) float64 { return 1 }}
	result, _, _ := smartRunMeasurement(t, server, smartMeasurementPlan{Download: smartTestPhase(1), Upload: smartTestUploadPhase(1)})
	server.mu.Lock()
	defer server.mu.Unlock()
	if !result.Download.Valid || !result.Upload.Valid || len(server.posts) < 2 || server.posts[0].Bytes != 65_536 || server.posts[1].Bytes < 220_000 || server.posts[1].Bytes > 260_000 || math.Abs(result.Upload.Value-1) > .1 {
		t.Fatalf("100/1 bootstrap: %+v posts=%v", result, server.posts)
	}
}

func TestSmartMeasurementDefaultUploadWindowExcludesWarmupCrossingPost(t *testing.T) {
	t.Parallel()
	server := &smartMeasurementServer{upRate: func(int, int) float64 { return 1 }}
	phase := smartTestUploadPhase(1)
	phase.Warmup = 2 * time.Second
	result, _, _ := smartRunMeasurement(t, server, smartMeasurementPlan{Upload: phase})
	server.mu.Lock()
	defer server.mu.Unlock()
	if !result.Upload.Valid || len(result.UploadPosts) < 3 || len(server.posts) < 5 {
		t.Fatalf("default upload window lacks a measured cohort: %+v posts=%v", result.Upload, result.UploadPosts)
	}
	warmupEnd := server.posts[0].Start.Add(phase.Warmup)
	crossing := server.posts[1]
	if !crossing.Start.Before(warmupEnd) || !crossing.End.After(warmupEnd) {
		t.Fatalf("second POST did not cross warmup: warmup_end=%s post=%+v", warmupEnd, crossing)
	}
	if len(result.UploadPosts) != len(server.posts)-2 || result.UploadPosts[0].Start.Before(crossing.End) {
		t.Fatalf("warmup-crossing POST entered measured evidence: measured=%v server=%v", result.UploadPosts, server.posts)
	}
	for _, post := range result.UploadPosts {
		if !post.Verified {
			t.Fatalf("measured POST lacks a receipt: %+v", post)
		}
	}
}

func TestSmartMeasurementOverestimatedUploadAdaptsDown(t *testing.T) {
	t.Parallel()
	server := &smartMeasurementServer{upRate: func(_, ordinal int) float64 {
		if ordinal == 1 {
			return 4
		}
		return 1
	}}
	phase := smartTestUploadPhase(1)
	phase.Window = 14 * time.Second
	result, _, _ := smartRunMeasurement(t, server, smartMeasurementPlan{Upload: phase})
	server.mu.Lock()
	defer server.mu.Unlock()
	if !result.Upload.Valid || len(server.posts) < 3 || server.posts[1].Bytes < 800_000 || server.posts[2].Bytes > 260_000 || math.Abs(result.Upload.Value-1) > .1 {
		t.Fatalf("overestimated POST did not adapt down: %+v posts=%v", result.Upload, server.posts)
	}
}

func TestSmartMeasurementInconclusiveUploadKeepsDownload(t *testing.T) {
	t.Parallel()
	server := &smartMeasurementServer{receipt: "missing", downRate: func(time.Duration) float64 { return 20 }, upRate: func(int, int) float64 { return 1 }}
	result, _, _ := smartRunMeasurement(t, server, smartMeasurementPlan{Download: smartTestPhase(1), Upload: smartTestUploadPhase(1)})
	if !result.Download.Valid || result.Upload.Valid || !result.UploadUnverified {
		t.Fatalf("upload erased independent download: %+v", result)
	}
}

func TestSmartMeasurementDeadlineIncludesEveryPhase(t *testing.T) {
	phase := &smartMeasurementPhase{Streams: 4, Warmup: 2 * time.Second, Window: 7 * time.Second}
	for _, test := range []struct {
		plan smartMeasurementPlan
		want time.Duration
	}{
		{smartMeasurementPlan{}, 30 * time.Second},
		{smartMeasurementPlan{Download: phase}, 54 * time.Second},
		{smartMeasurementPlan{Upload: phase}, 54 * time.Second},
		{smartMeasurementPlan{Download: phase, Upload: phase}, 78 * time.Second},
		{smartMeasurementPlan{Download: phase, DownloadPhases: 2}, 78 * time.Second},
		{smartMeasurementPlan{STUNBursts: 1}, 35 * time.Second},
		{smartMeasurementPlan{STUNBursts: 2}, 41 * time.Second},
	} {
		if got := test.plan.Deadline(); got != test.want {
			t.Fatalf("deadline %+v = %s, want %s", test.plan, got, test.want)
		}
	}
}
