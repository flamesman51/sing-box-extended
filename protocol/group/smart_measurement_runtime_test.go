package group

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing/common/control"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"
)

func TestSmartMeasurementPlanEstimatesOnlyPlannedDirections(t *testing.T) {
	s, clock := smartTestGroup()
	defer s.Close()
	now := clock.Now()
	s.primary.throughput = smartThroughput{Download: 25, At: now, Source: "active", UploadResult: smartDirectionResult{Value: 10000, At: now, Valid: true}}
	s.standby.throughput = smartThroughput{Download: 25, At: now, Source: "active"}
	plan := s.downloadMeasurementPlanLocked()
	if got := s.measurementEstimateLocked(s.primary, plan, nil); got != 34_375_000 {
		t.Fatalf("download-only estimate=%d", got)
	}
	e := smartTestDegradationEpisode(s, now)
	if got := s.measurementEstimateLocked(s.primary, e.plan, e); got != 68_750_000 {
		t.Fatalf("two-download estimate=%d", got)
	}
	ledger, err := getSmartLedger(new(smartLedgerTestCache))
	if err != nil {
		t.Fatal(err)
	}
	ledger.Register(s.Tag(), map[string]float64{"wifi": 50})
	s.ledger = ledger
	if _, _, err = ledger.ReservePlan(s.ctx, "wifi", now, 0, s.measurementEstimateLocked(s.primary, e.plan, e)); !errors.Is(err, errSmartBudget) {
		t.Fatalf("partial episode admitted: %v", err)
	}
	if budget := ledger.Snapshot("wifi", now, 0); budget.UsedMB != 0 || budget.ReservedMB != 0 {
		t.Fatalf("denied episode spent traffic: %+v", budget)
	}
	disabled := false
	s.options.Throughput.Upload = &disabled
	s.primary.throughput.At = now.Add(-25 * time.Hour)
	regular := s.regularMeasurementPlanLocked(s.primary, now)
	if regular.Download == nil || regular.Upload != nil || s.measurementEstimateLocked(s.primary, regular, nil) != 34_375_000 {
		t.Fatal("disabled upload entered regular estimate")
	}
}

func TestSmartMeasurementDirectionsPreservedAndRestoredIndependently(t *testing.T) {
	s, clock := smartTestGroup()
	defer s.Close()
	now := clock.Now()
	down := smartDirectionResult{Value: 20, P10: 18, Mean: 21, At: now.Add(-25 * time.Hour), Source: "active", Streams: 4, Window: 7 * time.Second, Valid: true}
	up := smartDirectionResult{Value: 2, PostMin: 1.8, At: now.Add(-time.Hour), Source: "active", Streams: 1, Window: 7 * time.Second, Valid: true}
	s.applyMeasurementResultLocked(smartResult{member: s.primary, measurement: smartMeasurementResult{Download: down, Upload: up}}, now)
	s.restoreThroughputLocked(now)
	if _, fresh := smartFreshDownload(s.primary, now); fresh {
		t.Fatal("stale download restored fresh")
	}
	if value, fresh := smartFreshUpload(s.primary, now); !fresh || value != 2 {
		t.Fatal("fresh upload expired with download")
	}
	plan := s.regularMeasurementPlanLocked(s.primary, now)
	if plan.Download == nil || plan.Upload != nil {
		t.Fatal("direction age did not select download-only plan")
	}
	freshDown := down
	freshDown.At = now
	freshDown.Value = 30
	s.applyMeasurementResultLocked(smartResult{member: s.primary, measurement: smartMeasurementResult{Download: freshDown}}, now)
	if s.primary.throughput.UploadResult != up {
		t.Fatal("download-only result changed upload")
	}
	uploadPhase := s.uploadPhaseLocked()
	s.applyMeasurementResultLocked(smartResult{member: s.primary, plan: smartMeasurementPlan{Upload: &uploadPhase}, measurement: smartMeasurementResult{UploadUnverified: true}}, now.Add(time.Minute))
	if s.primary.throughput.UploadResult != up || !s.primary.throughput.UploadUnverified || !s.primary.throughput.UploadInconclusive {
		t.Fatal("inconclusive upload replaced verified upload")
	}
	encoded, err := s.history.Encode()
	if err != nil {
		t.Fatal(err)
	}
	s.history, err = decodeSmartHistory(encoded)
	if err != nil {
		t.Fatal(err)
	}
	s.restoreThroughputLocked(now.Add(time.Minute))
	if s.primary.throughput.DownloadResult != freshDown || s.primary.throughput.UploadResult != up || !s.primary.throughput.UploadInconclusive {
		t.Fatal("independent results did not round trip")
	}
	plan = s.regularMeasurementPlanLocked(s.primary, now.Add(time.Minute))
	if plan.Download != nil || plan.Upload == nil {
		t.Fatal("fresh download blocked inconclusive-upload retry")
	}
}

func TestSmartMeasurementLegacyUploadHasNoReceiptProvenance(t *testing.T) {
	for _, oldUpload := range []float64{0, 10} {
		t.Run(map[bool]string{false: "zero_or_absent", true: "positive"}[oldUpload > 0], func(t *testing.T) {
			s, clock := smartTestGroup()
			defer s.Close()
			now := clock.Now()
			stat := s.history.statistic(s.network.Key, s.primary.identity, now)
			stat.Download, stat.Upload, stat.ThroughputAt = 30, oldUpload, now.Unix()
			s.restoreThroughputLocked(now)
			if _, fresh := smartFreshUpload(s.primary, now); fresh {
				t.Fatal("unverified old upload ranked")
			}
			if s.primary.throughput.LegacyUpload != oldUpload {
				t.Fatal("positive legacy upload diagnostic lost")
			}
			plan := s.regularMeasurementPlanLocked(s.primary, now)
			if plan.Download != nil || plan.Upload == nil {
				t.Fatal("old cache did not schedule upload-only plan")
			}
		})
	}
}

func TestSmartMeasurementStatusDirectionalSourcesAndFloor(t *testing.T) {
	s, clock := smartTestGroup()
	defer s.Close()
	now := clock.Now()
	s.primary.throughput = smartThroughput{Download: 3, At: now.Add(-time.Hour), Source: "active", UploadUnverified: true, LegacyUpload: 5}
	s.primary.legacyEpisodeObs = smartLegacyEpisodeObservation{Value: 30, At: now}
	s.primary.downloadUnderLoad = smartDirectionResult{Value: 30, At: now, Valid: true, Source: "active"}
	s.primary.floorState = smartFloorState{Kind: "proven", Target: 25, At: now, Value: 30}
	s.applyRTTMeasurementLocked(s.primary, []float64{10, 10, 10, 10, 10, 10, 10, 10, 10, 20}, []int{5, 5}, now)
	encoded, err := SmartStatus(context.Background(), smartTestManager{values: []adapter.Outbound{s}}, "")
	if err != nil {
		t.Fatal(err)
	}
	var status map[string]any
	if err = json.Unmarshal([]byte(encoded), &status); err != nil {
		t.Fatal(err)
	}
	member := status["members"].([]any)[0].(map[string]any)
	if status["upload_verified"] != false || member["throughput_source"] != "under_load" || member["download_mbps"] != float64(3) {
		t.Fatal(encoded)
	}
	if member["floor_state"].(map[string]any)["effective"] != true || member["udp_rtt_ms"] != float64(10) || member["jitter_ms"] != float64(0) {
		t.Fatal(encoded)
	}
	if _, exists := member["offered_up"]; !exists {
		t.Fatal("missing write diagnostic")
	}
}

func TestSmartMeasurementEpisodeBudgetAbortSettlesOnceAndIgnoresUserReads(t *testing.T) {
	t.Parallel()
	s, _ := smartDegradationTestGroup(t)
	s.clock = smartRealClock{}
	now := s.clock.Now()
	s.targetMbps = 4
	s.options.Throughput.Window = 0
	s.options.Throughput.DownloadURL = "http://203.0.113.1/down?bytes={bytes}"
	s.ledger.Register(s.Tag(), map[string]float64{"wifi": 20})
	for _, member := range s.members {
		member.throughput = smartThroughput{Download: 4, At: now, Source: "active"}
	}
	a := &smartMeasurementServer{downRate: func(time.Duration) float64 { return 1 }}
	c := &smartMeasurementServer{downRate: func(time.Duration) float64 { return 100 }}
	s.primary.outbound = &smartConfirmationTestOutbound{smartFakeOutbound: &smartFakeOutbound{tag: "a"}, dialer: a.dialer()}
	s.standby.outbound = &smartConfirmationTestOutbound{smartFakeOutbound: &smartFakeOutbound{tag: "b"}, dialer: c.dialer()}
	e := smartTestDegradationEpisode(s, now)
	e.plan.Download.Window = 5 * time.Second
	s.startDegradationAdmissionLocked(s.primary, smartDegradationConfirmation, e, now)
	admission := smartLedgerWait(t, s.results)
	s.applyResultLocked(admission, s.clock.Now())
	s.admissionWorkers.Wait()
	if len(s.pending) != 1 {
		t.Fatal("A1 not queued")
	}
	estimate := s.measurementEstimateLocked(s.primary, e.plan, e)
	run := func() smartResult { job := s.pending[0]; s.pending = s.pending[1:]; return s.runJob(job) }
	lease := &smartLease{group: s, member: s.primary, generation: s.generation, started: now, lastRead: now, lastWrite: now}
	stopped := make(chan struct{})
	readsDone := make(chan struct{})
	go func() {
		defer close(readsDone)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopped:
				return
			case <-ticker.C:
				lease.CountRead(8 * 1024 * 1024)
			}
		}
	}()
	first := run()
	close(stopped)
	<-readsDone
	if first.err != nil || !first.measurement.Download.Valid || math.Abs(first.measurement.Download.Value-1) > .1 {
		t.Fatalf("user reads contaminated A1: %+v %v", first.measurement.Download, first.err)
	}
	s.applyResultLocked(first, s.clock.Now())
	if budget := s.ledger.Snapshot("wifi", s.clock.Now(), 0); budget.ReservedMB != 20 || budget.UsedMB != 0 {
		t.Fatalf("A1 prematurely reconciled: %+v", budget)
	}
	if len(s.pending) != 1 {
		t.Fatal("C not queued under shared reservation")
	}
	second := run()
	if !errors.Is(second.err, errSmartMeasureCap) {
		t.Fatalf("C must hit hard budget: %v", second.err)
	}
	s.applyResultLocked(second, s.clock.Now())
	s.accountingWorkers.Wait()
	budget := s.ledger.Snapshot("wifi", s.clock.Now(), 0)
	if budget.ReservedMB != 0 || budget.UsedMB > 20 || budget.UsedMB*1e6 <= float64(estimate) {
		t.Fatalf("episode accounting %+v estimate=%d", budget, estimate)
	}
	for _, member := range s.members {
		if member.floorState.Kind != "" || member.downloadUnderLoad.Valid || !member.legacyEpisodeObs.At.IsZero() || member.throughput.Download != 4 {
			t.Fatal("budget abort published staged evidence")
		}
	}
	s.ledger.mu.Lock()
	starts := len(s.ledger.state.Starts)
	s.ledger.mu.Unlock()
	if starts != 1 {
		t.Fatalf("episode starts=%d", starts)
	}
}

type smartMeasurementPause struct {
	pause.Manager
	paused bool
}

func (p *smartMeasurementPause) IsPaused() bool { return p.paused }

func TestSmartMeasurementLifecycleSurvivesHeartbeatsAndCancelsExplicitly(t *testing.T) {
	for _, gate := range []string{"pause", "power-save", "network", "health", "shutdown"} {
		t.Run(gate, func(t *testing.T) {
			s, clock := smartTestGroup()
			defer s.Close()
			platform := new(smartAdmissionCountingPlatform)
			manager := &smartNetworkTestManager{iface: &adapter.NetworkInterface{Interface: control.Interface{Name: "link", Index: 1}, Type: C.InterfaceTypeWIFI}}
			s.ctx = service.ContextWith[adapter.PlatformInterface](s.ctx, platform)
			s.ctx = service.ContextWith[adapter.NetworkManager](s.ctx, manager)
			s.network = smartReadNetwork(s.ctx, nil, clock.Now())
			s.workCtx, s.workCancel = context.WithCancel(s.ctx)
			s.backgroundCtx, s.backgroundCancel = context.WithCancel(s.workCtx)
			ctx := s.measurementContextLocked(s.primary, s.downloadMeasurementPlanLocked())
			for _, elapsed := range []time.Duration{10 * time.Second, 15 * time.Second} {
				delete(s.primary.queued, "tcp")
				s.primary.inflight = false
				s.queueLocked(s.primary, "tcp", 0, false, clock.Now().Add(elapsed))
				if ctx.Err() != nil {
					t.Fatal("heartbeat cancelled admitted measurement")
				}
			}
			switch gate {
			case "pause":
				s.pauseManager = &smartMeasurementPause{paused: true}
				s.handleRequest(smartRequest{kind: "pause"})
			case "power-save":
				platform.power.Store(true)
				smartForceNetwork(s.ctx)
				s.handleRequest(smartRequest{kind: "snapshot"})
			case "network":
				network := s.network
				network.Fingerprint += "-changed"
				s.newGenerationLocked(network, clock.Now())
			case "health":
				s.applyResultLocked(smartResult{member: s.primary, kind: "tcp", generation: s.generation, configGeneration: s.configGeneration, memberGeneration: s.primary.generation, err: errors.New("probe failed")}, clock.Now())
			case "shutdown":
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if ctx.Err() == nil {
				t.Fatalf("%s did not cancel admitted measurement", gate)
			}
		})
	}
}

func TestSmartMeasurementEpisodeCancellationAttributesAfterIOStops(t *testing.T) {
	s, clock := smartDegradationTestGroup(t)
	now := clock.Now()
	id, charged, err := s.ledger.ReservePlan(s.ctx, "wifi", now, 0, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	e := smartTestDegradationEpisode(s, now)
	e.reservation, e.capBytes, e.meter = id, charged, newSmartMeasureMeter(charged)
	allowed := e.meter.allowance(990)
	e.meter.account(allowed, 990)
	e.accountingWait.Add(1)
	s.mu.Lock()
	s.finishEpisodeAccountingLocked(e)
	s.generation++
	e.accountingWait.Done()
	s.mu.Unlock()
	s.accountingWorkers.Wait()
	if wifi := s.ledger.Snapshot("wifi", now, 0); wifi.UsedMB != 0 || wifi.ReservedMB != 0 {
		t.Fatalf("uncertain usage retained old network: %+v", wifi)
	}
	if cellular := s.ledger.Snapshot("cellular", now, 0); cellular.UsedMB != .001089 {
		t.Fatalf("wrong charged completed usage: %+v", cellular)
	}
}

func TestSmartMeasurementHistoryRejectsInvalidEvidence(t *testing.T) {
	for _, invalid := range []string{
		`"download":{"value":-1}`,
		`"upload":{"post_min":-1}`,
		`"download_under_load":{"streams":-1}`,
		`"floor_state":{"kind":"unknown"}`,
		`"legacy_episode_obs":{"value":-1}`,
		`"udp_rtts":[-1]`,
		`"quic_rtts":[-1]`,
	} {
		if _, err := decodeSmartHistory([]byte(`{"stats":{"wifi":{"a":{` + invalid + `}}}}`)); err == nil {
			t.Fatalf("invalid measurement accepted: %s", invalid)
		}
	}
}

func TestSmartMeasurementEpisodeCancelsWhenEitherMemberLeavesHealthy(t *testing.T) {
	for _, primary := range []bool{false, true} {
		s, clock := smartDegradationTestGroup(t)
		e := smartTestDegradationEpisode(s, clock.Now())
		member := e.challenger
		if primary {
			member = e.primary
		}
		s.applyResultLocked(smartResult{member: member, kind: "tcp", generation: s.generation, configGeneration: s.configGeneration, memberGeneration: member.generation, err: errors.New("probe failed")}, clock.Now())
		if e.ctx.Err() == nil || s.degradation.episode != nil {
			t.Fatal("unhealthy episode member did not cancel the entire plan")
		}
	}
}
