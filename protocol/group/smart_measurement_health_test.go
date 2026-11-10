package group

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	N "github.com/sagernet/sing/common/network"
)

func smartMeasurementHealthJob(s *Smart, member *smartMember, kind string) smartJob {
	return smartJob{
		member: member, kind: kind, ctx: s.ctx, timeout: 5 * time.Second,
		generation: s.generation, memberGeneration: member.generation, configGeneration: s.configGeneration, networkType: s.network.Type,
	}
}

func smartApplyMeasurementHealthResult(s *Smart, result smartResult, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applyResultLocked(result, now)
}

func TestSmartMeasurementLoadedHealthResults(t *testing.T) {
	for _, cause := range []error{context.DeadlineExceeded, syscall.ECONNREFUSED, syscall.ECONNRESET, errors.New("TLS alert"), errors.New("unexpected probe response")} {
		t.Run(cause.Error(), func(t *testing.T) {
			s, clock := smartProfileThroughputTestGroup(t)
			member := s.primary
			s.probe = func(context.Context, N.Dialer, int) (time.Duration, error) { return 0, cause }
			lease, err := s.acquireMeasurementLease(s.ctx, false)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.release()
			result := s.runOperation(smartMeasurementHealthJob(s, member, "tcp"))
			if !result.loaded {
				t.Fatal("probe under data load lost its provenance")
			}
			before := member.lastOK
			smartApplyMeasurementHealthResult(s, result, clock.Now())
			if smartProbeTimeout(cause) {
				if member.state != "healthy" || member.lastOK != before || s.primary != member || !s.measurementGateClosedLocked() {
					t.Fatal("loaded timeout changed health or did not close data gate")
				}
			} else if member.state != "suspect" {
				t.Fatal("hard error under load was censored")
			}
		})
	}
}

func TestSmartMeasurementProbeProvenanceAtBothBoundaries(t *testing.T) {
	for _, overlap := range []string{"ends_during_probe", "whole_phase_during_probe", "delayed_clean_result"} {
		t.Run(overlap, func(t *testing.T) {
			s, clock := smartProfileThroughputTestGroup(t)
			member := s.primary
			var lease *smartMeasurementLease
			if overlap == "ends_during_probe" {
				lease, _ = s.acquireMeasurementLease(s.ctx, false)
			}
			s.probe = func(context.Context, N.Dialer, int) (time.Duration, error) {
				if overlap == "whole_phase_during_probe" {
					lease, _ = s.acquireMeasurementLease(s.ctx, false)
				}
				if lease != nil {
					lease.release()
				}
				return time.Millisecond, nil
			}
			clock.Advance(time.Minute)
			result := s.runOperation(smartMeasurementHealthJob(s, member, "tcp"))
			if overlap == "delayed_clean_result" {
				lease, _ = s.acquireMeasurementLease(s.ctx, false)
				defer lease.release()
				if result.loaded {
					t.Fatal("clean result was marked loaded")
				}
			} else if !result.loaded {
				t.Fatal("probe overlapping a finished phase was marked clean")
			}
			previous := member.lastOK
			smartApplyMeasurementHealthResult(s, result, clock.Now())
			if overlap == "delayed_clean_result" {
				if member.lastOK != clock.Now() {
					t.Fatal("application-time load discarded a clean delayed result")
				}
			} else if member.lastOK != previous {
				t.Fatal("loaded success changed lastOK")
			}
		})
	}
}

func TestSmartMeasurementLoadedSuccessCannotConfirm(t *testing.T) {
	for _, engine := range []string{"profile", "legacy"} {
		t.Run(engine, func(t *testing.T) {
			s, clock, candidate := smartPrepareConfirmation(t)
			if engine == "legacy" {
				s.preference.legacy = true
				s.primary.throughput.Download = 1
				candidate.throughput.Download = 50
				s.chooseLocked(clock.Now())
			}
			previous, count, at := candidate.lastOK, candidate.improvementObservations, candidate.improvementAt
			clock.Advance(time.Minute)
			s.probe = func(context.Context, N.Dialer, int) (time.Duration, error) { return time.Millisecond, nil }
			lease, _ := s.acquireMeasurementLease(s.ctx, false)
			result := s.runOperation(smartMeasurementHealthJob(s, candidate, "tcp"))
			lease.release()
			smartApplyMeasurementHealthResult(s, result, clock.Now())
			if candidate.lastOK != previous || candidate.improvementObservations != count || candidate.improvementAt != at {
				t.Fatal("loaded success advanced observation or confirmations")
			}
		})
	}
}

func TestSmartMeasurementLoadedUDPAndQUICDiscardAllEvidence(t *testing.T) {
	for _, kind := range []string{"udp", "udp443"} {
		t.Run(kind, func(t *testing.T) {
			s, clock := smartProfileThroughputTestGroup(t)
			member := s.standby
			beforeUDP, beforeQUIC := member.udp, member.udp443
			result := smartResult{
				member: member, kind: kind, loaded: true, rtt: time.Second,
				udpRTTs: []float64{1, 1, 1, 1, 1}, udpBurstSizes: []int{5},
				generation: s.generation, memberGeneration: member.generation, configGeneration: s.configGeneration,
			}
			smartApplyMeasurementHealthResult(s, result, clock.Now())
			if member.udp != beforeUDP || member.udp443 != beforeQUIC || len(member.udpRTTs) != 0 || len(member.quicRTTs) != 0 || !member.udpObservation.At.IsZero() {
				t.Fatal("loaded packet probe stored verdict, sample or observation")
			}
		})
	}
}

func TestSmartMeasurementPrimaryDeathCancelsChallengerAndCorroborates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, _ := smartProfileThroughputTestGroup(t)
		s.clock = smartRealClock{}
		primary, challenger := s.primary, s.standby
		s.primary.throughput.Download = 50
		server := &smartMeasurementServer{downRate: func(time.Duration) float64 { return 20 }}
		serverDone := make(chan struct{}, 16)
		dialer := server.dialer()
		dialer.httpHandler = func(conn net.Conn) {
			defer func() { serverDone <- struct{}{} }()
			server.serve(conn)
		}
		defer func() {
			for range dialer.httpDials.Load() {
				<-serverDone
			}
		}()
		challenger.outbound = &smartConfirmationTestOutbound{smartFakeOutbound: &smartFakeOutbound{challenger.tag}, dialer: dialer}
		phase := smartMeasurementPhase{Streams: 1, Window: 40 * time.Second, Warmup: 2 * time.Second}
		plan := smartMeasurementPlan{Download: &phase}
		id, capBytes, err := s.ledger.ReservePlan(s.ctx, s.network.Type, s.clock.Now(), 0, 1_000_000)
		if err != nil {
			t.Fatal(err)
		}
		job := smartMeasurementHealthJob(s, challenger, "throughput")
		job.ctx = s.measurementContextLocked(challenger, plan)
		job.plan, job.reservation, job.capBytes, job.meter, job.timeout = plan, id, capBytes, newSmartMeasureMeter(capBytes), plan.Deadline()
		job.member.queued["throughput"] = true
		s.options.Throughput.DownloadURL = "http://203.0.113.1/down?bytes={bytes}"
		done := make(chan smartResult, 1)
		go func() { done <- s.runJob(job) }()
		time.Sleep(3 * time.Second)
		s.probe = func(context.Context, N.Dialer, int) (time.Duration, error) { return 0, context.DeadlineExceeded }
		loaded := s.runOperation(smartMeasurementHealthJob(s, primary, "tcp"))
		smartApplyMeasurementHealthResult(s, loaded, s.clock.Now())
		if primary.state != "healthy" || !s.measurementGateClosedLocked() {
			t.Fatal("primary timeout did not defer health behind gate")
		}
		time.Sleep(9 * time.Second)
		s.mu.Lock()
		s.checkMeasurementPrimaryLocked(s.clock.Now())
		s.mu.Unlock()
		select {
		case <-done:
			t.Fatal("challenger stopped before 10s deferral bound")
		default:
		}
		time.Sleep(time.Second)
		s.mu.Lock()
		s.checkMeasurementPrimaryLocked(s.clock.Now())
		s.mu.Unlock()
		result := <-done
		if result.bytes <= 0 || !errors.Is(result.err, context.Canceled) || result.measurement.Download.Valid {
			t.Fatalf("cancelled phase published evidence or lost traffic: %+v", result)
		}
		budget := s.ledger.Snapshot(s.network.Type, s.clock.Now(), 0)
		if budget.UsedMB <= 0 || budget.ReservedMB != 0 {
			t.Fatalf("cancelled test not charged: %+v", budget)
		}
		smartApplyMeasurementHealthResult(s, result, s.clock.Now())
		s.checkMeasurementPrimaryLocked(s.clock.Now())
		if !primary.inflight {
			t.Fatal("unloaded primary recheck not queued")
		}
		var recheck smartJob
		for _, pending := range s.pending {
			if pending.member == primary && pending.kind == "tcp" {
				recheck = pending
				break
			}
		}
		s.pending = nil
		smartApplyMeasurementHealthResult(s, s.runOperation(recheck), s.clock.Now())
		if primary.state != "suspect" || !s.measurementGateClosedLocked() {
			t.Fatal("first unloaded failure reopened data gate")
		}
		waitCtx, cancel := context.WithCancel(s.ctx)
		waiting := make(chan error, 1)
		go func() {
			lease, err := s.acquireMeasurementLease(waitCtx, false)
			if lease != nil {
				lease.release()
			}
			waiting <- err
		}()
		synctest.Wait()
		select {
		case <-waiting:
			t.Fatal("another eligible member bypassed primary corroboration gate")
		default:
		}
		var confirm smartJob
		for _, pending := range s.pending {
			if pending.member == primary && pending.kind == "tcp" && pending.confirm {
				confirm = pending
				break
			}
		}
		if confirm.member == nil {
			t.Fatal("unloaded failure did not queue corroboration")
		}
		s.pending = nil
		smartApplyMeasurementHealthResult(s, s.runOperation(confirm), s.clock.Now())
		if s.primary != challenger || primary.state != "quarantined" || s.measurementGateClosedLocked() {
			t.Fatal("unloaded corroboration did not fail over and resolve gate")
		}
		synctest.Wait()
		cancel()
		if err := <-waiting; err != nil {
			t.Fatal(err)
		}
	})
}

func TestSmartMeasurementTimeoutClassificationPreservesHardErrors(t *testing.T) {
	for _, cause := range []error{syscall.ECONNREFUSED, io.EOF, errors.Join(context.DeadlineExceeded, syscall.ECONNRESET)} {
		if smartProbeTimeout(cause) {
			t.Fatalf("hard error classified as timeout: %v", cause)
		}
	}
	if !smartProbeTimeout(&net.OpError{Op: "read", Net: "tcp", Err: context.DeadlineExceeded}) {
		t.Fatal("wrapped timeout missed")
	}
}

func TestSmartMeasurementSTUNExecutionExcludesDataThroughCohortGap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, _ := smartProfileThroughputTestGroup(t)
		first, second := make(chan struct{}), make(chan struct{})
		s.udpBurstProbe = func(ctx context.Context, _ N.Dialer, bursts int) (smartRTTProbeResult, error) {
			if bursts != 2 {
				t.Errorf("jitter cohort bursts=%d", bursts)
			}
			close(first)
			timer := time.NewTimer(time.Second)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				return smartRTTProbeResult{}, ctx.Err()
			}
			close(second)
			return smartRTTProbeResult{Samples: []float64{10, 10, 10, 10, 10, 10, 10, 10, 10, 10}, BurstSizes: []int{5, 5}}, nil
		}
		job := smartMeasurementHealthJob(s, s.standby, "udp")
		job.plan.STUNBursts = 2
		done := make(chan smartResult, 1)
		go func() { done <- s.runOperation(job) }()
		<-first
		data := smartStartMeasurementLease(s, s.ctx, false)
		synctest.Wait()
		smartAssertMeasurementLeaseWaiting(t, data)
		select {
		case <-second:
			t.Fatal("cohort gap completed before its timer")
		default:
		}
		time.Sleep(time.Second)
		result := <-done
		lease := smartTakeMeasurementLease(t, data)
		defer lease.release()
		if result.loaded || result.err != nil || len(result.udpRTTs) != 10 {
			t.Fatalf("exclusive cohort lost clean samples: %+v", result)
		}
	})
}

func TestSmartMeasurementExpiredSTUNLeaseDoesNotFailUDP(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, clock := smartProfileThroughputTestGroup(t)
		member := s.standby
		before := member.udp
		lease, err := s.acquireMeasurementLease(s.ctx, false)
		if err != nil {
			t.Fatal(err)
		}
		defer lease.release()
		s.udpBurstProbe = func(context.Context, N.Dialer, int) (smartRTTProbeResult, error) {
			t.Fatal("STUN ran without its exclusive lease")
			return smartRTTProbeResult{}, nil
		}
		job := smartMeasurementHealthJob(s, member, "udp")
		job.timeout = time.Second
		result := s.runOperation(job)
		if !errors.Is(result.err, context.DeadlineExceeded) {
			t.Fatalf("lease wait lost its deadline: %v", result.err)
		}
		smartApplyMeasurementHealthResult(s, result, clock.Now())
		if member.udp != before || !member.udpObservation.At.IsZero() {
			t.Fatal("unexecuted lease waiter wrote failed UDP evidence")
		}
	})
}

func TestSmartMeasurementSuspectRecheckPreservesCorroborationOperator(t *testing.T) {
	s, clock := smartProfileThroughputTestGroup(t)
	primary := s.primary
	first := smartResult{
		member: primary, kind: "tcp", operator: int(s.generation % 3), err: syscall.ECONNREFUSED,
		generation: s.generation, memberGeneration: primary.generation, configGeneration: s.configGeneration,
	}
	smartApplyMeasurementHealthResult(s, first, clock.Now())
	var corroboration smartJob
	for _, job := range s.pending {
		if job.member == primary && job.kind == "tcp" {
			corroboration = job
		}
	}
	if corroboration.member == nil {
		t.Fatal("missing original corroboration")
	}
	s.pending = nil
	smartApplyMeasurementHealthResult(s, smartResult{
		member: primary, kind: "tcp", operator: corroboration.operator,
		confirm: true, loaded: true, generation: s.generation, memberGeneration: primary.generation,
		configGeneration: s.configGeneration,
	}, clock.Now())
	s.mu.Lock()
	s.checkMeasurementPrimaryLocked(clock.Now())
	s.mu.Unlock()
	for _, job := range s.pending {
		if job.member == primary && job.kind == "tcp" {
			if !job.confirm || job.operator != corroboration.operator {
				t.Fatalf("corroboration changed operator: original=%d recheck=%d confirm=%t", corroboration.operator, job.operator, job.confirm)
			}
			return
		}
	}
	t.Fatal("missing unloaded suspect recheck")
}

func TestSmartMeasurementLoadedURLTestDoesNotPublishRTT(t *testing.T) {
	s, clock := smartProfileThroughputTestGroup(t)
	member := s.primary
	waiter := &smartURLWaiter{remaining: map[*smartMember]bool{member: true}, result: make(map[string]uint16), done: make(chan struct{})}
	s.urlWaiters = append(s.urlWaiters, waiter)
	smartApplyMeasurementHealthResult(s, smartResult{
		member: member, kind: "tcp", loaded: true, rtt: time.Second,
		generation: s.generation, memberGeneration: member.generation, configGeneration: s.configGeneration,
	}, clock.Now())
	select {
	case <-waiter.done:
	default:
		t.Fatal("loaded URL test waiter did not complete")
	}
	if len(waiter.result) != 0 {
		t.Fatalf("loaded URL test published contaminated RTT: %v", waiter.result)
	}
}

func TestSmartMeasurementNaturalUnloadRechecksAndCleanSuccessReopensGate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, _ := smartProfileThroughputTestGroup(t)
		s.clock = smartRealClock{}
		primary := s.primary
		lease, err := s.acquireMeasurementLease(s.ctx, false)
		if err != nil {
			t.Fatal(err)
		}
		defer lease.release()
		smartApplyMeasurementHealthResult(s, smartResult{
			member: primary, kind: "tcp", loaded: true,
			err: context.DeadlineExceeded, generation: s.generation, memberGeneration: primary.generation,
			configGeneration: s.configGeneration,
		}, s.clock.Now())
		select {
		case <-s.kick:
		default:
		}
		woke := make(chan struct{})
		go func() {
			s.waitLoop(s.ctx, false)
			close(woke)
		}()
		synctest.Wait()
		lease.release()
		<-woke
		s.mu.Lock()
		s.tickLocked(s.clock.Now())
		queued := primary.inflight
		gated := s.measurementGateClosedLocked()
		s.mu.Unlock()
		if !queued || !gated {
			t.Fatal("natural unload did not wake the owner for a gated primary recheck")
		}
		data := smartStartMeasurementLease(s, s.ctx, false)
		synctest.Wait()
		smartAssertMeasurementLeaseWaiting(t, data)
		smartApplyMeasurementHealthResult(s, smartResult{
			member: primary, kind: "tcp", rtt: 20 * time.Millisecond,
			generation: s.generation, memberGeneration: primary.generation, configGeneration: s.configGeneration,
		}, s.clock.Now())
		next := smartTakeMeasurementLease(t, data)
		defer next.release()
		if primary.state != "healthy" || s.measurementGateClosedLocked() {
			t.Fatal("unloaded primary success failed to reopen data")
		}
	})
}

func TestSmartMeasurementPrimaryBoundCancelsEveryExecutingPhase(t *testing.T) {
	s, clock := smartProfileThroughputTestGroup(t)
	first, err := s.acquireMeasurementLease(s.ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	defer first.release()
	second, err := s.acquireMeasurementLease(s.ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	defer second.release()
	s.mu.Lock()
	s.openMeasurementPrimaryGateLocked(clock.Now())
	s.checkMeasurementPrimaryLocked(clock.Now().Add(10 * time.Second))
	count := len(s.measurementLoad.executing)
	s.mu.Unlock()
	if first.context().Err() != context.Canceled || second.context().Err() != context.Canceled || count != 2 {
		t.Fatal("primary bound did not cancel both phases while retaining their live leases")
	}
}

func TestSmartMeasurementLoadedNonPrimaryTimeoutDoesNotGateData(t *testing.T) {
	s, clock := smartProfileThroughputTestGroup(t)
	member := s.standby
	before := member.lastOK
	smartApplyMeasurementHealthResult(s, smartResult{
		member: member, kind: "tcp", loaded: true,
		err: context.DeadlineExceeded, generation: s.generation, memberGeneration: member.generation,
		configGeneration: s.configGeneration,
	}, clock.Now())
	if member.state != "healthy" || member.lastOK != before || s.measurementGateClosedLocked() {
		t.Fatal("loaded non-primary timeout changed health or gated the group")
	}
}

func TestSmartMeasurementAlreadyAdmittedUploadWaitsForPrimaryGate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, _ := smartProfileThroughputTestGroup(t)
		s.clock = smartRealClock{}
		member, primary := s.standby, s.primary
		server := &smartMeasurementServer{
			downRate: func(time.Duration) float64 { return 20 },
			upRate:   func(int, int) float64 { return 1 },
		}
		serverDone := make(chan struct{}, 16)
		dialer := server.dialer()
		dialer.httpHandler = func(conn net.Conn) {
			defer func() { serverDone <- struct{}{} }()
			server.serve(conn)
		}
		defer func() {
			for range dialer.httpDials.Load() {
				<-serverDone
			}
		}()
		member.outbound = &smartConfirmationTestOutbound{smartFakeOutbound: &smartFakeOutbound{member.tag}, dialer: dialer}
		s.options.Throughput.DownloadURL = "http://203.0.113.1/down?bytes={bytes}"
		s.options.Throughput.UploadURL = "http://203.0.113.1/up?bytes={bytes}"
		job := smartMeasurementHealthJob(s, member, "throughput")
		job.timeout = 30 * time.Second
		job.capBytes = 1_000_000_000
		job.meter = newSmartMeasureMeter(job.capBytes)
		job.plan = smartMeasurementPlan{
			Download: smartTestPhase(1), Upload: smartTestUploadPhase(1),
		}
		done := make(chan smartResult, 1)
		go func() { done <- s.runOperation(job) }()
		time.Sleep(5 * time.Second)
		smartApplyMeasurementHealthResult(s, smartResult{
			member: primary, kind: "tcp", loaded: true,
			err: context.DeadlineExceeded, generation: s.generation, memberGeneration: primary.generation,
			configGeneration: s.configGeneration,
		}, s.clock.Now())
		time.Sleep(2 * time.Second)
		s.mu.Lock()
		count := len(s.measurementLoad.executing)
		s.mu.Unlock()
		if count != 0 || dialer.httpDials.Load() != 1 {
			t.Fatalf("already-admitted upload started through the closed primary gate: executing=%d dials=%d", count, dialer.httpDials.Load())
		}
		smartApplyMeasurementHealthResult(s, smartResult{
			member: primary, kind: "tcp", rtt: 20 * time.Millisecond,
			generation: s.generation, memberGeneration: primary.generation, configGeneration: s.configGeneration,
		}, s.clock.Now())
		result := <-done
		if result.err != nil || !result.measurement.Download.Valid || !result.measurement.Upload.Valid {
			t.Fatalf("unblocked sequential phases failed: %+v error=%v", result.measurement, result.err)
		}
	})
}
