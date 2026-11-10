package group

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type smartRTTTestDialer struct {
	smartProbeTestDialer
	mu          sync.Mutex
	sent        []time.Time
	fail        map[int]bool
	panicPacket bool
}

func (d *smartRTTTestDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	if d.panicPacket {
		panic("test STUN socket panic")
	}
	d.mu.Lock()
	index := len(d.sent)
	d.sent = append(d.sent, time.Now())
	fail := d.fail[index]
	d.mu.Unlock()
	if fail {
		return nil, errors.New("test packet loss")
	}
	return d.smartProbeTestDialer.ListenPacket(ctx, destination)
}

func TestSmartSTUNBurstAndCohortPacing(t *testing.T) {
	for _, bursts := range []int{1, 2} {
		t.Run(time.Duration(bursts).String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				dialer := &smartRTTTestDialer{smartProbeTestDialer: smartProbeTestDialer{strictUDP: true, badFirst: true}}
				result, err := smartSTUNProbe(context.Background(), dialer, bursts)
				if err != nil || len(result.Samples) != 5*bursts || !slices.Equal(result.BurstSizes, []int{5, 5}[:bursts]) {
					t.Fatalf("burst result=%+v error=%v", result, err)
				}
				for index := 1; index < len(dialer.sent); index++ {
					want := 200 * time.Millisecond
					if index == 5 {
						want = time.Second
					}
					if gap := dialer.sent[index].Sub(dialer.sent[index-1]); gap != want {
						t.Fatalf("request %d spacing=%s want=%s", index, gap, want)
					}
				}
			})
		})
	}
}

func TestSmartSTUNPartialRepliesRetainBurstBoundaries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dialer := &smartRTTTestDialer{fail: map[int]bool{0: true, 2: true, 7: true}}
		result, err := smartSTUNProbe(context.Background(), dialer, 2)
		if err != nil || !slices.Equal(result.BurstSizes, []int{3, 4}) || len(result.Samples) != 7 || len(dialer.sent) != 10 {
			t.Fatalf("partial replies=%+v requests=%d error=%v", result, len(dialer.sent), err)
		}
	})
}

func TestSmartSTUNCancellationIsNotCompletedObservation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		dialer := &smartRTTTestDialer{smartProbeTestDialer: smartProbeTestDialer{block: true}}
		_, err := smartSTUNProbe(ctx, dialer, 2)
		if !errors.Is(err, context.DeadlineExceeded) || len(dialer.sent) != 2 {
			t.Fatalf("cancelled probe requests=%d error=%v", len(dialer.sent), err)
		}
	})
}

func smartApplyRTTTestResult(s *Smart, member *smartMember, samples []float64, sizes []int, now time.Time) {
	s.applyResultLocked(smartResult{member: member, kind: "udp", udpRTTs: samples, udpBursts: len(sizes), udpBurstSizes: sizes,
		generation: s.generation, memberGeneration: member.generation, configGeneration: s.configGeneration}, now)
}

func TestSmartRTTObservationMinimumsAndRollingCap(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "responsive")
	member := s.primary
	smartApplyRTTTestResult(s, member, []float64{10, 20}, []int{2}, clock.Now())
	if !member.udpObservation.At.IsZero() || len(member.udpRTTs) != 2 {
		t.Fatal("two replies created an RTT observation or lost samples")
	}
	clock.Advance(time.Second)
	smartApplyRTTTestResult(s, member, []float64{80, 30, 20}, []int{3}, clock.Now())
	if member.udpObservation.Value != 30 || member.udpObservation.Samples != 3 || !member.udpObservation.At.Equal(clock.Now()) {
		t.Fatalf("RTT observation=%+v", member.udpObservation)
	}
	for range 14 {
		clock.Advance(time.Second)
		smartApplyRTTTestResult(s, member, []float64{40, 40, 40, 40, 40}, []int{5}, clock.Now())
	}
	if len(member.udpRTTs) != 64 || member.udpRTTs[0] != 40 || !member.jitterObservation.At.IsZero() {
		t.Fatal("rolling samples exceeded cap or unrelated bursts manufactured a jitter observation")
	}
}

func TestSmartJitterObservationRequiresFreshScheduledCohort(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "realtime-calls")
	member := s.primary
	smartApplyRTTTestResult(s, member, []float64{1, 1, 1, 1, 1}, []int{5}, clock.Now())
	clock.Advance(time.Second)
	smartApplyRTTTestResult(s, member, []float64{20, 20, 20, 20, 20}, []int{5}, clock.Now())
	if !member.jitterObservation.At.IsZero() {
		t.Fatal("independently scheduled bursts counted as a cohort")
	}
	clock.Advance(time.Second)
	smartApplyRTTTestResult(s, member, []float64{10, 20, 30, 40, 50, 60, 70, 80, 90}, []int{4, 5}, clock.Now())
	if !member.jitterObservation.At.IsZero() {
		t.Fatal("nine replies created a jitter observation")
	}
	clock.Advance(time.Second)
	smartApplyRTTTestResult(s, member, []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}, []int{5, 5}, clock.Now())
	if member.jitterObservation.Value != 40 || member.jitterObservation.Samples != 10 || !member.jitterObservation.At.Equal(clock.Now()) {
		t.Fatalf("cohort observation=%+v", member.jitterObservation)
	}
}

func TestSmartRTTConfirmationUsesBurstAlone(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "responsive")
	s.generationSucceeded = true
	s.preference.rank = []string{"rtt"}
	s.preference.dwell = time.Hour
	primary, candidate := s.primary, s.standby
	smartSeedRTTTestSamples(primary, []float64{50, 50, 50, 50, 50}, clock.Now())
	smartSeedRTTTestSamples(candidate, []float64{10, 10, 10, 10, 10}, clock.Now())
	smartApplyRTTTestResult(s, candidate, []float64{80, 80, 80}, []int{3}, clock.Now())
	if candidate.improvementObservations != 0 {
		t.Fatal("favorable old rolling median confirmed an unfavorable burst")
	}
	clock.Advance(s.preference.confirmationInterval)
	smartApplyRTTTestResult(s, candidate, []float64{20, 20, 20}, []int{3}, clock.Now())
	if candidate.improvementObservations != 1 {
		t.Fatal("three-reply burst did not count as one RTT observation")
	}
	clock.Advance(s.preference.confirmationInterval)
	s.applyResultLocked(smartResult{member: candidate, kind: "tcp", rtt: time.Millisecond,
		generation: s.generation, memberGeneration: candidate.generation, configGeneration: s.configGeneration}, clock.Now())
	if candidate.improvementObservations != 1 {
		t.Fatal("TCP heartbeat advanced RTT confirmation")
	}
}

func TestSmartRTTAndJitterScheduledConfirmationsSwitch(t *testing.T) {
	for _, criterion := range []string{"rtt", "jitter"} {
		t.Run(criterion, func(t *testing.T) {
			s, clock := smartProfileTestGroup(t, "responsive")
			s.generationSucceeded = true
			t.Cleanup(func() { s.Close() })
			if criterion == "jitter" {
				s.preference.rank = []string{"jitter"}
			}
			s.preference.confirmations = 2
			s.selectedAt = clock.Now().Add(-time.Hour)
			primary, candidate := s.primary, s.standby
			smartSeedRTTTestSamples(primary, []float64{40, 40, 40, 40, 40, 100, 100, 100, 100, 100}, clock.Now())
			primary.udp = smartEvidence{state: "ok", at: clock.Now(), expires: clock.Now().Add(time.Hour)}
			s.udpBurstProbe = func(_ context.Context, _ N.Dialer, bursts int) (smartRTTProbeResult, error) {
				result := smartRTTProbeResult{}
				for range bursts {
					result.Samples = append(result.Samples, 10, 10, 10, 10, 10)
					result.BurstSizes = append(result.BurstSizes, 5)
				}
				return result, nil
			}
			s.queueProfileUDPLocked(clock.Now(), false)
			if len(s.pending) != 1 || s.pending[0].kind != "udp" || s.pending[0].member != candidate {
				t.Fatalf("RTT-only pool did not discover candidate: %+v", s.pending)
			}
			job := s.pending[0]
			s.pending = nil
			s.applyResultLocked(s.runOperation(job), clock.Now())
			if s.improvementCandidate != candidate || candidate.improvementCriterion != criterion || candidate.improvementObservations != 1 {
				t.Fatalf("initial %s observation did not enter confirmation: count=%d criterion=%s", criterion, candidate.improvementObservations, candidate.improvementCriterion)
			}
			clock.Advance(s.preference.confirmationInterval)
			s.leases[primary] = map[*smartLease]struct{}{&smartLease{lastRead: clock.Now(), lastWrite: clock.Now()}: {}}
			if !s.scheduleImprovementConfirmationLocked(clock.Now()) {
				t.Fatal("STUN-only confirmation required a throughput ledger")
			}
			admission := smartAwaitConfirmationAdmission(t, s, clock)
			if admission.err != nil {
				t.Fatal(admission.err)
			}
			s.applyAdmissionLocked(admission, clock.Now())
			if len(s.pending) != 1 {
				t.Fatalf("STUN-only confirmation jobs=%d", len(s.pending))
			}
			job = s.pending[0]
			s.pending = nil
			wantBursts := 1
			if criterion == "jitter" {
				wantBursts = 2
			}
			if job.kind != "udp" || job.reservation != "" || job.plan.Download != nil || job.plan.Upload != nil || job.plan.STUNBursts != wantBursts {
				t.Fatalf("incorrect decisive confirmation plan: %+v", job)
			}
			s.applyResultLocked(s.runOperation(job), clock.Now())
			if s.primary != candidate {
				t.Fatalf("%s confirmations did not complete switch", criterion)
			}
		})
	}
}

func TestSmartRTTAndQUICDiagnosticsAreSeparate(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "responsive")
	member := s.primary
	s.applyResultLocked(smartResult{member: member, kind: "udp443", rtt: 5 * time.Millisecond,
		generation: s.generation, memberGeneration: member.generation, configGeneration: s.configGeneration}, clock.Now())
	if len(member.udpRTTs) != 0 || len(member.quicRTTs) != 1 || member.quicRTTs[0] != 5 || !member.udpObservation.At.IsZero() {
		t.Fatal("QUIC diagnostic RTT leaked into generic STUN evidence")
	}
}

func TestSmartSTUNWorkerPanicDiscardsWholeCohort(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dialer := &smartRTTTestDialer{panicPacket: true}
		result, err := smartSTUNProbe(context.Background(), dialer, 2)
		if !errors.Is(err, errSmartContained) || len(result.Samples) != 0 || len(result.BurstSizes) != 0 {
			t.Fatalf("contained STUN cohort=%+v error=%v", result, err)
		}
	})
}

func TestSmartJitterConfirmationUsesCohortAlone(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "realtime-calls")
	s.preference.rank = []string{"jitter"}
	s.preference.dwell = time.Hour
	primary, candidate := s.primary, s.standby
	smartSeedRTTTestSamples(primary, []float64{40, 40, 40, 40, 40, 100, 100, 100, 100, 100}, clock.Now())
	smartSeedRTTTestSamples(candidate, make([]float64, 54), clock.Now())
	smartApplyRTTTestResult(s, candidate, []float64{10, 10, 10, 10, 10, 100, 100, 100, 100, 100}, []int{5, 5}, clock.Now())
	if candidate.improvementObservations != 0 {
		t.Fatal("favorable rolling jitter confirmed an unfavorable fresh cohort")
	}
	clock.Advance(s.preference.confirmationInterval)
	smartApplyRTTTestResult(s, candidate, []float64{20, 20, 20, 20, 20, 20, 20, 20, 20, 20}, []int{5, 5}, clock.Now())
	if candidate.improvementObservations != 1 {
		t.Fatal("complete favorable cohort did not count as exactly one observation")
	}
}

func TestSmartRoundTripBucketsUseProtocolSamples(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "responsive")
	primary, candidate := s.primary, s.standby
	smartSeedRTTTestSamples(primary, []float64{10, 10, 10, 10, 10}, clock.Now())
	smartSeedRTTTestSamples(candidate, []float64{19, 19, 19, 19, 19}, clock.Now())
	candidate.rtts = []float64{1, 1, 1}
	if s.profileCriterionCompareLocked(candidate, primary, "rtt", clock.Now()) != 0 {
		t.Fatal("RTT ranking ignored the 10 ms bucket or used TCP check time")
	}
	smartSeedRTTTestSamples(candidate, []float64{9, 9, 9, 9, 9}, clock.Now())
	if s.profileCriterionCompareLocked(candidate, primary, "rtt", clock.Now()) >= 0 {
		t.Fatal("RTT ranking did not distinguish adjacent 10 ms buckets")
	}
	smartSeedRTTTestSamples(primary, []float64{10, 10, 10, 10, 10, 19, 19, 19, 19, 19}, clock.Now())
	smartSeedRTTTestSamples(candidate, []float64{10, 10, 10, 10, 10, 15, 15, 15, 15, 15}, clock.Now())
	if s.profileCriterionCompareLocked(candidate, primary, "jitter", clock.Now()) != 0 {
		t.Fatal("jitter ranking ignored the 5 ms bucket")
	}
	smartSeedRTTTestSamples(candidate, []float64{10, 10, 10, 10, 10, 14, 14, 14, 14, 14}, clock.Now())
	if s.profileCriterionCompareLocked(candidate, primary, "jitter", clock.Now()) >= 0 {
		t.Fatal("jitter ranking did not distinguish adjacent 5 ms buckets")
	}
}

func smartSeedRTTTestSamples(member *smartMember, samples []float64, at time.Time) {
	member.udpRTTs = samples
	member.udpRTTAt = make([]time.Time, len(samples))
	for index := range member.udpRTTAt {
		member.udpRTTAt[index] = at
	}
}

func TestSmartRoundTripSampleFreshnessMatchesRestore(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "responsive")
	member := s.primary
	smartSeedRTTTestSamples(member, []float64{1, 1, 1, 1, 1, 50, 50, 50, 50, 50}, clock.Now())
	for index := range 5 {
		member.udpRTTAt[index] = clock.Now().Add(-6 * time.Hour)
	}
	var buffer [64]float64
	before := append([]float64(nil), smartFreshRTTSamples(member, clock.Now(), buffer[:])...)
	if !slices.Equal(before, []float64{50, 50, 50, 50, 50}) ||
		s.profileCriterionCompareLocked(member, s.standby, "rtt", clock.Now()) >= 0 ||
		s.profileCriterionCompareLocked(member, s.standby, "jitter", clock.Now()) != 0 {
		t.Fatal("expired samples contributed to quantiles or minimum counts")
	}
	stat := s.history.statistic(s.network.Key, member.identity, clock.Now())
	stat.UDPRTTs, stat.UDPRTTAt = slices.Clone(member.udpRTTs), slices.Clone(member.udpRTTAt)
	encoded, err := s.history.Encode()
	if err != nil {
		t.Fatal(err)
	}
	s.history, err = decodeSmartHistory(encoded)
	if err != nil {
		t.Fatal(err)
	}
	member.udpRTTs, member.udpRTTAt = nil, nil
	s.restoreThroughputLocked(clock.Now())
	after := smartFreshRTTSamples(member, clock.Now(), buffer[:])
	if !slices.Equal(before, after) {
		t.Fatalf("restore changed fresh samples: before=%v after=%v", before, after)
	}
	member.udpRTTAt = nil
	if len(smartFreshRTTSamples(member, clock.Now(), buffer[:])) != 0 {
		t.Fatal("samples with no timestamp received ranked credit")
	}
}

func TestSmartPartialBurstDoesNotRefreshConfirmationObservation(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "responsive")
	member := s.primary
	smartApplyRTTTestResult(s, member, []float64{10, 10, 10, 10, 10, 10, 10, 10, 10, 10}, []int{5, 5}, clock.Now())
	at := clock.Now()
	clock.Advance(30 * time.Minute)
	smartApplyRTTTestResult(s, member, []float64{1, 1}, []int{2}, clock.Now())
	if !member.udpObservation.At.Equal(at) || !member.jitterObservation.At.Equal(at) ||
		s.profileCriterionFreshLocked(member, "rtt", clock.Now()) || s.profileCriterionFreshLocked(member, "jitter", clock.Now()) {
		t.Fatal("partial burst extended an old RTT or jitter confirmation observation")
	}
}

func TestSmartSTUNConfirmationBlocksOverlappingAdmission(t *testing.T) {
	for _, criterion := range []string{"rtt", "jitter"} {
		t.Run(criterion, func(t *testing.T) {
			s, clock := smartProfileThroughputTestGroup(t)
			s.preference.rank = []string{criterion}
			primary, candidate := s.primary, s.standby
			smartApplyRTTTestResult(s, primary, []float64{40, 40, 40, 40, 40, 100, 100, 100, 100, 100}, []int{5, 5}, clock.Now())
			smartApplyRTTTestResult(s, candidate, []float64{10, 10, 10, 10, 10, 10, 10, 10, 10, 10}, []int{5, 5}, clock.Now())
			clock.Advance(s.preference.confirmationInterval)
			if !s.scheduleImprovementConfirmationLocked(clock.Now()) {
				t.Fatal("STUN confirmation was not scheduled")
			}
			admission := smartAwaitConfirmationAdmission(t, s, clock)
			s.applyAdmissionLocked(admission, clock.Now())
			if len(s.pending) != 1 || s.pending[0].kind != "udp" {
				t.Fatal("STUN confirmation was not queued")
			}
			job := s.pending[0]
			s.startThroughputAdmissionLocked(candidate, clock.Now())
			if job.ctx.Err() != nil || s.admission != nil {
				t.Fatal("regular admission cancelled or overlapped the live STUN confirmation")
			}
			s.pending = nil
			s.applyResultLocked(smartResult{member: candidate, kind: job.kind, purpose: job.purpose,
				udpRTTs: []float64{10, 10, 10, 10, 10, 10, 10, 10, 10, 10}, udpBursts: 2, udpBurstSizes: []int{5, 5},
				generation: job.generation, memberGeneration: job.memberGeneration, configGeneration: job.configGeneration,
				primaryEpoch: job.primaryEpoch, improvementEpoch: job.improvementEpoch}, clock.Now())
			s.startThroughputAdmissionLocked(candidate, clock.Now())
			if s.admission == nil {
				t.Fatal("completed STUN confirmation blocked regular admission")
			}
		})
	}
}

func TestSmartExpiredRTTConfirmationCannotSwitchAfterDwell(t *testing.T) {
	for _, criterion := range []string{"rtt", "jitter"} {
		t.Run(criterion, func(t *testing.T) {
			s, clock := smartProfileThroughputTestGroup(t)
			s.preference.rank = []string{criterion}
			s.preference.dwell = time.Hour
			s.selectedAt = clock.Now()
			primary, candidate := s.primary, s.standby
			smartApplyRTTTestResult(s, primary, []float64{40, 40, 40, 40, 40, 100, 100, 100, 100, 100}, []int{5, 5}, clock.Now())
			for range s.preference.confirmations {
				smartApplyRTTTestResult(s, candidate, []float64{10, 10, 10, 10, 10, 10, 10, 10, 10, 10}, []int{5, 5}, clock.Now())
				clock.Advance(s.preference.confirmationInterval)
			}
			if candidate.improvementObservations != s.preference.confirmations || s.primary != primary {
				t.Fatal("confirmations did not complete before dwell")
			}
			clock.Advance(30 * time.Minute)
			// Keep generic UDP healthy without refreshing the decisive burst or cohort.
			candidate.udp = smartEvidence{state: "ok", at: clock.Now(), expires: clock.Now().Add(2 * time.Hour)}
			s.chooseLocked(clock.Now())
			if candidate.improvementObservations != 0 {
				t.Error("expired decisive observations preserved confirmation count")
			}
			clock.Advance(time.Hour)
			s.chooseLocked(clock.Now())
			if s.primary != primary {
				t.Fatal("expired RTT or jitter evidence switched after dwell")
			}
		})
	}
}
