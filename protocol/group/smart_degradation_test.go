package group

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
)

func smartTestDegradationEpisode(s *Smart, now time.Time) *smartDegradationEpisode {
	ctx, cancel := context.WithCancel(s.ctx)
	plan := s.downloadMeasurementPlanLocked()
	plan.DownloadPhases = 2
	challenger := s.degradationCandidateLocked(now)
	e := &smartDegradationEpisode{primary: s.primary, challenger: challenger, key: s.network.Key, generation: s.generation, configGeneration: s.configGeneration, memberGeneration: s.primary.generation, epoch: s.primaryEpoch, ctx: ctx, cancel: cancel, until: now.Add(plan.Deadline()), plan: plan}
	if challenger != nil {
		e.challengerGeneration = challenger.generation
	}
	s.degradation.episode = e
	return e
}

func smartTestDegradationResult(s *Smart, e *smartDegradationEpisode, m *smartMember, purpose string, rate float64, now time.Time) {
	if m.queued == nil {
		m.queued = make(map[string]bool)
	}
	m.queued["throughput"], m.degradationQueued = true, e
	s.applyResultLocked(smartResult{member: m, kind: "throughput", episode: e, purpose: purpose, measurement: smartMeasurementResult{Download: smartDirectionResult{Value: rate, At: now, Streams: 1, Window: 7 * time.Second, Valid: rate > 0}}, generation: e.generation, configGeneration: e.configGeneration, memberGeneration: m.generation}, now)
}

func smartDegradationTestGroup(t *testing.T) (*Smart, *smartFakeClock) {
	t.Helper()
	s, clock := smartTestGroup()
	ledger, err := getSmartLedger(&smartLedgerTestCache{})
	if err != nil {
		t.Fatal(err)
	}
	ledger.Register(s.Tag(), map[string]float64{"wifi": 5000, "cellular": 1000})
	s.ledger, s.throughputEnabled, s.generationSucceeded = ledger, true, true
	s.primary.throughput = smartThroughput{Download: 50, At: clock.Now(), Source: "active"}
	s.standby.throughput = s.primary.throughput
	for _, member := range s.members {
		member.queued = make(map[string]bool)
	}
	t.Cleanup(func() { s.Close() })
	return s, clock
}

func smartTestRestoreEpisodeEvidence(t *testing.T, s *Smart, now time.Time) {
	t.Helper()
	encoded, err := s.history.Encode()
	if err != nil {
		t.Fatal(err)
	}
	history, err := decodeSmartHistory(encoded)
	if err != nil {
		t.Fatal(err)
	}
	s.history = history
	for _, member := range s.members {
		member.floorState = smartFloorState{}
		member.legacyEpisodeObs = smartLegacyEpisodeObservation{}
		member.downloadUnderLoad = smartDirectionResult{}
	}
	s.restoreThroughputLocked(now)
}

func TestSmartDegradationSustainedDemand(t *testing.T) {
	for _, scenario := range []struct {
		name    string
		read    int
		episode bool
	}{
		{"slow", 375000, true}, {"sufficient", 4000000, false}, {"chat", 100, false}, {"exact_byte_floor", 6554, true}, {"below_byte_floor", 6553, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			s, clock := smartDegradationTestGroup(t)
			lease := &smartLease{group: s, member: s.primary, generation: s.generation, started: clock.Now().Add(-10 * time.Second), lastRead: clock.Now()}
			s.leases[s.primary] = map[*smartLease]struct{}{lease: {}}
			s.scheduleDegradationLocked(clock.Now())
			for second := range 20 {
				lease.CountRead(scenario.read)
				clock.Advance(time.Second)
				s.scheduleDegradationLocked(clock.Now())
				if second < 19 && s.degradation.episode != nil {
					t.Fatal("less than twenty complete bins triggered an episode")
				}
			}
			if got := s.degradation.episode != nil; got != scenario.episode {
				t.Fatalf("episode=%v, want %v", got, scenario.episode)
			}
			if s.primary.throughput.Download != 50 {
				t.Fatal("passive bytes changed regular evidence")
			}
			if e := s.degradation.episode; e != nil && (e.challenger != s.standby || e.plan.DownloadPhases != 2 || e.plan.Upload != nil) {
				t.Fatal("challenger and complete download-only plan were not chosen before admission")
			}
		})
	}
}

func TestSmartDegradationRecoveryAndIdleResetBins(t *testing.T) {
	for _, reset := range []string{"recovery", "idle", "closed"} {
		t.Run(reset, func(t *testing.T) {
			s, clock := smartDegradationTestGroup(t)
			lease := &smartLease{group: s, member: s.primary, generation: s.generation, started: clock.Now().Add(-10 * time.Second), lastRead: clock.Now()}
			s.leases[s.primary] = map[*smartLease]struct{}{lease: {}}
			s.scheduleDegradationLocked(clock.Now())
			for range 19 {
				lease.CountRead(375000)
				clock.Advance(time.Second)
				s.scheduleDegradationLocked(clock.Now())
			}
			switch reset {
			case "recovery":
				lease.CountRead(4_000_000)
				clock.Advance(time.Second)
				s.scheduleDegradationLocked(clock.Now())
			case "idle":
				clock.Advance(9 * time.Second)
				s.scheduleDegradationLocked(clock.Now())
			case "closed":
				lease.Close()
				s.scheduleDegradationLocked(clock.Now())
				lease = &smartLease{group: s, member: s.primary, generation: s.generation, started: clock.Now().Add(-10 * time.Second), lastRead: clock.Now()}
				s.leases[s.primary] = map[*smartLease]struct{}{lease: {}}
			}
			lease.CountRead(375000)
			s.scheduleDegradationLocked(clock.Now())
			for second := range 20 {
				if second > 0 {
					lease.CountRead(375000)
				}
				clock.Advance(time.Second)
				s.scheduleDegradationLocked(clock.Now())
				if second < 19 && s.degradation.episode != nil {
					t.Fatal("reset reused earlier slow bins")
				}
			}
			if s.degradation.episode == nil {
				t.Fatal("fresh twenty-second low window did not trigger")
			}
		})
	}
}

func TestSmartProfileDegradationThreshold(t *testing.T) {
	for _, scenario := range []struct {
		name            string
		rank            []string
		rate            int
		legacy, episode bool
	}{
		{"legacy_seventy_percent", nil, 2_187_500, true, false},
		{"floor_seventy_percent", []string{"speed_floor", "download"}, 2_187_500, false, true},
		{"download_seventy_percent", []string{"download"}, 2_187_500, false, true},
		{"throughput_seventy_percent", []string{"throughput"}, 2_187_500, false, true},
		{"floor_threshold", []string{"speed_floor"}, 2_500_000, false, false},
		{"stability_only", []string{"stability"}, 375000, false, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			s, clock := smartProfileThroughputTestGroup(t)
			s.preference.legacy, s.preference.rank = scenario.legacy, scenario.rank
			lease := &smartLease{group: s, member: s.primary, generation: s.generation, started: clock.Now().Add(-10 * time.Second), lastRead: clock.Now()}
			s.leases[s.primary] = map[*smartLease]struct{}{lease: {}}
			s.scheduleDegradationLocked(clock.Now())
			for range 20 {
				lease.CountRead(scenario.rate)
				clock.Advance(time.Second)
				s.scheduleDegradationLocked(clock.Now())
			}
			if got := s.degradation.episode != nil; got != scenario.episode {
				t.Fatalf("episode=%v, want %v", got, scenario.episode)
			}
		})
	}
}

func TestSmartDegradationEpisodeInvalidation(t *testing.T) {
	for _, change := range []string{"key", "generation", "config", "member", "away-back", "pause", "power", "closed"} {
		t.Run(change, func(t *testing.T) {
			s, clock := smartDegradationTestGroup(t)
			primary := s.primary
			e := smartTestDegradationEpisode(s, clock.Now())
			switch change {
			case "key":
				s.network.Key += "/tower:new"
			case "generation":
				s.generation++
			case "config":
				s.configGeneration++
			case "member":
				primary.generation++
			case "away-back":
				s.switchLocked(s.standby, "switch", "test", clock.Now())
				s.switchLocked(primary, "switch", "test", clock.Now())
			case "pause":
				s.paused = true
			case "power":
				s.network.PowerSave = true
			case "closed":
				s.closed = true
			}
			smartTestDegradationResult(s, e, primary, smartDegradationConfirmation, 3, clock.Now())
			if s.degradation.episode != nil || primary.degradationQueued != nil || primary.throughput.Download != 50 || primary.floorState.Kind != "" {
				t.Fatal("invalid episode retained ownership or published evidence")
			}
			s.closed = false
		})
	}
}

func TestSmartDegradationHeartbeatLifetime(t *testing.T) {
	s, clock := smartDegradationTestGroup(t)
	s.workCtx, s.workCancel = context.WithCancel(s.ctx)
	s.backgroundCtx, s.backgroundCancel = context.WithCancel(s.workCtx)
	e := smartTestDegradationEpisode(s, clock.Now())
	s.preemptBackgroundLocked(smartJob{kind: "tcp", urgent: true, member: s.primary})
	if e.ctx.Err() != nil {
		t.Fatal("heartbeat cancelled admitted measurement")
	}
	s.clearDegradationLocked()
	if !errors.Is(e.ctx.Err(), context.Canceled) {
		t.Fatal("invalidation did not cancel measurement")
	}
}

func TestSmartProfileDegradationStagedFloorOnly(t *testing.T) {
	for _, scenario := range []struct {
		name       string
		challenger float64
		outcome    string
		switches   bool
	}{
		{"below_floor", 20, "challenger_slow", false}, {"meets_floor", 25, "switched", true}, {"above_floor", 40, "switched", true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			s, clock := smartProfileThroughputTestGroup(t)
			primary, challenger := s.primary, s.standby
			beforePrimary, beforeChallenger := primary.throughput, challenger.throughput
			clock.Advance(time.Second)
			e := smartTestDegradationEpisode(s, clock.Now())
			smartTestDegradationResult(s, e, primary, smartDegradationConfirmation, 3, clock.Now())
			if primary.floorState.Kind != "" || primary.downloadUnderLoad.Valid || !s.meetsFloorLocked(primary, clock.Now()) {
				t.Fatal("A1 evidence was committed before terminal outcome")
			}
			clock.Advance(time.Second)
			smartTestDegradationResult(s, e, challenger, smartDegradationChallenger, scenario.challenger, clock.Now())
			if (s.primary == challenger) != scenario.switches || s.degradation.state != scenario.outcome {
				t.Fatalf("selected=%s outcome=%s", s.primary.tag, s.degradation.state)
			}
			if primary.throughput != beforePrimary || challenger.throughput != beforeChallenger {
				t.Fatal("under-load results replaced regular downloads")
			}
			if primary.floorState.Kind != "revoked" || s.meetsFloorLocked(primary, clock.Now()) {
				t.Fatal("completed slow A1 retained floor credit")
			}
			if !primary.downloadUnderLoad.Valid || primary.downloadUnderLoad.Value != 3 || challenger.downloadUnderLoad.Value != scenario.challenger {
				t.Fatal("completed plan diagnostics missing")
			}
			if scenario.switches && (challenger.floorState.Kind != "proven" || !s.meetsFloorLocked(challenger, clock.Now())) {
				t.Fatal("challenger proof missing")
			}
			if primary.legacyEpisodeObs.At.IsZero() == false || challenger.legacyEpisodeObs.At.IsZero() == false {
				t.Fatal("profile results leaked into legacy observations")
			}
		})
	}
}

func TestSmartProfileDegradationCancellationDiscardsStagedResults(t *testing.T) {
	for _, failure := range []error{context.Canceled, errSmartBudget} {
		s, clock := smartProfileThroughputTestGroup(t)
		primary, challenger := s.primary, s.standby
		e := smartTestDegradationEpisode(s, clock.Now())
		smartTestDegradationResult(s, e, primary, smartDegradationConfirmation, 3, clock.Now())
		challenger.queued["throughput"], challenger.degradationQueued = true, e
		s.applyResultLocked(smartResult{member: challenger, kind: "throughput", episode: e, purpose: smartDegradationChallenger, err: failure, generation: e.generation, configGeneration: e.configGeneration, memberGeneration: challenger.generation}, clock.Now())
		if primary.floorState.Kind != "" || primary.downloadUnderLoad.Valid || challenger.downloadUnderLoad.Valid || !primary.legacyEpisodeObs.At.IsZero() {
			t.Fatal("failed C committed staged A1")
		}
		if s.degradation.episode != nil {
			t.Fatal("failed plan retained owner")
		}
	}
}

func TestSmartProfileDegradationPrimaryFastUsesHysteresis(t *testing.T) {
	for _, rate := range []float64{20, 22, 25, 100} {
		s, clock := smartProfileThroughputTestGroup(t)
		before := s.primary.throughput
		e := smartTestDegradationEpisode(s, clock.Now())
		smartTestDegradationResult(s, e, s.primary, smartDegradationConfirmation, rate, clock.Now())
		if s.degradation.state != "primary_fast" || s.degradation.episode != nil || s.primary.throughput != before || s.primary.downloadUnderLoad.Value != rate {
			t.Fatalf("rate=%v did not finish primary_fast without magnitude publication", rate)
		}
	}
}

func TestSmartProfileDegradationRequiresDecisiveFloor(t *testing.T) {
	for _, scenario := range []struct {
		name           string
		rank           []string
		worseStability bool
		outcome        string
	}{
		{"speed_max", []string{"download"}, false, "no_floor_criterion"},
		{"throughput_only", []string{"throughput"}, false, "no_floor_criterion"},
		{"stability_before_floor", []string{"stability", "speed_floor"}, true, "challenger_slow"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			s, clock := smartProfileThroughputTestGroup(t)
			s.preference.rank = scenario.rank
			primary, challenger := s.primary, s.standby
			primary.throughput.Download, challenger.throughput.Download = 20, 30
			if scenario.worseStability {
				stat := s.history.statistic(s.network.Key, primary.identity, clock.Now())
				stat.S, stat.W, stat.UpdatedAt = 1000, 1000, clock.Now().Unix()
				stat = s.history.statistic(s.network.Key, challenger.identity, clock.Now())
				stat.S, stat.W, stat.UpdatedAt = 850, 1000, clock.Now().Unix()
			}
			clock.Advance(time.Second)
			e := smartTestDegradationEpisode(s, clock.Now())
			smartTestDegradationResult(s, e, primary, smartDegradationConfirmation, 3, clock.Now())
			clock.Advance(time.Second)
			smartTestDegradationResult(s, e, challenger, smartDegradationChallenger, 25, clock.Now())
			if s.primary != primary || s.degradation.state != scenario.outcome {
				t.Fatalf("non-floor decision switched: state=%s", s.degradation.state)
			}
			if primary.throughput.Download != 20 || challenger.throughput.Download != 30 {
				t.Fatal("episode published download magnitude")
			}
			if s.degradation.state == "shared_bottleneck_suspected" {
				t.Fatal("profile emitted shared-bottleneck diagnosis")
			}
		})
	}
}

func TestSmartProfileDegradationUnmeasuredChallenger(t *testing.T) {
	s, clock := smartProfileThroughputTestGroup(t)
	primary, challenger := s.primary, s.standby
	challenger.throughput = smartThroughput{}
	clock.Advance(time.Second)
	e := smartTestDegradationEpisode(s, clock.Now())
	if e.challenger != challenger {
		t.Fatal("unmeasured healthy challenger was not planned")
	}
	smartTestDegradationResult(s, e, primary, smartDegradationConfirmation, 3, clock.Now())
	clock.Advance(time.Second)
	smartTestDegradationResult(s, e, challenger, smartDegradationChallenger, 25, clock.Now())
	if s.primary != challenger || !s.meetsFloorLocked(challenger, clock.Now()) || !challenger.throughput.At.IsZero() {
		t.Fatal("unmeasured challenger did not retain independent proven floor")
	}
	s.rankLocked(clock.Now())
	if !s.meetsFloorLocked(challenger, clock.Now()) {
		t.Fatal("ordinary ranking lost proven floor")
	}
	smartTestRestoreEpisodeEvidence(t, s, clock.Now())
	if !s.meetsFloorLocked(challenger, clock.Now()) || challenger.floorState.Kind != "proven" {
		t.Fatal("restart lost proven floor")
	}
}

func TestSmartProfileDegradationRevocationRecoveryAndRestore(t *testing.T) {
	s, clock := smartProfileThroughputTestGroup(t)
	primary, challenger := s.primary, s.standby
	clock.Advance(time.Second)
	e := smartTestDegradationEpisode(s, clock.Now())
	smartTestDegradationResult(s, e, primary, smartDegradationConfirmation, 3, clock.Now())
	clock.Advance(time.Second)
	smartTestDegradationResult(s, e, challenger, smartDegradationChallenger, 10, clock.Now())
	for _, rate := range []float64{15, 22, 25} {
		smartTestRestoreEpisodeEvidence(t, s, clock.Now())
		if s.meetsFloorLocked(primary, clock.Now()) {
			t.Fatal("restore erased revocation")
		}
		clock.Advance(time.Second)
		s.applyResultLocked(smartResult{member: primary, kind: "throughput", measurement: smartMeasurementResult{Download: smartDirectionResult{Value: rate, At: clock.Now(), Valid: true}}, generation: s.generation, configGeneration: s.configGeneration, memberGeneration: primary.generation}, clock.Now())
		if got := s.meetsFloorLocked(primary, clock.Now()); got != (rate == 25) {
			t.Fatalf("idle=%v met=%v", rate, got)
		}
		if rate < 25 && primary.floorState.Kind != "revoked" {
			t.Fatal("hysteresis-band idle result cleared revocation")
		}
	}
	smartTestRestoreEpisodeEvidence(t, s, clock.Now())
	if primary.floorState.Kind != "" || !s.meetsFloorLocked(primary, clock.Now()) {
		t.Fatal("full-target recovery did not persist clearing")
	}
}

func TestSmartProfileDegradationStaleCandidateCannotDriveOrdinarySwitch(t *testing.T) {
	s, clock := smartProfileThroughputTestGroup(t)
	s.preference.confirmations = 1
	s.selectedAt = clock.Now().Add(-time.Hour)
	primary, challenger := s.primary, s.standby
	clock.Advance(time.Second)
	e := smartTestDegradationEpisode(s, clock.Now())
	smartTestDegradationResult(s, e, primary, smartDegradationConfirmation, 3, clock.Now())
	clock.Advance(time.Second)
	smartTestDegradationResult(s, e, challenger, smartDegradationChallenger, 10, clock.Now())
	if !s.downloadStaleAfterRevocationLocked(challenger) {
		t.Fatal("pre-revocation candidate download remained current for scheduling")
	}
	if _, fresh := s.effectiveDownloadLocked(challenger, clock.Now()); fresh || s.meetsFloorLocked(challenger, clock.Now()) {
		t.Fatal("pre-revocation candidate retained profile credit")
	}
	s.chooseLocked(clock.Now())
	if s.primary != primary {
		t.Fatal("ordinary chooser switched on stale candidate evidence")
	}
	primary.lastThroughput = clock.Now()
	s.scheduleThroughputLocked(clock.Now())
	if s.admission == nil || s.admission.job.member != challenger || s.admission.job.plan.Download == nil {
		t.Fatal("ordinary scheduler skipped TTL-fresh pre-revocation challenger download")
	}
}

func TestSmartDegradationOrdinaryChooserSuppressedOnTickAndHeartbeat(t *testing.T) {
	for _, dispatch := range []string{"tick", "heartbeat"} {
		t.Run(dispatch, func(t *testing.T) {
			s, clock := smartProfileThroughputTestGroup(t)
			primary := s.primary
			primary.throughput.Download = 3
			s.preference.confirmations = 1
			s.selectedAt = clock.Now().Add(-time.Hour)
			e := smartTestDegradationEpisode(s, clock.Now())
			smartTestDegradationResult(s, e, primary, smartDegradationConfirmation, 3, clock.Now())
			if dispatch == "tick" {
				s.tickLocked(clock.Now())
			} else {
				s.applyResultLocked(smartResult{member: s.standby, kind: "tcp", rtt: 10 * time.Millisecond, generation: s.generation, configGeneration: s.configGeneration, memberGeneration: s.standby.generation}, clock.Now())
			}
			if s.primary != primary || s.degradation.episode != e {
				t.Fatal("ordinary improvement bypassed pending complete plan")
			}
		})
	}
}

func TestSmartDegradationLegacyObservationsPreserveClassification(t *testing.T) {
	for _, scenario := range []struct {
		name                          string
		a1, c                         float64
		outcome                       string
		primaryClass, challengerClass int
	}{
		{"challenger_slow", 3, 10, "shared_bottleneck_suspected", 5, 4},
		{"switched", 3, 25, "switched", 5, 0},
		{"primary_fast", 30, 0, "primary_fast", 0, 4},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			s, clock := smartDegradationTestGroup(t)
			primary, challenger := s.primary, s.standby
			primary.throughput.Download, challenger.throughput.Download = 3, 10
			for _, member := range s.members {
				stat := s.history.statistic(s.network.Key, member.identity, clock.Now())
				stat.Download, stat.ThroughputAt = member.throughput.Download, member.throughput.At.Unix()
			}
			clock.Advance(time.Second)
			e := smartTestDegradationEpisode(s, clock.Now())
			smartTestDegradationResult(s, e, primary, smartDegradationConfirmation, scenario.a1, clock.Now())
			if scenario.c > 0 {
				clock.Advance(time.Second)
				smartTestDegradationResult(s, e, challenger, smartDegradationChallenger, scenario.c, clock.Now())
			}
			if s.degradation.state != scenario.outcome {
				t.Fatalf("outcome=%s", s.degradation.state)
			}
			if primary.throughput.Download != 3 || challenger.throughput.Download != 10 {
				t.Fatal("legacy episode overwrote regular download")
			}
			for _, restore := range []bool{false, true} {
				if restore {
					smartTestRestoreEpisodeEvidence(t, s, clock.Now())
				}
				if got := s.throughputClass(primary, clock.Now()); got != scenario.primaryClass {
					t.Fatalf("primary class=%d after restore=%v", got, restore)
				}
				if got := s.throughputClass(challenger, clock.Now()); got != scenario.challengerClass {
					t.Fatalf("challenger class=%d after restore=%v", got, restore)
				}
			}
			if scenario.name == "primary_fast" {
				s.preference.confirmations = 1
				s.selectedAt = clock.Now().Add(-time.Hour)
				s.chooseLocked(clock.Now())
				if s.primary != primary {
					t.Fatal("legacy chooser used obsolete primary magnitude")
				}
			}
		})
	}
}

func TestSmartDegradationFastCooldownAndV1Status(t *testing.T) {
	s, clock := smartDegradationTestGroup(t)
	e := smartTestDegradationEpisode(s, clock.Now())
	smartTestDegradationResult(s, e, s.primary, smartDegradationConfirmation, 50, clock.Now())
	if s.degradation.retry.Sub(clock.Now()) != time.Hour || s.degradation.episode != nil {
		t.Fatal("fast primary lost cooldown")
	}
	text, err := SmartStatus(context.Background(), smartTestManager{values: []adapter.Outbound{s}}, "")
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err = json.Unmarshal([]byte(text), &document); err != nil {
		t.Fatal(err)
	}
	if document["version"] != float64(1) || document["throughput_degradation"] == nil {
		t.Fatal("status envelope changed")
	}
}

func TestSmartProfileDegradationTargetOneHundred(t *testing.T) {
	s, clock := smartProfileThroughputTestGroup(t)
	s.targetMbps = 100
	primary, challenger := s.primary, s.standby
	clock.Advance(time.Second)
	e := smartTestDegradationEpisode(s, clock.Now())
	smartTestDegradationResult(s, e, primary, smartDegradationConfirmation, 20, clock.Now())
	clock.Advance(time.Second)
	smartTestDegradationResult(s, e, challenger, smartDegradationChallenger, 100, clock.Now())
	if s.primary != challenger || challenger.floorState.Target != 100 || !s.meetsFloorLocked(challenger, clock.Now()) {
		t.Fatal("target 100 did not produce a floor-only switch")
	}
}

func TestSmartProfileDegradationProbeSafety(t *testing.T) {
	for _, change := range []string{"unmeasured", "suspect", "other_tier", "below_minimum"} {
		t.Run(change, func(t *testing.T) {
			s, clock := smartProfileThroughputTestGroup(t)
			s.preference.minStability = 2
			challenger := s.standby
			challenger.throughput = smartThroughput{}
			switch change {
			case "suspect":
				challenger.state = "suspect"
			case "other_tier":
				challenger.tier++
			case "below_minimum":
				stat := s.history.statistic(s.network.Key, challenger.identity, clock.Now())
				stat.S, stat.W, stat.UpdatedAt = 500, 1000, clock.Now().Unix()
			}
			eligible := change == "unmeasured"
			if got := s.degradationCandidateLocked(clock.Now()) == challenger; got != eligible {
				t.Fatalf("candidate eligibility=%v, want %v", got, eligible)
			}
			e := smartTestDegradationEpisode(s, clock.Now())
			e.challenger, e.challengerGeneration = challenger, challenger.generation
			job := smartJob{member: challenger, ctx: e.ctx, episode: e, purpose: smartDegradationChallenger}
			if got := s.degradationJobValid(job); got != eligible {
				t.Fatalf("execution eligibility=%v, want %v", got, eligible)
			}
		})
	}
}

func TestSmartDegradationStaleCompletionCannotClearNewOwner(t *testing.T) {
	s, clock := smartDegradationTestGroup(t)
	old := smartTestDegradationEpisode(s, clock.Now())
	s.clearDegradationLocked()
	current := smartTestDegradationEpisode(s, clock.Now())
	s.primary.degradationQueued, s.primary.queued["throughput"] = current, true
	s.applyResultLocked(smartResult{member: s.primary, kind: "throughput", episode: old, purpose: smartDegradationConfirmation, err: context.Canceled}, clock.Now())
	if s.degradation.episode != current || s.primary.degradationQueued != current || !s.primary.queued["throughput"] {
		t.Fatal("late result cleared newer episode ownership")
	}
}

func TestSmartDegradationDelayedAdmissionFencing(t *testing.T) {
	for _, change := range []string{"key", "away-back", "power", "pause", "member", "config"} {
		t.Run(change, func(t *testing.T) {
			s, clock := smartDegradationTestGroup(t)
			e := smartTestDegradationEpisode(s, clock.Now())
			admission := &smartAdmission{job: smartJob{member: s.primary, kind: "throughput", ctx: e.ctx, episode: e, purpose: smartDegradationConfirmation, generation: s.generation, configGeneration: s.configGeneration, memberGeneration: s.primary.generation, plan: e.plan}, ack: make(chan bool, 1)}
			s.admission = admission
			switch change {
			case "key":
				s.network.Key += "/tower:new"
			case "away-back":
				primary := s.primary
				s.switchLocked(s.standby, "switch", "test", clock.Now())
				s.switchLocked(primary, "switch", "test", clock.Now())
			case "power":
				s.network.PowerSave = true
			case "pause":
				s.paused = true
			case "member":
				s.primary.generation++
			case "config":
				s.configGeneration++
			}
			s.applyResultLocked(smartResult{kind: "admission", admission: admission}, clock.Now())
			if <-admission.ack || len(s.pending) != 0 {
				t.Fatal("stale admission queued episode work")
			}
		})
	}
}

func TestSmartDegradationRotationAcrossCooldown(t *testing.T) {
	s, clock := smartDegradationTestGroup(t)
	primary, b := s.primary, s.standby
	c := &smartMember{tag: "c", identity: "c", serverIdentity: "c", outbound: &smartFakeOutbound{"c"}, tier: 1, generation: 1, state: "healthy", queued: make(map[string]bool), throughput: primary.throughput}
	s.members = append(s.members, c)
	e := smartTestDegradationEpisode(s, clock.Now())
	smartTestDegradationResult(s, e, primary, smartDegradationConfirmation, 3, clock.Now())
	if e.challenger != b || !b.queued["throughput"] {
		t.Fatal("first untried challenger was not queued")
	}
	// Complete the queued phase through its normal accounting ownership.
	s.pending = nil
	e.accountingWait.Done()
	smartTestDegradationResult(s, e, b, smartDegradationChallenger, 3, clock.Now())
	if s.degradation.episode != nil || s.primary != primary {
		t.Fatal("slow challenger did not finish without switching")
	}
	clock.Advance(time.Hour)
	e = smartTestDegradationEpisode(s, clock.Now())
	smartTestDegradationResult(s, e, primary, smartDegradationConfirmation, 3, clock.Now())
	if e.challenger != c || !c.queued["throughput"] {
		t.Fatal("failed B starved untried fast C after cooldown")
	}
	s.pending = nil
	e.accountingWait.Done()
	smartTestDegradationResult(s, e, c, smartDegradationChallenger, 50, clock.Now())
	if s.primary != c {
		t.Fatal("fresh fast C did not waive dwell")
	}
}
