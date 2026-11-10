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
	e := &smartDegradationEpisode{primary: s.primary, key: s.network.Key, generation: s.generation, configGeneration: s.configGeneration, memberGeneration: s.primary.generation, epoch: s.primaryEpoch, ctx: ctx, cancel: cancel, until: now.Add(5 * time.Minute)}
	s.degradation.episode = e
	return e
}

func smartTestDegradationResult(s *Smart, e *smartDegradationEpisode, m *smartMember, purpose string, rate float64, now time.Time) {
	if m.queued == nil {
		m.queued = make(map[string]bool)
	}
	m.queued["throughput"] = true
	m.degradationQueued = e
	s.applyResultLocked(smartResult{member: m, kind: "throughput", episode: e, purpose: purpose, download: rate, generation: e.generation, configGeneration: e.configGeneration, memberGeneration: m.generation}, now)
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

func TestSmartDegradationSustainedDemand(t *testing.T) {
	for _, pattern := range []string{"slow", "sufficient", "chat", "gaps", "short"} {
		t.Run(pattern, func(t *testing.T) {
			s, clock := smartDegradationTestGroup(t)
			l := &smartLease{group: s, member: s.primary, generation: s.generation, started: clock.Now().Add(-10 * time.Second), lastRead: clock.Now()}
			s.leases[s.primary] = map[*smartLease]struct{}{l: {}}
			if pattern == "short" {
				l.started = clock.Now().Add(time.Hour)
			}
			s.scheduleDegradationLocked(clock.Now())
			for i := range 21 {
				clock.Advance(time.Second)
				n := 375000
				switch pattern {
				case "sufficient":
					n = 4000000
				case "chat":
					n = 100
				case "gaps":
					if i%5 == 0 || i%5 == 1 || i%5 == 2 {
						n = 0
					}
				}
				if n > 0 {
					l.CountRead(n)
				}
				s.scheduleDegradationLocked(clock.Now())
			}
			if pattern == "slow" && s.degradation.episode == nil {
				t.Fatal("sustained 3 Mbit/s demand did not create episode")
			}
			if pattern != "slow" && s.degradation.episode != nil {
				t.Fatal("app-limited or sufficient traffic created episode")
			}
			if s.primary.throughput.Download != 50 {
				t.Fatal("passive evidence demoted primary")
			}
		})
	}
}

func TestSmartDegradationProvisionalBlocksAgedDwell(t *testing.T) {
	s, clock := smartDegradationTestGroup(t)
	primary := s.primary
	s.selectedAt = clock.Now().Add(-time.Hour)
	e := smartTestDegradationEpisode(s, clock.Now())
	smartTestDegradationResult(s, e, primary, smartDegradationConfirmation, 3, clock.Now())
	if primary.throughput.Download != 50 {
		t.Fatal("low confirmation was published")
	}
	for range 3 {
		clock.Advance(30 * time.Second)
		s.standby.lastOK = clock.Now()
		s.chooseLocked(clock.Now())
	}
	if s.primary != primary {
		t.Fatal("stale standby bypassed fresh challenger")
	}
	smartTestDegradationResult(s, e, s.standby, smartDegradationChallenger, 3, clock.Now())
	if s.primary != primary || primary.throughput.Download != 50 || s.standby.throughput.Download != 50 {
		t.Fatal("both-slow pair affected ordinary ranking")
	}
}

func TestSmartDegradationRotationAcrossCooldown(t *testing.T) {
	s, clock := smartDegradationTestGroup(t)
	primary, b := s.primary, s.standby
	c := &smartMember{tag: "c", identity: "c", serverIdentity: "c", outbound: &smartFakeOutbound{"c"}, tier: 1, generation: 1, state: "healthy", queued: make(map[string]bool), throughput: primary.throughput}
	s.members = append(s.members, c)
	e := smartTestDegradationEpisode(s, clock.Now())
	smartTestDegradationResult(s, e, primary, smartDegradationConfirmation, 3, clock.Now())
	if e.challenger != b {
		t.Fatal("first untried challenger missing")
	}
	s.degradation.attempts = map[string]time.Time{b.identity: clock.Now()}
	s.lastChallenger = clock.Now()
	smartTestDegradationResult(s, e, b, smartDegradationChallenger, 3, clock.Now())
	clock.Advance(time.Hour)
	e = smartTestDegradationEpisode(s, clock.Now())
	smartTestDegradationResult(s, e, primary, smartDegradationConfirmation, 3, clock.Now())
	if e.challenger != c {
		t.Fatal("failed B starved untried fast C")
	}
	smartTestDegradationResult(s, e, c, smartDegradationChallenger, 50, clock.Now())
	if s.primary != c {
		t.Fatal("fresh fast C did not waive dwell")
	}
}

func TestSmartDegradationEpisodeInvalidation(t *testing.T) {
	for _, change := range []string{"key", "generation", "config", "member", "away-back", "pause", "closed"} {
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
			case "closed":
				s.closed = true
			}
			smartTestDegradationResult(s, e, primary, smartDegradationConfirmation, 3, clock.Now())
			if s.degradation.episode != nil || primary.degradationQueued != nil || primary.throughput.Download != 50 {
				t.Fatal("invalid episode retained ownership or published result")
			}
			s.closed = false
		})
	}
}

func TestSmartDegradationNeutralCompletion(t *testing.T) {
	for _, contained := range []bool{false, true} {
		s, clock := smartDegradationTestGroup(t)
		e := smartTestDegradationEpisode(s, clock.Now())
		m := s.primary
		m.degradationQueued = e
		m.queued["throughput"] = true
		s.applyResultLocked(smartResult{member: m, kind: "throughput", episode: e, purpose: smartDegradationConfirmation, err: context.Canceled, contained: contained}, clock.Now())
		if m.degradationQueued != nil || m.queued["throughput"] || s.degradation.episode != nil || m.state != "healthy" || m.throughput.Download != 50 {
			t.Fatal("neutral completion leaked state or changed health")
		}
	}
}

func TestSmartDegradationHeartbeatLifetime(t *testing.T) {
	s, clock := smartDegradationTestGroup(t)
	s.workCtx, s.workCancel = context.WithCancel(s.ctx)
	s.backgroundCtx, s.backgroundCancel = context.WithCancel(s.workCtx)
	for _, purpose := range []string{smartDegradationConfirmation, smartDegradationChallenger} {
		e := smartTestDegradationEpisode(s, clock.Now())
		e.ctx, e.cancel = context.WithCancel(s.workCtx)
		s.preemptBackgroundLocked(smartJob{kind: "tcp", urgent: true, member: s.primary})
		if e.ctx.Err() != nil {
			t.Fatal("heartbeat cancelled degradation job")
		}
		job := smartJob{ctx: e.ctx, episode: e, purpose: purpose}
		s.clearDegradationLocked()
		if !errors.Is(job.ctx.Err(), context.Canceled) {
			t.Fatal("invalidation did not cancel job")
		}
	}
}

func TestSmartDegradationFastCooldownAndV1Status(t *testing.T) {
	s, clock := smartDegradationTestGroup(t)
	e := smartTestDegradationEpisode(s, clock.Now())
	smartTestDegradationResult(s, e, s.primary, smartDegradationConfirmation, 50, clock.Now())
	if s.degradation.retry.Sub(clock.Now()) != time.Hour || s.degradation.episode != nil {
		t.Fatal("fast proof did not receive purpose cooldown")
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
		t.Fatal("status contract changed")
	}
}

func TestSmartDegradationDelayedAdmissionFencing(t *testing.T) {
	for _, change := range []string{"key", "away-back", "power", "pause", "member", "config"} {
		t.Run(change, func(t *testing.T) {
			s, clock := smartDegradationTestGroup(t)
			e := smartTestDegradationEpisode(s, clock.Now())
			id, capBytes, err := s.ledger.ReserveDegradation(s.ctx, "wifi", clock.Now(), 0, smartDegradationConfirmation, 10_000_000)
			if err != nil {
				t.Fatal(err)
			}
			admission := &smartAdmission{job: smartJob{member: s.primary, kind: "throughput", ctx: e.ctx, episode: e, purpose: smartDegradationConfirmation, generation: s.generation, configGeneration: s.configGeneration, memberGeneration: s.primary.generation, reservation: id, capBytes: capBytes}, ack: make(chan bool, 1)}
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
				t.Fatal("stale or disabled admission queued degradation test")
			}
			if err = s.ledger.Reconcile(s.ctx, id, 0, false); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSmartDegradationPurposeDenialDoesNotBlockCalibration(t *testing.T) {
	s, clock := smartDegradationTestGroup(t)
	e := smartTestDegradationEpisode(s, clock.Now())
	admission := &smartAdmission{job: smartJob{member: s.primary, kind: "throughput", ctx: e.ctx, episode: e, purpose: smartDegradationConfirmation, generation: s.generation, configGeneration: s.configGeneration, memberGeneration: s.primary.generation, networkType: "wifi"}, ack: make(chan bool, 1)}
	s.admission = admission
	s.applyResultLocked(smartResult{kind: "admission", admission: admission, err: errSmartDegradationBudget}, clock.Now())
	if <-admission.ack || !s.admissionRetryAt.IsZero() {
		t.Fatal("purpose allowance denial blocked all calibration")
	}
	s.startThroughputAdmissionLocked(s.primary, clock.Now())
	if s.admission == nil {
		t.Fatal("ordinary calibration did not enter admission")
	}
	s.clearDegradationLocked()
	s.cancel()
}

func TestSmartDegradationStaleCompletionCannotClearNewOwner(t *testing.T) {
	s, clock := smartDegradationTestGroup(t)
	old := smartTestDegradationEpisode(s, clock.Now())
	s.clearDegradationLocked()
	current := smartTestDegradationEpisode(s, clock.Now())
	s.primary.degradationQueued = current
	s.primary.queued["throughput"] = true
	s.applyResultLocked(smartResult{member: s.primary, kind: "throughput", episode: old, purpose: smartDegradationConfirmation, err: context.Canceled}, clock.Now())
	if s.degradation.episode != current || s.primary.degradationQueued != current || !s.primary.queued["throughput"] {
		t.Fatal("late result cleared new episode ownership")
	}
}

func TestSmartDegradationSupervisorRestartClearsOwnership(t *testing.T) {
	s, clock := smartDegradationTestGroup(t)
	episode := smartTestDegradationEpisode(s, clock.Now())
	s.primary.degradationQueued = episode
	s.primary.queued["throughput"] = true
	s.testHook = func(site string) {
		if site == "tick" {
			panic("degradation restart")
		}
	}
	s.loopDone = make(chan struct{})
	go s.loop(s.ctx)
	smartAwaitGroup(t, s, clock, func() bool { return s.generationContained })
	s.mu.Lock()
	cleared := s.degradation.episode == nil && s.primary.degradationQueued == nil && !s.primary.queued["throughput"]
	s.mu.Unlock()
	if !cleared || episode.ctx.Err() == nil {
		t.Fatal("restart retained degradation owner")
	}
}

func TestSmartDegradationContainedJobUsesNormalSettlement(t *testing.T) {
	s, clock := smartDegradationTestGroup(t)
	e := smartTestDegradationEpisode(s, clock.Now())
	id, capBytes, err := s.ledger.ReserveDegradation(s.ctx, "wifi", clock.Now(), 0, smartDegradationConfirmation, 10_000_000)
	if err != nil {
		t.Fatal(err)
	}
	s.primary.degradationQueued = e
	s.primary.queued["throughput"] = true
	s.testHook = func(site string) {
		if site == "probe/throughput" {
			panic("degradation measurement")
		}
	}
	result := s.runJob(smartJob{member: s.primary, kind: "throughput", ctx: e.ctx, timeout: 30 * time.Second, episode: e, purpose: smartDegradationConfirmation, generation: s.generation, configGeneration: s.configGeneration, memberGeneration: s.primary.generation, networkType: "wifi", reservation: id, capBytes: capBytes})
	if !result.contained {
		t.Fatal("new operation bypassed containment")
	}
	s.applyResultLocked(result, clock.Now())
	budget := s.ledger.Snapshot("wifi", clock.Now(), 0)
	if budget.ReservedMB != 0 || budget.UsedMB != 10 || budget.Degradation.UsedMB != 10 || s.degradation.episode != nil || s.primary.state != "healthy" {
		t.Fatalf("contained result did not settle neutrally: %+v", budget)
	}
}

func TestSmartDegradationChallengerAdmissionPersistsPurposeCooldown(t *testing.T) {
	s, clock := smartDegradationTestGroup(t)
	e := smartTestDegradationEpisode(s, clock.Now())
	smartTestDegradationResult(s, e, s.primary, smartDegradationConfirmation, 3, clock.Now())
	s.startDegradationAdmissionLocked(e.challenger, smartDegradationChallenger, e, clock.Now())
	result := smartLedgerWait(t, s.results)
	s.applyResultLocked(result, clock.Now())
	s.admissionWorkers.Wait()
	s.accountingWorkers.Wait()
	if len(s.pending) != 1 || s.pending[0].ctx != e.ctx || s.pending[0].episode != e {
		t.Fatal("challenger lost its episode lifetime at queue acceptance")
	}
	if s.ledger.DegradationCooldown(s.degradationChallengerCooldownKey()).Sub(clock.Now()) != time.Hour {
		t.Fatal("accepted challenger did not persist its hourly purpose cooldown")
	}
	if s.degradation.attempts[e.challenger.identity] != clock.Now() {
		t.Fatal("admission did not record challenger identity independently of published throughput")
	}
	s.clearPendingLocked()
	s.accountingWorkers.Wait()
	s.lastChallenger = time.Time{}
	s.network.Key += "/tower:new"
	if !s.degradationChallengerRetryLocked().After(clock.Now()) {
		t.Fatal("key-only change or volatile-state loss bypassed hourly challenger limit")
	}
}

func TestSmartDegradationSampleRecoveryResetsWindow(t *testing.T) {
	s, clock := smartDegradationTestGroup(t)
	lease := &smartLease{group: s, member: s.primary, generation: s.generation, started: clock.Now().Add(-10 * time.Second), lastRead: clock.Now()}
	s.leases[s.primary] = map[*smartLease]struct{}{lease: {}}
	s.scheduleDegradationLocked(clock.Now())
	for range 19 {
		clock.Advance(time.Second)
		lease.CountRead(1_625_000)
		s.scheduleDegradationLocked(clock.Now())
	}
	clock.Advance(time.Second)
	lease.CountRead(125_000)
	s.scheduleDegradationLocked(clock.Now())
	if s.degradation.episode != nil {
		t.Fatal("19 recovered samples followed by one slow sample qualified as sustained slow")
	}
	for range 18 {
		clock.Advance(time.Second)
		lease.CountRead(125_000)
		s.scheduleDegradationLocked(clock.Now())
	}
	if s.degradation.episode != nil {
		t.Fatal("less than twenty slow samples qualified")
	}
	clock.Advance(time.Second)
	lease.CountRead(125_000)
	s.scheduleDegradationLocked(clock.Now())
	if s.degradation.episode == nil {
		t.Fatal("twenty consecutive low samples did not qualify")
	}
}

func TestSmartDegradationUndersizedChallengerDoesNotConsumeTurn(t *testing.T) {
	s, clock := smartDegradationTestGroup(t)
	e := smartTestDegradationEpisode(s, clock.Now())
	smartTestDegradationResult(s, e, s.primary, smartDegradationConfirmation, 3, clock.Now())
	s.ledger.mu.Lock()
	s.ledger.state.Days[smartDegradationDayKey("wifi", clock.Now().Format("2006-01-02"))] = 498_790_000
	s.ledger.mu.Unlock()
	s.startDegradationAdmissionLocked(e.challenger, smartDegradationChallenger, e, clock.Now())
	result := smartLedgerWait(t, s.results)
	s.applyResultLocked(result, clock.Now())
	s.admissionWorkers.Wait()
	if !errors.Is(result.err, errSmartDegradationBudget) || len(s.pending) != 0 || len(s.degradation.attempts) != 0 || !s.lastChallenger.IsZero() || !s.ledger.DegradationCooldown(s.degradationChallengerCooldownKey()).IsZero() {
		t.Fatal("unmeasurable challenger consumed its turn or hourly cooldown")
	}
}

func TestSmartDegradationJitteredTicksReachSuspicion(t *testing.T) {
	s, clock := smartDegradationTestGroup(t)
	lease := &smartLease{group: s, member: s.primary, generation: s.generation, started: clock.Now().Add(-10 * time.Second), lastRead: clock.Now()}
	s.leases[s.primary] = map[*smartLease]struct{}{lease: {}}
	s.scheduleDegradationLocked(clock.Now())
	intervals := []time.Duration{900 * time.Millisecond, 1100 * time.Millisecond, 1400 * time.Millisecond, 2200 * time.Millisecond}
	start := clock.Now()
	for i := range 24 {
		elapsed := intervals[i%len(intervals)]
		clock.Advance(elapsed)
		lease.CountRead(int(187500 * elapsed / time.Second))
		s.scheduleDegradationLocked(clock.Now())
		if s.degradation.episode != nil {
			if clock.Now().Sub(start) < 20*time.Second {
				t.Fatal("jitter shortened sustained demand window")
			}
			return
		}
	}
	t.Fatalf("sustained 1.5 Mbps download stayed %q under jittered sampling", s.degradation.state)
}
