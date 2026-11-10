package group

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
)

func TestSmartPassiveFiveSecondDiagnosticsAndExpiry(t *testing.T) {
	s, clock := smartDegradationTestGroup(t)
	member := s.primary
	before := member.throughput
	for second := range 5 {
		// The five complete bins total 15 MB read and 1.5 MB written.
		s.countPassiveLocked(member, int64(second+1)*1_000_000, int64(second+1)*100_000, clock.Now())
		clock.Advance(time.Second)
	}
	down, up := s.passiveDiagnosticsLocked(member, clock.Now())
	if down != 24 || up != 2.4 {
		t.Fatalf("diagnostics=(%v,%v), want (24,2.4)", down, up)
	}
	if member.throughput != before {
		t.Fatal("passive diagnostics modified regular capacity evidence")
	}
	s.countPassiveLocked(member, 500_000_000, 500_000_000, clock.Now())
	if gotDown, gotUp := s.passiveDiagnosticsLocked(member, clock.Now()); gotDown != down || gotUp != up {
		t.Fatal("unfinished second entered five-second diagnostics")
	}
	clock.Advance(6*time.Hour + 5*time.Second)
	if down, up = s.passiveDiagnosticsLocked(member, clock.Now()); down != 0 || up != 0 {
		t.Fatal("expired use observations survived six-hour horizon")
	}
}

func TestSmartPassivePeakFallsBackToRetainedWindow(t *testing.T) {
	s, clock := smartDegradationTestGroup(t)
	start := clock.Now()
	for range 5 {
		s.countPassiveLocked(s.primary, 5_000_000, 0, clock.Now())
		clock.Advance(time.Second)
	}
	clock.Advance(time.Hour)
	for range 5 {
		s.countPassiveLocked(s.primary, 1_000_000, 0, clock.Now())
		clock.Advance(time.Second)
	}
	if down, _ := s.passiveDiagnosticsLocked(s.primary, clock.Now()); down != 40 {
		t.Fatalf("peak=%v, want 40", down)
	}
	clock.Advance(start.Add(6*time.Hour + 5*time.Second).Sub(clock.Now()))
	if down, _ := s.passiveDiagnosticsLocked(s.primary, clock.Now()); down != 8 {
		t.Fatalf("retained peak=%v, want 8", down)
	}
}

func TestSmartPassiveBufferDrainNeverGrantsRankedCredit(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		s, clock := smartProfileThroughputTestGroup(t)
		s.preference.legacy = legacy
		candidate := s.standby
		candidate.throughput = smartThroughput{}
		beforeClass := s.throughputClass(candidate, clock.Now())
		for range 5 {
			s.countPassiveLocked(candidate, 100_000_000, 100_000_000, clock.Now())
			clock.Advance(time.Second)
		}
		if _, fresh := smartFreshDownload(candidate, clock.Now()); fresh {
			t.Fatal("buffer drain created fresh active download")
		}
		if s.meetsFloorLocked(candidate, clock.Now()) || s.throughputClass(candidate, clock.Now()) != beforeClass {
			t.Fatal("passive bytes changed floor or legacy class")
		}
		if candidate.improvementObservations != 0 {
			t.Fatal("passive bytes counted as confirmations")
		}
		if plan := s.regularMeasurementPlanLocked(candidate, clock.Now()); plan.Download == nil {
			t.Fatal("passive bytes skipped needed active download")
		}
		candidate.throughput = smartThroughput{Download: 1000, At: clock.Now(), Source: "passive"}
		if _, fresh := smartFreshDownload(candidate, clock.Now()); fresh {
			t.Fatal("pre-v2 passive source retained active freshness")
		}
		if s.meetsFloorLocked(candidate, clock.Now()) || s.throughputClass(candidate, clock.Now()) != 1 {
			t.Fatal("pre-v2 passive source retained ranked credit")
		}
		if plan := s.regularMeasurementPlanLocked(candidate, clock.Now()); plan.Download == nil {
			t.Fatal("pre-v2 passive source skipped an active test")
		}
	}
}

func TestSmartPassiveDiagnosticsAppearInStatus(t *testing.T) {
	s, clock := smartDegradationTestGroup(t)
	for range 5 {
		s.countPassiveLocked(s.primary, 1_000_000, 500_000, clock.Now())
		clock.Advance(time.Second)
	}
	text, err := SmartStatus(context.Background(), smartTestManager{values: []adapter.Outbound{s}}, "")
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Members []struct {
			Tag  string  `json:"tag"`
			Down float64 `json:"observed_peak_down"`
			Up   float64 `json:"offered_up"`
		} `json:"members"`
	}
	if err := json.Unmarshal([]byte(text), &document); err != nil {
		t.Fatal(err)
	}
	for _, member := range document.Members {
		if member.Tag == s.primary.tag {
			if member.Down != 8 || member.Up != 4 {
				t.Fatalf("status diagnostics=(%v,%v), want (8,4)", member.Down, member.Up)
			}
			return
		}
	}
	t.Fatal("primary missing from status")
}

func TestSmartFloorProofPrecedence(t *testing.T) {
	for _, scenario := range []struct {
		name                 string
		age                  time.Duration
		proofTarget, regular float64
		newer                bool
		met                  bool
	}{
		{"unmeasured", 0, 25, 0, false, true},
		{"expires_at_24h", 24 * time.Hour, 25, 0, false, false},
		{"target_raised", 0, 20, 0, false, false},
		{"newer_slow_regular", 0, 25, 3, true, false},
		{"older_slow_regular", 0, 25, 3, false, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			s, clock := smartProfileThroughputTestGroup(t)
			member := s.standby
			member.throughput = smartThroughput{}
			at := clock.Now().Add(-scenario.age)
			member.floorState = smartFloorState{Kind: "proven", Target: scenario.proofTarget, At: at, Value: 25}
			if scenario.regular > 0 {
				regularAt := at.Add(-time.Second)
				if scenario.newer {
					regularAt = at.Add(time.Second)
					clock.Advance(time.Second)
				}
				member.throughput = smartThroughput{Download: scenario.regular, At: regularAt, Source: "active"}
			}
			if got := s.meetsFloorLocked(member, clock.Now()); got != scenario.met {
				t.Fatalf("floor=%v, want %v", got, scenario.met)
			}
		})
	}
}

func TestSmartFloorNewestEpisodeRecordReplacesPrevious(t *testing.T) {
	s, clock := smartProfileThroughputTestGroup(t)
	primary, challenger := s.primary, s.standby
	clock.Advance(time.Second)
	challenger.floorState = smartFloorState{Kind: "revoked", Target: 25, At: clock.Now(), Value: 3}
	clock.Advance(time.Second)
	e := smartTestDegradationEpisode(s, clock.Now())
	smartTestDegradationResult(s, e, primary, smartDegradationConfirmation, 3, clock.Now())
	clock.Advance(time.Second)
	smartTestDegradationResult(s, e, challenger, smartDegradationChallenger, 25, clock.Now())
	if challenger.floorState.Kind != "proven" || !s.meetsFloorLocked(challenger, clock.Now()) {
		t.Fatal("new proof did not replace revocation")
	}
	smartTestRestoreEpisodeEvidence(t, s, clock.Now())
	if challenger.floorState.Kind != "proven" || !s.meetsFloorLocked(challenger, clock.Now()) {
		t.Fatal("restore revived replaced revocation")
	}
	clock.Advance(time.Second)
	e = smartTestDegradationEpisode(s, clock.Now())
	smartTestDegradationResult(s, e, challenger, smartDegradationConfirmation, 3, clock.Now())
	clock.Advance(time.Second)
	smartTestDegradationResult(s, e, primary, smartDegradationChallenger, 10, clock.Now())
	if challenger.floorState.Kind != "revoked" || s.meetsFloorLocked(challenger, clock.Now()) {
		t.Fatal("new revocation did not replace proof")
	}
	smartTestRestoreEpisodeEvidence(t, s, clock.Now())
	if challenger.floorState.Kind != "revoked" || s.meetsFloorLocked(challenger, clock.Now()) {
		t.Fatal("restore revived replaced proof")
	}
}

func TestSmartLegacyEpisodeObservationUsesIndependentAge(t *testing.T) {
	s, clock := smartDegradationTestGroup(t)
	member := s.primary
	member.throughput = smartThroughput{Download: 50, At: clock.Now().Add(-time.Hour), Source: "active"}
	member.legacyEpisodeObs = smartLegacyEpisodeObservation{Value: 3, At: clock.Now()}
	if got := s.throughputClass(member, clock.Now()); got != 5 {
		t.Fatalf("latest episode class=%d, want 5", got)
	}
	clock.Advance(time.Second)
	member.throughput = smartThroughput{Download: 15, At: clock.Now(), Source: "active"}
	if got := s.throughputClass(member, clock.Now()); got != 3 {
		t.Fatalf("newer regular class=%d, want 3", got)
	}
	member.throughput = smartThroughput{}
	clock.Advance(24 * time.Hour)
	if got := s.throughputClass(member, clock.Now()); got != 1 {
		t.Fatalf("expired episode class=%d, want unknown 1", got)
	}
	stat := s.history.statistic(s.network.Key, member.identity, clock.Now())
	stat.LegacyEpisodeObs = member.legacyEpisodeObs
	smartTestRestoreEpisodeEvidence(t, s, clock.Now())
	if got := s.throughputClass(member, clock.Now()); got != 1 {
		t.Fatalf("restored expired episode class=%d, want unknown 1", got)
	}
}

func TestSmartRevocationSchedulingPersistsAfterSwitch(t *testing.T) {
	s, clock := smartProfileThroughputTestGroup(t)
	primary, challenger := s.primary, s.standby
	third := &smartMember{identity: "c", throughput: primary.throughput}
	s.members = append(s.members, third)
	clock.Advance(time.Second)
	e := smartTestDegradationEpisode(s, clock.Now())
	smartTestDegradationResult(s, e, primary, smartDegradationConfirmation, 3, clock.Now())
	clock.Advance(time.Second)
	smartTestDegradationResult(s, e, challenger, smartDegradationChallenger, 25, clock.Now())
	if !s.downloadStaleAfterRevocationLocked(third) {
		t.Fatal("switch erased network refresh requirement for other old records")
	}
	if !s.meetsFloorLocked(challenger, clock.Now()) {
		t.Fatal("ordinary refresh requirement erased independent proof")
	}
}

func TestSmartDegradationNormallyInvalidChallengerCommitsValidPrimary(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		s, clock := smartProfileThroughputTestGroup(t)
		s.preference.legacy = legacy
		primary, challenger := s.primary, s.standby
		clock.Advance(time.Second)
		e := smartTestDegradationEpisode(s, clock.Now())
		smartTestDegradationResult(s, e, primary, smartDegradationConfirmation, 3, clock.Now())
		clock.Advance(time.Second)
		smartTestDegradationResult(s, e, challenger, smartDegradationChallenger, 0, clock.Now())
		if s.degradation.state != "inconclusive" || primary.floorState.Kind != "revoked" || primary.downloadUnderLoad.Value != 3 || challenger.downloadUnderLoad.Valid {
			t.Fatal("completed invalid C did not commit only valid A1")
		}
		if legacy && primary.legacyEpisodeObs.Value != 3 {
			t.Fatal("completed invalid C lost valid legacy A1")
		}
		if primary.throughput.Download != 50 || challenger.throughput.Download != 50 {
			t.Fatal("inconclusive plan replaced regular downloads")
		}
	}
}

func TestSmartDegradationBufferedUserReadsNeverBecomeEpisodeEvidence(t *testing.T) {
	s, clock := smartProfileThroughputTestGroup(t)
	primary, challenger := s.primary, s.standby
	clock.Advance(time.Second)
	e := smartTestDegradationEpisode(s, clock.Now())
	s.countPassiveLocked(primary, 500_000_000, 0, clock.Now())
	smartTestDegradationResult(s, e, primary, smartDegradationConfirmation, 3, clock.Now())
	clock.Advance(time.Second)
	s.countPassiveLocked(primary, 500_000_000, 0, clock.Now())
	smartTestDegradationResult(s, e, challenger, smartDegradationChallenger, 10, clock.Now())
	if s.degradation.state != "challenger_slow" || primary.downloadUnderLoad.Value != 3 || challenger.downloadUnderLoad.Value != 10 {
		t.Fatal("buffered user reads inflated staged active measurements")
	}
}
