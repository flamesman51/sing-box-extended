package group

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
)

func smartProfileTestGroup(t *testing.T, profile string) (*Smart, *smartFakeClock) {
	t.Helper()
	s, clock := smartTestGroup()
	if err := json.Unmarshal([]byte(fmt.Sprintf(`{"preference":{"profile":%q}}`, profile)), &s.options); err != nil {
		t.Fatal(err)
	}
	if err := s.configure(); err != nil {
		t.Fatal(err)
	}
	return s, clock
}

func smartProfileSetStability(s *Smart, member *smartMember, probability float64, now time.Time) {
	stat := s.history.statistic(s.network.Key, member.identity, now)
	stat.S, stat.W = probability*10000, 10000
}

func TestSmartProfileBalanceRanking(t *testing.T) {
	for _, test := range []struct {
		profile string
		fast    float64
		want    string
	}{
		{"balance-speed-4k", 30, "b"}, {"server", 30, "b"},
		{"default", 30, "a"}, {"stability-downtime", 30, "a"},
		{"balance-speed-4k", 20, "b"}, {"server", 20, "b"},
	} {
		t.Run(fmt.Sprintf("%s/%g", test.profile, test.fast), func(t *testing.T) {
			s, clock := smartProfileTestGroup(t, test.profile)
			smartProfileSetStability(s, s.primary, 1, clock.Now())
			smartProfileSetStability(s, s.standby, .9, clock.Now())
			s.primary.throughput = smartThroughput{Download: 3, At: clock.Now(), Source: "active"}
			s.standby.throughput = smartThroughput{Download: test.fast, At: clock.Now(), Source: "active"}
			s.rankLocked(clock.Now())
			if got := s.members[0].tag; got != test.want {
				t.Fatalf("first=%s want=%s", got, test.want)
			}
		})
	}
}

func TestSmartProfileUnknownStillExplored(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "balance-speed-1080p")
	s.primary.throughput = smartThroughput{Download: 3, At: clock.Now(), Source: "active"}
	s.rankLocked(clock.Now())
	if s.members[0] != s.primary || !s.safeToProbeLocked(s.standby, s.primary, clock.Now()) {
		t.Fatal("measured below-floor member must rank first without blocking unknown exploration")
	}
}

func TestSmartProfileFloorHysteresisAndExpiry(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "balance-speed-4k")
	primary, candidate := s.primary, s.standby
	for _, rate := range []float64{25, 24, 21, 20, 24, 25} {
		primary.throughput = smartThroughput{Download: rate, At: clock.Now(), Source: "active"}
		candidate.throughput = primary.throughput
		if !s.meetsFloorLocked(primary, clock.Now()) {
			t.Fatalf("primary lost hysteresis credit at %g", rate)
		}
		if s.meetsFloorLocked(candidate, clock.Now()) != (rate >= 25) {
			t.Fatalf("challenger received hysteresis credit at %g", rate)
		}
	}
	clock.Advance(24 * time.Hour)
	if s.meetsFloorLocked(primary, clock.Now()) {
		t.Fatal("expired active measurement retained floor credit")
	}
	primary.throughput = smartThroughput{Download: 30, At: clock.Now(), Source: "passive"}
	clock.Advance(6 * time.Hour)
	if s.meetsFloorLocked(primary, clock.Now()) {
		t.Fatal("expired passive measurement retained floor credit")
	}
}

func TestSmartProfileSpeedMargin(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "speed-max")
	primary, candidate := s.primary, s.standby
	primary.throughput = smartThroughput{Download: 40, At: clock.Now(), Source: "active"}
	for _, test := range []struct {
		rate float64
		want bool
	}{{0, false}, {49, false}, {50, true}, {60, true}} {
		candidate.throughput = smartThroughput{}
		if test.rate > 0 {
			candidate.throughput = smartThroughput{Download: test.rate, At: clock.Now(), Source: "active"}
		}
		if got, _ := s.betterLocked(candidate, primary, clock.Now()); got != test.want {
			t.Fatalf("candidate=%g better=%v want=%v", test.rate, got, test.want)
		}
	}
}

func TestSmartProfileQuantiles(t *testing.T) {
	for size := 1; size <= 64; size++ {
		values := make([]float64, size)
		for i := range values {
			values[i] = float64(size - i)
		}
		p50, p90 := smartProfileRTTQuantiles(values)
		if p50 > p90 {
			t.Fatalf("n=%d p50=%g p90=%g", size, p50, p90)
		}
	}
	p50, p90 := smartProfileRTTQuantiles([]float64{40, 10, 30, 20})
	if p50 != 20 || p90 != 40 {
		t.Fatalf("nearest rank=%g/%g", p50, p90)
	}
	legacy50, legacy90 := smartRTTQuantiles([]float64{20, 10})
	if legacy50 != 20 || legacy90 != 10 {
		t.Fatal("legacy estimator changed")
	}
}

func TestSmartProfileConfirmationsUseDecisiveEvidence(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "responsive")
	primary, candidate := s.primary, s.standby
	s.selectedAt = clock.Now().Add(-time.Hour)
	primary.rtts = []float64{70, 70, 70}
	candidate.rtts = []float64{40, 40, 40}
	s.chooseLocked(clock.Now())
	if candidate.improvementObservations != 1 {
		t.Fatal("initial confirmation missing")
	}
	clock.Advance(30 * time.Second)
	candidate.throughput = smartThroughput{Download: 100, At: clock.Now(), Source: "active"}
	s.chooseLocked(clock.Now())
	if candidate.improvementObservations != 1 || s.primary != primary {
		t.Fatal("unrelated throughput confirmed latency")
	}
	candidate.lastOK = clock.Now()
	s.chooseLocked(clock.Now())
	if s.primary != candidate {
		t.Fatal("fresh latency evidence did not confirm switch")
	}
}

func TestSmartProfileLatencyBucketsDoNotOscillate(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "responsive")
	primary, candidate := s.primary, s.standby
	s.selectedAt = clock.Now().Add(-time.Hour)
	primary.rtts = []float64{50, 50, 50}
	for _, rtt := range []float64{51, 74, 49, 51, 49, 51} {
		clock.Advance(30 * time.Second)
		candidate.rtts = []float64{rtt, rtt, rtt}
		candidate.lastOK = clock.Now()
		s.chooseLocked(clock.Now())
		if s.primary != primary {
			t.Fatalf("bucket oscillation switched at %g", rtt)
		}
	}
}

func TestSmartProfileConfirmationReset(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "responsive")
	primary, b := s.primary, s.standby
	c := &smartMember{tag: "c", identity: "c", serverIdentity: "c", state: "healthy", tier: 1, generation: 1, configOrder: 2, lastOK: clock.Now()}
	s.members = append(s.members, c)
	primary.rtts = []float64{100, 100, 100}
	b.rtts = []float64{60, 60, 60}
	c.rtts = []float64{80, 80, 80}
	s.chooseLocked(clock.Now())
	clock.Advance(30 * time.Second)
	c.rtts = []float64{30, 30, 30}
	c.lastOK = clock.Now()
	s.chooseLocked(clock.Now())
	if b.improvementObservations != 0 || c.improvementObservations != 1 {
		t.Fatal("candidate change retained confirmations")
	}
	primary.rtts = []float64{30, 30, 30}
	smartProfileSetStability(s, primary, .8, clock.Now())
	smartProfileSetStability(s, c, 1, clock.Now())
	clock.Advance(30 * time.Second)
	c.lastOK = clock.Now()
	s.chooseLocked(clock.Now())
	if c.improvementCriterion != "stability" || c.improvementObservations != 1 {
		t.Fatal("criterion change retained confirmations")
	}
}

func TestSmartProfileUDPAndQUICAreSeparate(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "realtime-gaming")
	primary, candidate := s.primary, s.standby
	primary.udp = smartEvidence{state: "failed", at: clock.Now(), expires: clock.Now().Add(time.Hour)}
	candidate.udp = smartEvidence{state: "ok", at: clock.Now(), expires: clock.Now().Add(time.Hour)}
	primary.udp443 = candidate.udp
	candidate.udp443 = primary.udp
	if better, criterion := s.betterLocked(candidate, primary, clock.Now()); !better || criterion != "udp" {
		t.Fatal("generic UDP conflated with QUIC")
	}
	s.preference.rank = []string{"quic"}
	if better, _ := s.betterLocked(candidate, primary, clock.Now()); better {
		t.Fatal("QUIC used generic UDP evidence")
	}
}

func TestSmartProfileUDPPoolBoundedAndRotating(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "realtime-gaming")
	s.preference.rank = []string{"udp", "quic"}
	s.preference.confirmations, s.preference.dwell = 100, 24*time.Hour
	s.selectedAt = clock.Now()
	for order := 2; order < 8; order++ {
		tag := fmt.Sprintf("member-%d", order)
		s.members = append(s.members, &smartMember{tag: tag, identity: tag, serverIdentity: tag, tier: 1, state: "healthy", configOrder: order, generation: 1})
	}
	seen := map[string]bool{}
	for range 8 {
		s.pending = nil
		s.queueProfileUDPLocked(clock.Now(), false)
		jobs := s.pending
		s.pending = nil
		counts := map[string]int{}
		for _, job := range jobs {
			counts[job.kind]++
			if job.kind == "udp443" {
				seen[job.member.tag] = true
			}
			smartCompleteProfileUDPRefresh(t, s, job, clock.Now())
		}
		if counts["udp"] > 3 || counts["udp443"] > 3 {
			t.Fatalf("unbounded pool: %v", counts)
		}
		clock.Advance(8 * time.Minute)
	}
	for _, member := range s.members {
		if member != s.primary && !seen[member.tag] {
			t.Fatalf("rotating pool missed %s", member.tag)
		}
	}
}

func TestSmartProfileQUICRefreshPreservesConfirmation(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "responsive")
	s.preference.rank = []string{"quic"}
	s.preference.confirmations = 3
	s.preference.dwell = time.Hour
	s.selectedAt = clock.Now()
	primary, candidate := s.primary, s.standby
	primary.udp443 = smartEvidence{state: "failed", at: clock.Now(), expires: clock.Now().Add(time.Hour)}
	candidate.udp443 = smartEvidence{state: "ok", at: clock.Now(), expires: clock.Now().Add(10 * time.Minute)}
	s.chooseLocked(clock.Now())
	for want := 2; want <= 3; want++ {
		clock.Advance(8 * time.Minute)
		if candidate.udp443.value(clock.Now()) != "ok" {
			t.Fatal("QUIC evidence lapsed before refresh")
		}
		s.queueProfileUDPLocked(clock.Now(), false)
		if len(s.pending) != 1 || s.pending[0].member != candidate {
			t.Fatal("scheduled QUIC refresh did not reach the fixed candidate")
		}
		job := s.pending[0]
		s.pending = nil
		smartCompleteProfileUDPRefresh(t, s, job, clock.Now())
		if candidate.improvementObservations != want {
			t.Fatalf("confirmations=%d want=%d", candidate.improvementObservations, want)
		}
	}
}

func TestSmartProfileTierRestoreDisabled(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "responsive")
	primary, candidate := s.primary, s.standby
	primary.tier = 2
	candidate.tier = 1
	s.preference.tierRestore = 0
	clock.Advance(time.Hour)
	s.chooseLocked(clock.Now())
	if s.primary != primary {
		t.Fatal("zero hold restored tier instead of disabling")
	}
	s.preference.tierRestore = time.Minute
	s.preference.improvement = false
	s.chooseLocked(clock.Now())
	if s.primary != candidate {
		t.Fatal("improvement false blocked tier restore")
	}
}

func TestSmartProfileStatus(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "balance-speed-1080p")
	s.primary.throughput = smartThroughput{Download: 10, At: clock.Now(), Source: "active"}
	text, err := SmartStatus(context.Background(), smartTestManager{values: []adapter.Outbound{s}}, "")
	if err != nil {
		t.Fatal(err)
	}
	var status struct {
		Preference struct {
			Profile string  `json:"profile"`
			Target  float64 `json:"target_mbps"`
		}
		Members []map[string]any
	}
	if err = json.Unmarshal([]byte(text), &status); err != nil {
		t.Fatal(err)
	}
	if status.Preference.Profile != "balance-speed-1080p" || status.Preference.Target != 8 || status.Members[0]["meets_floor"] != true || status.Members[0]["download_bucket"] == nil {
		t.Fatalf("status=%s", text)
	}
}

func TestSmartGoldenLegacyDecisions(t *testing.T) {
	// These decision traces pin the comparator and switching contract from the pre-profile engine.
	for _, preference := range []string{`{}`, `{"preference":{}}`, `{"preference":{"profile":"default"}}`} {
		t.Run(preference, func(t *testing.T) {
			var traces []string
			for _, scenario := range []string{"country", "unequal-quantiles", "latency-only", "degradation"} {
				s, clock := smartTestGroup()
				if err := json.Unmarshal([]byte(preference), &s.options); err != nil {
					t.Fatal(err)
				}
				if err := s.configure(); err != nil {
					t.Fatal(err)
				}
				primary, b := s.primary, s.standby
				s.selectedAt = clock.Now().Add(-time.Hour)
				switch scenario {
				case "country":
					primary.country = "AA"
					b.country = "BB"
					c := &smartMember{tag: "c", identity: "c", serverIdentity: "c", state: "healthy", tier: 1, configOrder: 2, country: "AA"}
					s.members = append(s.members, c)
				case "unequal-quantiles":
					primary.rtts = []float64{5, 10, 15, 100, 120}
					b.rtts = []float64{20, 25, 30, 35, 40}
				case "latency-only":
					primary.rtts = []float64{100, 100, 100}
					b.rtts = []float64{10, 10, 10}
				case "degradation":
					e := smartTestDegradationEpisode(s, clock.Now())
					smartTestDegradationResult(s, e, primary, smartDegradationConfirmation, 3, clock.Now())
					smartTestDegradationResult(s, e, b, smartDegradationChallenger, 30, clock.Now())
				}
				s.chooseLocked(clock.Now())
				clock.Advance(30 * time.Second)
				b.lastOK = clock.Now()
				s.chooseLocked(clock.Now())
				order := make([]string, len(s.members))
				for i, m := range s.members {
					order[i] = m.tag
				}
				traces = append(traces, fmt.Sprintf("%s:%v:%s", scenario, order, s.primary.tag))
			}
			want := []string{"country:[a c b]:a", "unequal-quantiles:[b a]:a", "latency-only:[b a]:a", "degradation:[b a]:b"}
			if !slices.Equal(traces, want) {
				t.Fatalf("legacy decisions=%q want=%q", traces, want)
			}
		})
	}
}

func TestSmartProfileFloorRespectsEarlierCriteria(t *testing.T) {
	for _, criterion := range []string{"udp", "stability"} {
		t.Run(criterion, func(t *testing.T) {
			s, clock := smartProfileTestGroup(t, "realtime-calls")
			primary, candidate := s.primary, s.standby
			s.preference.rank = []string{criterion, "speed_floor", "latency"}
			primary.throughput = smartThroughput{Download: 1, At: clock.Now(), Source: "active"}
			candidate.throughput = smartThroughput{Download: 3, At: clock.Now(), Source: "active"}
			primary.udp = smartEvidence{state: "ok", at: clock.Now(), expires: clock.Now().Add(time.Hour)}
			candidate.udp = smartEvidence{state: "failed", at: clock.Now(), expires: clock.Now().Add(time.Hour)}
			smartProfileSetStability(s, primary, 1, clock.Now())
			smartProfileSetStability(s, candidate, .9, clock.Now())
			s.rankLocked(clock.Now())
			if s.members[0] != primary {
				t.Fatalf("below-floor download overrode earlier %s", criterion)
			}
		})
	}
}

func TestSmartProfilePinnedUDPAndDemandPool(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "realtime-gaming")
	s.preference.rank = []string{"quic"}
	s.preference.confirmations, s.preference.dwell = 100, 24*time.Hour
	s.selectedAt = clock.Now()
	for order := 2; order < 9; order++ {
		tag := fmt.Sprintf("member-%d", order)
		s.members = append(s.members, &smartMember{tag: tag, identity: tag, serverIdentity: tag, tier: 1, state: "healthy", configOrder: order, generation: 1})
	}
	pinned, next, standby := s.members[7], s.members[2], s.standby
	s.members[8].tier = 2
	seen := map[string]bool{}
	for range 8 {
		s.pending = nil
		s.improvementCandidate = pinned
		pinned.improvementCriterion, pinned.improvementObservations = "quic", 1
		s.queueProfileUDPLocked(clock.Now(), false)
		jobs := s.pending
		s.pending = nil
		fixed := map[*smartMember]bool{}
		for _, job := range jobs {
			if job.kind != "udp443" {
				t.Fatal("QUIC-only pool probed generic UDP")
			}
			if job.member.tier != 1 {
				t.Fatal("exploration crossed the primary tier")
			}
			fixed[job.member] = true
			seen[job.member.tag] = true
			smartCompleteProfileUDPRefresh(t, s, job, clock.Now())
		}
		if len(jobs) > 4 || !fixed[standby] || !fixed[next] || !fixed[pinned] {
			t.Fatalf("fixed/pinned pool not bounded or refreshed: %v", fixed)
		}
		clock.Advance(8 * time.Minute)
		if pinned.udp443.value(clock.Now()) != "ok" {
			t.Fatal("pinned evidence expired between refreshes")
		}
	}
	for _, member := range s.members {
		if member != s.primary && member.tier == 1 && !seen[member.tag] {
			t.Fatalf("rotation missed %s", member.tag)
		}
	}
	clock.Advance(10 * time.Minute)
	s.pending = nil
	s.handleRequest(smartRequest{kind: "udp443"})
	if len(s.pending) > 4 || len(s.pending) == 0 {
		t.Fatalf("demand discovery escaped pool: jobs=%d", len(s.pending))
	}
	before := len(s.pending)
	s.handleRequest(smartRequest{kind: "udp443"})
	if len(s.pending) != before {
		t.Fatal("demand discovery ignored 10-minute gate")
	}
}

func TestSmartProfileRawRankControlsLiveSelection(t *testing.T) {
	s, clock := smartTestGroup()
	if err := json.Unmarshal([]byte(`{"preference":{"rank":["latency"],"switch":{"confirmations":1}}}`), &s.options); err != nil {
		t.Fatal(err)
	}
	if err := s.configure(); err != nil {
		t.Fatal(err)
	}
	candidate := s.standby
	s.primary.rtts, candidate.rtts = []float64{100, 100, 100}, []float64{25, 25, 25}
	s.selectedAt = clock.Now().Add(-time.Hour)
	s.chooseLocked(clock.Now())
	if s.preference.profile != "custom" || s.primary != candidate {
		t.Fatal("raw rank without a profile did not drive the live engine")
	}
}

func TestSmartProfileRTTEvidenceMinimums(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "responsive")
	member := s.primary
	for _, test := range []struct {
		criterion string
		count     int
		known     bool
	}{
		{"latency", 2, false}, {"latency", 3, true},
		{"rtt_spread", 7, false}, {"rtt_spread", 8, true},
	} {
		t.Run(fmt.Sprintf("%s/%d", test.criterion, test.count), func(t *testing.T) {
			member.rtts = make([]float64, test.count)
			for i := range member.rtts {
				member.rtts[i] = 40
			}
			s.rankProfileMemberLocked(member, clock.Now())
			s.rankProfileMemberLocked(s.standby, clock.Now())
			got := s.profileCriterionCompareLocked(member, s.standby, test.criterion, clock.Now()) < 0
			if got != test.known {
				t.Fatalf("known=%v want=%v", got, test.known)
			}
		})
	}
}

func TestSmartProfileMinimumStabilityPrecedesSpeed(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "balance-speed-4k")
	primary, candidate := s.primary, s.standby
	smartProfileSetStability(s, primary, .5, clock.Now())
	smartProfileSetStability(s, candidate, .9, clock.Now())
	primary.throughput = smartThroughput{Download: 100, At: clock.Now(), Source: "active"}
	candidate.throughput = smartThroughput{Download: 3, At: clock.Now(), Source: "active"}
	s.rankLocked(clock.Now())
	if s.members[0] != candidate {
		t.Fatal("speed overrode the minimum-stability gate")
	}
}
