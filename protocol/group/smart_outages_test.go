package group

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
)

func TestSmartOutageRetriesRemainOneEpisode(t *testing.T) {
	s, clock := smartTestGroup()
	member := s.primary
	s.observeOutagesLocked(member, clock.now)
	clock.Advance(3 * time.Hour)
	s.observeOutagesLocked(member, clock.now)
	member.state = "suspect"
	s.openOutageLocked(member, clock.now)
	member.state = "quarantined"
	s.observeOutagesLocked(member, clock.now)
	for range 3 {
		clock.Advance(24 * time.Hour)
		s.observeOutagesLocked(member, clock.now)
		member.state = "half-open"
		s.observeOutagesLocked(member, clock.now)
		member.state = "quarantined"
		s.observeOutagesLocked(member, clock.now)
		s.openOutageLocked(member, clock.now)
	}
	episodes, _, _ := s.outageEstimateLocked(member, clock.now)
	if math.Abs(episodes-0.5) > 1e-12 || !s.history.Stats["wifi"][member.identity].OutageOpen {
		t.Fatalf("three failed probation attempts changed one decayed episode: episodes=%v", episodes)
	}
	member.state = "healthy"
	s.closeOutageLocked(member, clock.now)
	if s.history.Stats["wifi"][member.identity].OutageOpen {
		t.Fatal("confirmed healthy recovery did not close the episode")
	}
	member.state = "suspect"
	s.openOutageLocked(member, clock.now)
	episodes, _, _ = s.outageEstimateLocked(member, clock.now)
	if math.Abs(episodes-1.5) > 1e-12 {
		t.Fatalf("new loss after recovery = %v episodes; want 1.5", episodes)
	}
}

func TestSmartOutageExposureExcludesPauseAndOtherNetworks(t *testing.T) {
	s, clock := smartTestGroup()
	member := s.primary
	wifi := s.network
	s.observeOutagesLocked(member, clock.now)
	clock.Advance(time.Hour)
	s.observeOutagesLocked(member, clock.now)
	s.paused, s.pauseStarted = true, clock.now
	s.observeOutagesLocked(member, clock.now)
	clock.Advance(4 * time.Hour)
	s.observeOutagesLocked(member, clock.now)
	s.network = smartNetwork{Key: "cell", Path: []string{"cell"}, Link: true}
	s.generation++
	s.observeOutagesLocked(member, clock.now)
	clock.Advance(4 * time.Hour)
	s.observeOutagesLocked(member, clock.now)
	s.network = wifi
	s.generation++
	s.pausedTotal += 8 * time.Hour
	s.paused = false
	s.observeOutagesLocked(member, clock.now)
	clock.Advance(time.Hour)
	s.observeOutagesLocked(member, clock.now)
	_, exposure, _ := s.outageEstimateLocked(member, clock.now)
	if want := 1 + math.Exp2(-9.0/72); math.Abs(exposure-want) > 1e-12 {
		t.Fatalf("paused exposure = %v; want %v", exposure, want)
	}
	if s.history.Stats["cell"][member.identity] != nil {
		t.Fatal("paused network accumulated outage evidence")
	}

	s.network = smartNetwork{Key: "cell", Path: []string{"cell"}, Link: true}
	s.generation++
	s.observeOutagesLocked(member, clock.now)
	clock.Advance(time.Hour)
	s.observeOutagesLocked(member, clock.now)
	_, cellExposure, _ := s.outageEstimateLocked(member, clock.now)
	if cellExposure != 1 {
		t.Fatalf("new network inherited previous observation interval: %v hours", cellExposure)
	}
	s.network = wifi
	s.generation++
	s.observeOutagesLocked(member, clock.now)
	_, exposure, _ = s.outageEstimateLocked(member, clock.now)
	if want := (1 + math.Exp2(-9.0/72)) * math.Exp2(-1.0/72); math.Abs(exposure-want) > 1e-12 {
		t.Fatalf("away-network time accrued on Wi-Fi: %v; want %v", exposure, want)
	}
}

func TestSmartOutageMembersAndHierarchyObserveOnce(t *testing.T) {
	s, clock := smartTestGroup()
	s.network = smartNetwork{Key: "cell/00101/LTE", Path: []string{"cell/00101/LTE", "cell/00101", "cell", "", "cell"}, Link: true}
	for _, member := range s.members {
		s.observeOutagesLocked(member, clock.now)
	}
	clock.Advance(3 * time.Hour)
	for _, member := range s.members {
		s.observeOutagesLocked(member, clock.now)
	}
	s.openOutageLocked(s.primary, clock.now)
	s.openOutageLocked(s.primary, clock.now)
	for _, key := range []string{"cell/00101/LTE", "cell/00101", "cell"} {
		primary := s.history.Stats[key][s.primary.identity]
		standby := s.history.Stats[key][s.standby.identity]
		if primary.OutageEpisodes != 1 || primary.OutageExposureHours != 3 || standby.OutageEpisodes != 0 || standby.OutageExposureHours != 3 {
			t.Fatalf("%s mixed members or repeated a path key: primary=%+v standby=%+v", key, primary, standby)
		}
	}
	if s.history.Stats[""] != nil {
		t.Fatal("empty path key accumulated outage evidence")
	}
}

func TestSmartOutageClassThresholdsAndWeakExposure(t *testing.T) {
	for _, test := range []struct {
		name     string
		episodes float64
		hours    float64
		class    int
	}{
		{"empty", 0, 0, -1},
		{"weak_success", 0, 1.99, -1},
		{"weak_failure", 4, 1.99, -1},
		{"excellent", 0, 2, 0},
		{"below_005", 0.099, 2, 0},
		{"at_005", 0.1, 2, 1},
		{"below_025", 0.499, 2, 1},
		{"at_025", 0.5, 2, 2},
		{"below_1", 1.999, 2, 2},
		{"at_1", 2, 2, 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, clock := smartTestGroup()
			s.history.statistic("wifi", s.primary.identity, clock.now).OutageEpisodes = test.episodes
			s.history.Stats["wifi"][s.primary.identity].OutageExposureHours = test.hours
			episodes, exposure, class := s.outageEstimateLocked(s.primary, clock.now)
			if episodes != test.episodes || exposure != test.hours || class != test.class {
				t.Fatalf("estimate = %v, %v, %d; want %v, %v, %d", episodes, exposure, class, test.episodes, test.hours, test.class)
			}
		})
	}
}

func TestSmartOutageHierarchyFallsBackWithoutDoubleCounting(t *testing.T) {
	s, clock := smartTestGroup()
	s.network = smartNetwork{Key: "cell/00101/LTE", Path: []string{"cell/00101/LTE", "cell/00101", "cell"}, Link: true}
	leaf := s.history.statistic(s.network.Key, s.primary.identity, clock.now)
	leaf.OutageEpisodes, leaf.OutageExposureHours = 3, 1
	parent := s.history.statistic("cell/00101", s.primary.identity, clock.now)
	parent.OutageEpisodes, parent.OutageExposureHours = 1, 10
	root := s.history.statistic("cell", s.primary.identity, clock.now)
	root.OutageEpisodes, root.OutageExposureHours = 0, 100
	episodes, exposure, class := s.outageEstimateLocked(s.primary, clock.now)
	if episodes != 1 || exposure != 10 || class != 1 {
		t.Fatalf("ancestor fallback = %v, %v, %d; want 1, 10, 1", episodes, exposure, class)
	}
	leaf.OutageExposureHours = 2
	episodes, exposure, class = s.outageEstimateLocked(s.primary, clock.now)
	if episodes != 3 || exposure != 2 || class != 3 {
		t.Fatalf("specific evidence did not win: %v, %v, %d", episodes, exposure, class)
	}
}

func TestSmartOutageHierarchyIncludesMatchingPriorsBeforeFallback(t *testing.T) {
	s, clock := smartTestGroup()
	s.network = smartNetwork{Key: "cell/00101/LTE", Path: []string{"cell/00101/LTE", "cell/00101", "cell"}, Link: true}
	leaf := s.history.statistic(s.network.Key, s.primary.identity, clock.now)
	leaf.OutageEpisodes, leaf.OutageExposureHours = 1, 1
	s.priors = []smartPrior{
		{Network: s.network.Key, ServerIdentity: s.primary.serverIdentity, OutageExposureHours: 1, At: clock.now.Unix()},
		{Network: "cell", ServerIdentity: s.primary.serverIdentity, OutageExposureHours: 100, At: clock.now.Unix()},
		{Network: s.network.Key, ServerIdentity: s.standby.serverIdentity, OutageExposureHours: 100, At: clock.now.Unix()},
	}
	episodes, exposure, class := s.outageEstimateLocked(s.primary, clock.now)
	if episodes != 1 || exposure != 2 || class != 2 {
		t.Fatalf("matching prior was not combined before selecting level: %v, %v, %d", episodes, exposure, class)
	}
	leaf.OutageExposureHours = 0
	s.priors = s.priors[1:]
	episodes, exposure, class = s.outageEstimateLocked(s.primary, clock.now)
	if episodes != 0 || exposure != 100 || class != 0 {
		t.Fatalf("generic imported evidence did not apply to new path: %v, %v, %d", episodes, exposure, class)
	}
}

func TestSmartOutageDecayAndImportTrustPreserveRate(t *testing.T) {
	s, clock := smartTestGroup()
	statistic := s.history.statistic("wifi", s.primary.identity, clock.now)
	statistic.OutageEpisodes, statistic.OutageExposureHours = 4, 40
	statistic.OutageObservedHours = 80
	clock.Advance(72 * time.Hour)
	episodes, exposure, observed, class := s.outageEvidenceLocked(s.primary, clock.now)
	if episodes != 2 || exposure != 20 || observed != 40 || class != 1 {
		t.Fatalf("decayed rate or diagnostics changed: %v, %v, %v, %d", episodes, exposure, observed, class)
	}
	statistic = s.history.statistic("wifi", s.primary.identity, clock.now)
	if statistic.OutageEpisodes != 2 || statistic.OutageExposureHours != 20 || statistic.OutageObservedHours != 40 {
		t.Fatalf("stored decay = %v, %v, %v; want 2, 20, 40", statistic.OutageEpisodes, statistic.OutageExposureHours, statistic.OutageObservedHours)
	}
	prior, ok := smartImportedPrior(smartExportStatistic{Network: "wifi", ServerIdentity: s.primary.serverIdentity, S: 100, W: 100, OutageEpisodes: 4, OutageExposureHours: 40, OutageObservedHours: 80}, "source", clock.now.Add(-72*time.Hour).Unix(), 0.25, clock.now)
	if !ok || prior.W != 3 || prior.OutageEpisodes != 0.5 || prior.OutageExposureHours != 5 || prior.OutageObservedHours != 10 {
		t.Fatalf("import trust or stability cap distorted outage rate: %+v, accepted=%v", prior, ok)
	}
	s.history.Reset()
	s.priors = []smartPrior{prior}
	episodes, exposure, observed, class = s.outageEvidenceLocked(s.primary, clock.now)
	if episodes != 0.5 || exposure != 5 || observed != 10 || class != 1 {
		t.Fatalf("imported estimate = %v, %v, %v, %d; want 0.5, 5, 10, 1", episodes, exposure, observed, class)
	}
}

func TestSmartOutageExportImportRoundTripWithoutProbeWeight(t *testing.T) {
	s, clock := smartTestGroup()
	record := smartExportStatistic{Network: "wifi", ServerIdentity: s.primary.serverIdentity, OutageEpisodes: 2, OutageExposureHours: 20, OutageObservedHours: 40}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	var imported smartExportStatistic
	if err = json.Unmarshal(data, &imported); err != nil {
		t.Fatal(err)
	}
	prior, ok := smartImportedPrior(imported, "source", clock.now.Unix(), 0.5, clock.now)
	if !ok || prior.W != 0 || prior.S != 0 || prior.OutageEpisodes != 1 || prior.OutageExposureHours != 10 || prior.OutageObservedHours != 20 {
		t.Fatalf("outage-only export/import lost evidence: %+v, accepted=%v", prior, ok)
	}
	s.priors = []smartPrior{prior}
	episodes, exposure, class := s.outageEstimateLocked(s.primary, clock.now)
	if episodes != 1 || exposure != 10 || class != 1 {
		t.Fatalf("outage-only prior estimate = %v, %v, %d", episodes, exposure, class)
	}
	prior.OutageExposureHours = 1
	s.priors = []smartPrior{prior}
	if _, _, class = s.outageEstimateLocked(s.primary, clock.now); class != -1 {
		t.Fatalf("weak imported exposure scored as known: %d", class)
	}
}

func TestSmartOutageEstimateDoesNotCreateHistory(t *testing.T) {
	s, clock := smartTestGroup()
	for range 100 {
		episodes, exposure, class := s.outageEstimateLocked(s.primary, clock.now)
		if episodes != 0 || exposure != 0 || class != -1 {
			t.Fatalf("unknown estimate = %v, %v, %d", episodes, exposure, class)
		}
	}
	if len(s.history.Stats) != 0 || len(s.history.Networks) != 0 {
		t.Fatal("outage ranking created evidence")
	}
}

func TestSmartOutageAPIExportImportMergesServerEvidence(t *testing.T) {
	s, clock := smartTestGroup()
	ledger, err := getSmartLedger(nil)
	if err != nil {
		t.Fatal(err)
	}
	s.ledger = ledger
	s.installID, s.seed, s.directSeed = ledger.Installation()
	for index, member := range s.members {
		statistic := s.history.statistic("wifi", member.identity, clock.now)
		statistic.OutageEpisodes = float64(index+1) * 2
		statistic.OutageExposureHours = float64(index+1) * 20
		statistic.OutageObservedHours = float64(index+1) * 40
		s.history.IdentityServers[member.identity] = s.primary.serverIdentity
	}
	manager := smartTestManager{values: []adapter.Outbound{s}}
	exported, err := SmartExport(context.Background(), manager, false)
	if err != nil {
		t.Fatal(err)
	}
	var document smartExportDocument
	if err = json.Unmarshal([]byte(exported), &document); err != nil {
		t.Fatal(err)
	}
	records := document.Groups[s.Tag()].Stats
	if len(records) != 1 || records[0].OutageEpisodes != 6 || records[0].OutageExposureHours != 60 || records[0].OutageObservedHours != 120 {
		t.Fatalf("export did not merge local identities for one server: %+v", records)
	}
	s.history.Reset()
	if _, err = SmartImport(context.Background(), manager, exported, 0.25); err != nil {
		t.Fatal(err)
	}
	episodes, exposure, observed, class := s.outageEvidenceLocked(s.primary, clock.now)
	if episodes != 1.5 || exposure != 15 || observed != 30 || class != 1 {
		t.Fatalf("API outage-only round trip = %v, %v, %v, %d; want 1.5, 15, 30, 1", episodes, exposure, observed, class)
	}
}

func TestSmartOutageRecoverySurvivesRestartWithoutDelayingUsability(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "stability-downtime")
	member := s.primary
	s.openOutageLocked(member, clock.now)
	data, err := s.history.Encode()
	if err != nil {
		t.Fatal(err)
	}
	s.history, err = decodeSmartHistory(data)
	if err != nil {
		t.Fatal(err)
	}
	member.state = "unknown"
	applySuccess := func() {
		s.applyResultLocked(smartResult{
			member: member, generation: s.generation, memberGeneration: member.generation,
			configGeneration: s.configGeneration, kind: "tcp", rtt: time.Millisecond,
		}, clock.now)
	}
	applySuccess()
	statistic := s.history.Stats["wifi"][member.identity]
	if member.state != "healthy" || s.primary != member || !statistic.OutageOpen || statistic.OutageRecoverySuccesses != 1 {
		t.Fatalf("first startup success delayed usability or closed outage: state=%s primary=%v statistic=%+v", member.state, s.primary == member, statistic)
	}

	data, err = s.history.Encode()
	if err != nil {
		t.Fatal(err)
	}
	s.history, err = decodeSmartHistory(data)
	if err != nil {
		t.Fatal(err)
	}
	s.newGenerationLocked(s.network, clock.now)
	clock.Advance(20 * time.Second)
	applySuccess()
	statistic = s.history.Stats["wifi"][member.identity]
	if !statistic.OutageOpen || statistic.OutageRecoverySuccesses != 2 || member.state != "healthy" {
		t.Fatalf("restart or generation reset recovery evidence: state=%s statistic=%+v", member.state, statistic)
	}
	clock.Advance(20*time.Second - time.Millisecond)
	applySuccess()
	if !statistic.OutageOpen || statistic.OutageRecoverySuccesses != 2 {
		t.Fatalf("success less than 20 seconds apart confirmed recovery: %+v", statistic)
	}
	clock.Advance(time.Millisecond)
	applySuccess()
	if statistic.OutageOpen || statistic.OutageRecoverySuccesses != 0 || statistic.OutageRecoveryAtNS != 0 {
		t.Fatalf("three spaced successes did not close the saved episode: %+v", statistic)
	}
}

func TestSmartOutageRecoveryFirstOperatorFailureResetsSameEpisode(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "stability-downtime")
	member := s.primary
	s.openOutageLocked(member, clock.now)
	applyTCP := func(err error) {
		s.applyResultLocked(smartResult{
			member: member, generation: s.generation, memberGeneration: member.generation,
			configGeneration: s.configGeneration, kind: "tcp", rtt: time.Millisecond, err: err,
		}, clock.now)
	}
	applyTCP(nil)
	clock.Advance(20 * time.Second)
	applyTCP(nil)
	applyTCP(io.ErrUnexpectedEOF)
	statistic := s.history.Stats["wifi"][member.identity]
	if !statistic.OutageOpen || statistic.OutageRecoverySuccesses != 0 || statistic.OutageRecoveryAtNS != 0 || member.state != "suspect" {
		t.Fatalf("first operator failure did not reset outage recovery: state=%s statistic=%+v", member.state, statistic)
	}
	s.openOutageLocked(member, clock.now)
	if want := math.Exp2(-20.0 / (72 * 3600)); math.Abs(statistic.OutageEpisodes-want) > 1e-12 {
		t.Fatalf("failed recovery reopened an existing episode: episodes=%v want=%v", statistic.OutageEpisodes, want)
	}
	for index := range 3 {
		clock.Advance(20 * time.Second)
		applyTCP(nil)
		if wantOpen := index < 2; statistic.OutageOpen != wantOpen {
			t.Fatalf("post-failure success %d: open=%v; want %v", index+1, statistic.OutageOpen, wantOpen)
		}
	}
}

func TestSmartOutageRecoveryRemainsIndependentPerPath(t *testing.T) {
	s, clock := smartTestGroup()
	s.network = smartNetwork{Key: "cell/00101", Path: []string{"cell/00101", "cell", "cell", ""}, Link: true}
	s.openOutageLocked(s.primary, clock.now)
	s.observeOutageRecoveryLocked(s.primary, true, clock.now)
	clock.Advance(20 * time.Second)
	s.observeOutageRecoveryLocked(s.primary, true, clock.now)
	leaf := s.history.Stats["cell/00101"][s.primary.identity]
	root := s.history.Stats["cell"][s.primary.identity]
	if leaf.OutageRecoverySuccesses != 2 || root.OutageRecoverySuccesses != 2 {
		t.Fatalf("duplicate path keys repeated recovery observations: leaf=%+v root=%+v", leaf, root)
	}
	s.network = smartNetwork{Key: "cell/00102", Path: []string{"cell/00102", "cell"}, Link: true}
	s.generation++
	s.openOutageLocked(s.primary, clock.now)
	clock.Advance(20 * time.Second)
	s.observeOutageRecoveryLocked(s.primary, true, clock.now)
	other := s.history.Stats["cell/00102"][s.primary.identity]
	if !leaf.OutageOpen || leaf.OutageRecoverySuccesses != 2 || root.OutageOpen || !other.OutageOpen || other.OutageRecoverySuccesses != 1 {
		t.Fatalf("one path closed a distinct path's episode: leaf=%+v root=%+v other=%+v", leaf, root, other)
	}
}

func TestSmartOutageRestartCensorsOnlyAfterThirtyUnobservedMinutes(t *testing.T) {
	for _, gap := range []time.Duration{10 * time.Minute, 30 * time.Minute, 30*time.Minute + time.Nanosecond, time.Hour} {
		t.Run(gap.String(), func(t *testing.T) {
			s, clock := smartTestGroup()
			member := s.primary
			s.openOutageLocked(member, clock.now)
			s.observeOutageRecoveryLocked(member, true, clock.now)
			data, err := s.history.Encode()
			if err != nil {
				t.Fatal(err)
			}
			s.history, err = decodeSmartHistory(data)
			if err != nil {
				t.Fatal(err)
			}
			clock.Advance(gap)
			s.observeOutagesLocked(member, clock.now)
			statistic := s.history.Stats["wifi"][member.identity]
			wantOpen := gap <= 30*time.Minute
			wantSuccesses := 0
			if wantOpen {
				wantSuccesses = 1
			}
			if statistic.OutageOpen != wantOpen || statistic.OutageRecoverySuccesses != wantSuccesses || statistic.OutageExposureHours != 0 {
				t.Fatalf("restart gap %s: open=%v successes=%d exposure=%v", gap, statistic.OutageOpen, statistic.OutageRecoverySuccesses, statistic.OutageExposureHours)
			}
			wantEpisodes := math.Exp2(-gap.Seconds() / (72 * 3600))
			if math.Abs(statistic.OutageEpisodes-wantEpisodes) > 1e-12 {
				t.Fatalf("censoring changed episode evidence: %v; want %v", statistic.OutageEpisodes, wantEpisodes)
			}
			if wantOpen {
				s.observeOutageRecoveryLocked(member, true, clock.now)
				clock.Advance(20 * time.Second)
				s.observeOutageRecoveryLocked(member, true, clock.now)
				if statistic.OutageOpen {
					t.Fatal("short restart lost persisted recovery progress")
				}
			} else {
				s.openOutageLocked(member, clock.now)
				if math.Abs(statistic.OutageEpisodes-(wantEpisodes+1)) > 1e-12 {
					t.Fatalf("new loss after censoring did not add an episode: %v", statistic.OutageEpisodes)
				}
			}
		})
	}
}

func TestSmartOutageInactiveTimeAddsNoExposure(t *testing.T) {
	for _, kind := range []string{"paused", "offline", "stopped", "loop_stopped", "link_down"} {
		t.Run(kind, func(t *testing.T) {
			s, clock := smartTestGroup()
			member := s.primary
			s.observeOutagesLocked(member, clock.now)
			clock.Advance(time.Hour)
			s.openOutageLocked(member, clock.now)
			s.observeOutageRecoveryLocked(member, true, clock.now)
			switch kind {
			case "paused":
				s.paused, s.pauseStarted = true, clock.now
			case "offline":
				s.offline = true
			case "stopped":
				s.closed = true
			case "loop_stopped":
				s.loopStopped = true
			case "link_down":
				s.network.Link = false
			}
			s.observeOutagesLocked(member, clock.now)
			clock.Advance(time.Hour)
			s.observeOutagesLocked(member, clock.now)
			statistic := s.history.Stats["wifi"][member.identity]
			if statistic.OutageExposureHours != 1 || !statistic.OutageOpen {
				t.Fatalf("inactive observation accrued exposure or censored early: %+v", statistic)
			}
			if s.paused {
				s.pausedTotal += clock.now.Sub(s.pauseStarted)
			}
			s.paused, s.offline, s.closed, s.loopStopped, s.network.Link = false, false, false, false, true
			s.observeOutagesLocked(member, clock.now)
			if want := math.Exp2(-1.0 / 72); math.Abs(statistic.OutageExposureHours-want) > 1e-12 || statistic.OutageOpen || statistic.OutageRecoverySuccesses != 0 {
				t.Fatalf("inactive hour accrued exposure or retained uncensored episode: %+v; want exposure=%v", statistic, want)
			}
		})
	}
}

func TestSmartOutageCensorsNetworkAndProviderAbsence(t *testing.T) {
	for _, kind := range []string{"network", "provider"} {
		t.Run(kind, func(t *testing.T) {
			s, clock := smartTestGroup()
			member := s.primary
			wifi := s.network
			s.openOutageLocked(member, clock.now)
			s.observeOutageRecoveryLocked(member, true, clock.now)
			s.observeOutagesLocked(member, clock.now)
			if kind == "network" {
				s.network = smartNetwork{Key: "cell", Path: []string{"cell"}, Link: true}
				s.generation++
				s.observeOutagesLocked(member, clock.now)
			} else {
				s.configGeneration++
			}
			clock.Advance(31 * time.Minute)
			if kind == "network" {
				s.observeOutagesLocked(member, clock.now)
				s.network = wifi
				s.generation++
			} else {
				s.configGeneration++
			}
			s.observeOutagesLocked(member, clock.now)
			statistic := s.history.Stats["wifi"][member.identity]
			if statistic.OutageOpen || statistic.OutageRecoverySuccesses != 0 || statistic.OutageExposureHours != 0 {
				t.Fatalf("%s absence was credited as exposure or retained open episode: %+v", kind, statistic)
			}
			episodes := statistic.OutageEpisodes
			s.openOutageLocked(member, clock.now)
			if statistic.OutageEpisodes != episodes+1 {
				t.Fatalf("loss after %s censoring did not start a new episode: %+v", kind, statistic)
			}
		})
	}
}

func TestSmartOutageDowntimeCannotImproveItsOwnRate(t *testing.T) {
	s, clock := smartTestGroup()
	member := s.primary
	s.observeOutagesLocked(member, clock.now)
	clock.Advance(4 * time.Hour)
	s.openOutageLocked(member, clock.now)
	member.state = "quarantined"
	s.observeOutagesLocked(member, clock.now)
	for index, state := range []string{"quarantined", "half-open", "probation", "unknown"} {
		member.state = state
		s.observeOutagesLocked(member, clock.now)
		clock.Advance(time.Hour)
		s.observeOutagesLocked(member, clock.now)
		episodes, exposure, observed, class := s.outageEvidenceLocked(member, clock.now)
		wantExposure := 4 * math.Exp2(-float64(index+1)/72)
		if math.Abs(exposure-wantExposure) > 1e-12 || math.Abs(episodes/exposure-0.25) > 1e-12 || class != 2 || observed <= exposure {
			t.Fatalf("%s downtime improved outage rate: episodes=%v exposure=%v observed=%v class=%d; want exposure=%v and rate=0.25", state, episodes, exposure, observed, class, wantExposure)
		}
	}
	member.state = "healthy"
	s.observeOutagesLocked(member, clock.now)
	clock.Advance(time.Hour)
	s.observeOutagesLocked(member, clock.now)
	_, exposure, _, _ := s.outageEvidenceLocked(member, clock.now)
	if want := 4*math.Exp2(-5.0/72) + 1; math.Abs(exposure-want) > 1e-12 {
		t.Fatalf("confirmed usable time did not resume at-risk exposure: %v; want %v", exposure, want)
	}
	member.state = "suspect"
	s.observeOutagesLocked(member, clock.now)
	clock.Advance(time.Hour)
	s.observeOutagesLocked(member, clock.now)
	_, exposure, _, _ = s.outageEvidenceLocked(member, clock.now)
	if want := (4*math.Exp2(-5.0/72)+1)*math.Exp2(-1.0/72) + 1; math.Abs(exposure-want) > 1e-12 {
		t.Fatalf("suspect usable time did not accrue at-risk exposure: %v; want %v", exposure, want)
	}
}

func TestSmartOutageDiagnosticOnlyImportDoesNotCreateAtRiskEvidence(t *testing.T) {
	s, clock := smartTestGroup()
	prior, ok := smartImportedPrior(smartExportStatistic{
		Network: "wifi", ServerIdentity: s.primary.serverIdentity, OutageObservedHours: 100,
	}, "source", clock.now.Unix(), 0.25, clock.now)
	if !ok || prior.OutageObservedHours != 25 {
		t.Fatalf("diagnostic-only evidence was lost: %+v, accepted=%v", prior, ok)
	}
	s.priors = []smartPrior{prior}
	episodes, exposure, observed, class := s.outageEvidenceLocked(s.primary, clock.now)
	if episodes != 0 || exposure != 0 || observed != 25 || class != -1 {
		t.Fatalf("diagnostic time became ranking exposure: %v, %v, %v, %d", episodes, exposure, observed, class)
	}
}
