package group

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/option"
)

func TestSmartHistoryHierarchyGolden(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	history := newSmartHistory()
	path := []string{"wifi/bssid:0123456789abcdef", "wifi/ssid:fedcba9876543210", "wifi"}
	history.Stats["wifi"] = map[string]*smartStatistic{"member": {S: 18, W: 20, UpdatedAt: now.Unix()}}
	history.Stats[path[1]] = map[string]*smartStatistic{"member": {S: 8, W: 10, UpdatedAt: now.Unix()}}
	history.Stats[path[0]] = map[string]*smartStatistic{"member": {S: 1, W: 2, UpdatedAt: now.Unix()}}
	probability, weight := history.Estimate(path, "member", "server", now, nil)
	if math.Abs(probability-0.753125) > 1e-12 || weight != 2 {
		t.Fatalf("estimate = %.12f, %.12f; want 0.753125, 2", probability, weight)
	}
	if class := history.Class(path, "member", "server", now, nil); class != 3 {
		t.Fatalf("class = %d; want D", class)
	}
}

func TestSmartHistoryObservationTimeGolden(t *testing.T) {
	start := time.Unix(1_800_000_000, 0)
	history := newSmartHistory()
	path := []string{"wifi"}
	history.Observe(path, "member", true, 100, start, 0)
	history.Observe(path, "member", false, 0, start.Add(10*time.Minute), 8*time.Minute)
	statistic := history.Stats["wifi"]["member"]
	if math.Abs(statistic.W-1.3983967791713545) > 1e-12 || math.Abs(statistic.S-0.9983967791713544) > 1e-12 {
		t.Fatalf("pause sums = %.15f, %.15f", statistic.S, statistic.W)
	}
	history.Observe(path, "member", true, 120, start.Add(24*time.Hour), 8*time.Minute)
	if math.Abs(statistic.W-2.1116905445988845) > 1e-12 || math.Abs(statistic.S-1.7937005259840997) > 1e-12 {
		t.Fatalf("idle gap did not cap observation weight: %v", statistic.W)
	}
	if len(statistic.Flaps) != 2 || len(statistic.RTTs) != 2 {
		t.Fatalf("flaps/RTTs = %v/%v", statistic.Flaps, statistic.RTTs)
	}
}

func TestSmartHistoryPauseExclusionSurvivesNetworkChanges(t *testing.T) {
	s, clock := smartTestGroup()
	member := s.primary
	start := clock.now
	wifi := smartNetwork{
		Key: "wifi/bssid:0123456789abcdef", Type: "wifi",
		Path: []string{"wifi/bssid:0123456789abcdef", "wifi/ssid:fedcba9876543210", "wifi"}, Link: true,
	}
	s.network = wifi
	s.pausedTotal = 2 * time.Minute
	s.recordObservationLocked(member, true, 100*time.Millisecond, clock.now)

	s.pausedTotal += 8 * time.Minute
	clock.Advance(8 * time.Minute)
	s.network = smartNetwork{Key: "cell/25001/LTE", Type: "cellular", Path: []string{"cell/25001/LTE", "cell/25001", "cell"}, Link: true}
	s.recordObservationLocked(member, true, 100*time.Millisecond, clock.now)
	for _, key := range s.network.Path {
		if weight := s.history.Stats[key][member.identity].W; weight != 1 {
			t.Fatalf("first observation on %s has weight %v; want 1", key, weight)
		}
	}

	clock.Advance(2 * time.Minute)
	s.network = wifi
	s.recordObservationLocked(member, false, 0, clock.now)
	for _, key := range wifi.Path {
		statistic := s.history.Stats[key][member.identity]
		if math.Abs(statistic.W-1.3983967791713545) > 1e-12 || math.Abs(statistic.S-0.9983967791713544) > 1e-12 {
			t.Fatalf("%s pause sums = %.15f, %.15f; want 1.3983967791713545, 0.9983967791713544", key, statistic.W, statistic.S)
		}
	}
	if clock.now.Sub(start) != 10*time.Minute {
		t.Fatal("test did not include the eight-minute pause and two observed minutes")
	}
}

func TestSmartHistoryPauseBaselineBelongsToEachHierarchyLevel(t *testing.T) {
	start := time.Unix(1_800_000_000, 0)
	history := newSmartHistory()
	first := []string{"wifi/bssid:0123456789abcdef", "wifi/ssid:fedcba9876543210", "wifi"}
	second := []string{"wifi/bssid:123456789abcdef0", first[1], "wifi"}
	history.Observe(first, "member", true, 100, start, 0)
	history.Observe(second, "member", true, 100, start.Add(9*time.Minute), 8*time.Minute)
	history.Observe(first, "member", false, 0, start.Add(10*time.Minute), 8*time.Minute)
	child := history.Stats[first[0]]["member"]
	if math.Abs(child.W-1.3983967791713545) > 1e-12 {
		t.Fatalf("returning BSSID weight = %.15f; want 1.3983967791713545", child.W)
	}
	for _, key := range first[1:] {
		statistic := history.Stats[key]["member"]
		expectedSuccess := math.Exp2(-10.0/4320) + 0.2*math.Exp2(-1.0/4320)
		if math.Abs(statistic.S-expectedSuccess) > 1e-12 || math.Abs(statistic.W-expectedSuccess-0.2) > 1e-12 {
			t.Fatalf("%s parent sums = %.15f, %.15f; want %.15f, %.15f", key, statistic.S, statistic.W, expectedSuccess, expectedSuccess+0.2)
		}
	}
}

func TestSmartHistoryDecayAndImportConfidence(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	history := newSmartHistory()
	history.Stats["wifi"] = map[string]*smartStatistic{"member": {S: 12, W: 12, UpdatedAt: now.Add(-72 * time.Hour).Unix()}}
	probability, weight := history.Estimate([]string{"wifi"}, "member", "server", now, nil)
	if math.Abs(probability-0.95) > 1e-12 || weight != 6 {
		t.Fatalf("decayed estimate = %v, %v; want 0.95, 6", probability, weight)
	}
	prior, ok := smartImportedPrior(smartExportStatistic{Network: "cell", ServerIdentity: "server", S: 40, W: 40}, "source", now.Add(-72*time.Hour).Unix(), 0.5, now)
	if !ok || prior.S != 6 || prior.W != 6 {
		t.Fatalf("import = %+v, %v; want S=W=6", prior, ok)
	}
	probability, weight = history.Estimate([]string{"cell"}, "new-member", "server", now, []smartPrior{prior})
	if math.Abs(probability-0.95) > 1e-12 || weight != 0 {
		t.Fatalf("import estimate = %v, %v; want 0.95, 0", probability, weight)
	}
	if class := history.Class([]string{"cell"}, "new-member", "server", now, []smartPrior{prior}); class != 2 {
		t.Fatalf("import-only class = %d; want C", class)
	}
}

func TestSmartHistoryImportsReplaceAndKeepSixteenSources(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	var priors []smartPrior
	var sources []smartPriorSource
	for index := range 17 {
		source := fmt.Sprintf("source-%02d", index)
		var replaced bool
		priors, sources, replaced = smartReplacePriors(priors, sources, source, []smartPrior{{Network: "wifi", ServerIdentity: "server", S: 1, W: 1, At: now.Unix()}}, now.Add(time.Duration(index)*time.Second))
		if replaced {
			t.Fatal("new source was reported as replaced")
		}
	}
	if len(sources) != 16 || len(priors) != 16 || sources[0].ID != "source-01" {
		t.Fatalf("lineage = %+v; priors = %d", sources, len(priors))
	}
	priors, sources, replaced := smartReplacePriors(priors, sources, "source-01", []smartPrior{{Network: "wifi", ServerIdentity: "server", W: 2, At: now.Unix()}}, now.Add(time.Minute))
	if !replaced || len(sources) != 16 || len(priors) != 16 {
		t.Fatalf("re-import grew lineage: replaced=%v sources=%d priors=%d", replaced, len(sources), len(priors))
	}
	var weight float64
	for _, prior := range priors {
		if prior.Source == "source-01" {
			weight += prior.W
		}
	}
	if weight != 2 {
		t.Fatalf("replacement weight = %v; want 2", weight)
	}
	for _, key := range []string{"wifi", "cell", "cell/25001", "cell/25001/LTE", "eth/eth0", "other"} {
		if !smartImportNetworkAllowed(key, false) {
			t.Errorf("foreign coarse key rejected: %s", key)
		}
	}
	for _, key := range []string{"wifi/bssid:0123456789abcdef", "wifi/ssid:fedcba9876543210", "cell/25001/LTE/tower:0123456789abcdef"} {
		if smartImportNetworkAllowed(key, false) || !smartImportNetworkAllowed(key, true) {
			t.Errorf("hashed key mapping violated: %s", key)
		}
	}
}

func TestSmartHistoryBoundsRoundTripAndTimeline(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	history := newSmartHistory()
	for index := range 49 {
		history.Observe([]string{fmt.Sprintf("eth/eth%d", index)}, "member", true, 100, now.Add(time.Duration(index)*time.Minute), 0)
	}
	if len(history.Networks) != 48 || history.Networks["eth/eth0"] != nil {
		t.Fatalf("network LRU = %d entries; oldest = %+v", len(history.Networks), history.Networks["eth/eth0"])
	}
	for index := range 33 {
		history.Observe([]string{"wifi"}, fmt.Sprintf("member-%02d", index), true, 100, now.Add(time.Duration(index)*time.Minute), 0)
	}
	if len(history.Stats["wifi"]) != 32 {
		t.Fatalf("identity count = %d", len(history.Stats["wifi"]))
	}
	history.IdentityServers["timeline-member"] = "server"
	for index := range 97 {
		history.Observe([]string{"wifi"}, "timeline-member", true, 100, now.Add(time.Duration(index)*15*time.Minute), 0)
	}
	points := history.Timeline["wifi"]["timeline-member"]
	if len(points) != 96 || points[0].Time != now.Unix()/900*900+900 {
		t.Fatalf("timeline retention = %d, %+v", len(points), points[0])
	}
	for index := range 501 {
		history.AddEvent(smartEvent{Time: int64(index), Kind: "switch"})
	}
	if len(history.Events) != 500 || history.Events[0].Time != 1 {
		t.Fatalf("event retention = %d", len(history.Events))
	}
	data, err := history.Encode()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := decodeSmartHistory(data)
	if err != nil || restored.IdentityServers["timeline-member"] != "server" || len(restored.Timeline["wifi"]["timeline-member"]) != 96 {
		t.Fatalf("history round trip: %+v, %v", restored, err)
	}
	history.AddEvent(smartEvent{Reason: strings.Repeat("x", smartHistoryMaxBytes)})
	data, err = history.Encode()
	if err != nil || len(data) > smartHistoryMaxBytes {
		t.Fatalf("serialized bound = %d, %v", len(data), err)
	}
	history.Reset()
	if len(history.Networks)+len(history.Stats)+len(history.Events)+len(history.Labels)+len(history.Timeline)+len(history.IdentityServers) != 0 {
		t.Fatal("reset retained learned history")
	}
}

func TestSmartLiveHistoryBoundsIdentityMappingsWithoutPersistence(t *testing.T) {
	s, clock := smartTestGroup()
	member := s.primary
	for index := range 100 {
		member.identity = fmt.Sprintf("configuration-%03d", index)
		member.serverIdentity = fmt.Sprintf("server-%03d", index)
		s.recordObservationLocked(member, true, 100*time.Millisecond, clock.Now())
		if len(s.history.IdentityServers) != min(index+1, 32) {
			t.Fatalf("configuration %d retained %d identity mappings", index, len(s.history.IdentityServers))
		}
		for identity := range s.history.Stats["wifi"] {
			if s.history.IdentityServers[identity] == "" {
				t.Fatalf("live statistic lost server identity %s", identity)
			}
		}
		clock.Advance(time.Minute)
	}
}

func TestSmartHistoryRetainsIdentityUntilLastStatisticEviction(t *testing.T) {
	s, clock := smartTestGroup()
	member := s.primary
	member.identity, member.serverIdentity = "shared", "shared-server"
	for _, key := range []string{"wifi", "cell"} {
		s.network = smartNetwork{Key: key, Type: key, Path: []string{key}, Link: true}
		s.recordObservationLocked(member, true, 100*time.Millisecond, clock.Now())
		clock.Advance(time.Minute)
	}
	for _, key := range []string{"wifi", "cell"} {
		s.network = smartNetwork{Key: key, Type: key, Path: []string{key}, Link: true}
		for index := range 32 {
			member.identity = fmt.Sprintf("%s-%02d", key, index)
			member.serverIdentity = member.identity
			s.recordObservationLocked(member, true, 100*time.Millisecond, clock.Now())
			clock.Advance(time.Minute)
		}
		if s.history.Stats[key]["shared"] != nil {
			t.Fatalf("%s did not evict oldest statistic", key)
		}
		_, retained := s.history.IdentityServers["shared"]
		if retained != (key == "wifi") {
			t.Fatalf("shared identity retained after %s eviction: %v", key, retained)
		}
	}
}

func TestSmartHistoryNetworkEvictionKeepsNewObservationIdentity(t *testing.T) {
	s, clock := smartTestGroup()
	member := s.primary
	for index := range 49 {
		key := fmt.Sprintf("eth/eth%d", index)
		s.network = smartNetwork{Key: key, Type: "ethernet", Path: []string{key}, Link: true}
		member.identity = fmt.Sprintf("member-%d", index)
		if index == 48 {
			member.identity = "member-0"
		}
		member.serverIdentity = member.identity
		s.recordObservationLocked(member, true, 100*time.Millisecond, clock.Now())
		clock.Advance(time.Minute)
	}
	if s.history.Stats["eth/eth0"] != nil || s.history.IdentityServers["member-0"] != "member-0" {
		t.Fatal("network eviction lost the new observation's server identity")
	}
}

func TestSmartThroughputHistoryKeepsServerIdentityWithoutTCPObservation(t *testing.T) {
	s, clock := smartTestGroup()
	member := s.primary
	s.applyResultLocked(smartResult{
		member: member, kind: "throughput", generation: s.generation, configGeneration: s.configGeneration,
		memberGeneration: member.generation, download: 25,
	}, clock.Now())
	if s.history.Stats["wifi"][member.identity] == nil || s.history.IdentityServers[member.identity] != member.serverIdentity {
		t.Fatal("throughput-only statistic lost its server identity")
	}
}

func TestSmartHistoryRejectsInvalidStatistics(t *testing.T) {
	for _, data := range []string{
		"null",
		`{"stats":{"wifi":{"member":{"S":2,"W":1}}}}`,
		`{"stats":{"wifi":{"member":{"S":0,"W":-1}}}}`,
		`{"stats":{"wifi":{"member":null}}}`,
		`{"timeline":{"wifi":{"member":[{"success":1,"weight":0}]}}}`,
	} {
		if _, err := decodeSmartHistory([]byte(data)); err == nil {
			t.Errorf("accepted invalid history %s", data)
		}
	}
}

func TestSmartHistoryHourClassCap(t *testing.T) {
	for _, test := range []struct {
		name                           string
		base                           int
		probability, successes, weight float64
		want                           int
	}{
		{"insufficient weight", 0, 0.99, 0, 23.99, 0},
		{"bad hour capped", 0, 0.99, 0, 24, 1},
		{"good hour capped", 3, 0.2, 240, 240, 2},
		{"unchanged good class", 0, 0.99, 240, 240, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := smartHourClass(test.base, test.probability, test.successes, test.weight); got != test.want {
				t.Fatalf("hour class = %d; want %d", got, test.want)
			}
		})
	}
	start := time.Date(2026, 11, 9, 8, 0, 0, 0, time.FixedZone("platform", 3*3600))
	statistic := &smartStatistic{}
	smartObserveHour(statistic, true, 1, start)
	smartObserveHour(statistic, false, 1, start.Add(72*time.Hour))
	if statistic.Hours[2] != [2]float64{0.5, 1.5} {
		t.Fatalf("hour decay = %v; want [0.5 1.5]", statistic.Hours[2])
	}
}

func TestSmartConfiguredIdentityUsesPublicKeyAndClientCredential(t *testing.T) {
	private, err := hex.DecodeString("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a")
	if err != nil {
		t.Fatal(err)
	}
	options := option.WireGuardEndpointOptions{
		PrivateKey: base64.StdEncoding.EncodeToString(private),
		Peers:      []option.WireGuardPeer{{Address: "vpn.example", Port: 51820, PublicKey: "peer-public-key"}},
	}
	local, server, err := smartIdentityFromOptions("wireguard", options)
	if err != nil {
		t.Fatal(err)
	}
	private[0] ^= 1 // X25519 clamps these bits, so the derived public key is unchanged.
	options.PrivateKey = base64.StdEncoding.EncodeToString(private)
	equivalentLocal, equivalentServer, err := smartIdentityFromOptions("wireguard", options)
	if err != nil || equivalentLocal != local || equivalentServer != server {
		t.Fatalf("identity depended on private key text: local=%s server=%s err=%v", equivalentLocal, equivalentServer, err)
	}
	private[0] ^= 8
	options.PrivateKey = base64.StdEncoding.EncodeToString(private)
	changedLocal, unchangedServer, err := smartIdentityFromOptions("wireguard", options)
	if err != nil || changedLocal == local || unchangedServer != server {
		t.Fatalf("local key isolation failed: local=%s server=%s err=%v", changedLocal, unchangedServer, err)
	}
	options.Peers[0].PublicKey = "changed-peer-public-key"
	_, changedServer, err := smartIdentityFromOptions("wireguard", options)
	if err != nil || changedServer == server {
		t.Fatalf("server key change did not change identity: %s, %v", changedServer, err)
	}
	client := map[string]any{"server": "vpn.example", "server_port": 443, "uuid": "client-one", "transport": map[string]any{"type": "ws", "path": "/one"}}
	firstLocal, firstServer, err := smartIdentityFromOptions("vless", client)
	if err != nil {
		t.Fatal(err)
	}
	client["uuid"] = "client-two"
	secondLocal, secondServer, err := smartIdentityFromOptions("vless", client)
	if err != nil || firstLocal == secondLocal || firstServer != secondServer {
		t.Fatalf("client credential isolation failed: %v", err)
	}
	client["transport"] = map[string]any{"type": "ws", "path": "/two"}
	thirdLocal, thirdServer, err := smartIdentityFromOptions("vless", client)
	if err != nil || secondLocal == thirdLocal || secondServer != thirdServer {
		t.Fatalf("transport identity isolation failed: %v", err)
	}
}

func TestSmartNetworkHashAndPath(t *testing.T) {
	if got := smartHash([]byte("key"), "The quick brown fox jumps over the lazy dog"); got != "f7bc83f430538424" {
		t.Fatalf("network hash = %q", got)
	}
	path := smartNetworkPath("cell/25001/LTE/tower:0123456789abcdef")
	want := []string{"cell/25001/LTE/tower:0123456789abcdef", "cell/25001/LTE", "cell/25001", "cell"}
	if len(path) != len(want) {
		t.Fatalf("network path = %v", path)
	}
	for index := range path {
		if path[index] != want[index] {
			t.Fatalf("network path = %v; want %v", path, want)
		}
	}
}

func TestSmartHistoryRankingDoesNotCreateEvidenceOrEvict(t *testing.T) {
	history := newSmartHistory()
	now := time.Unix(1800000000, 0)
	for index := range 100 {
		identity := string(rune(index + 100))
		history.Class([]string{"wifi"}, identity, identity, now, nil)
	}
	if len(history.Stats) != 0 || len(history.Networks) != 0 {
		t.Fatal("ranking unknown members created or evicted learned history")
	}
}

func TestSmartHistoryHourBucketChangesClassByOne(t *testing.T) {
	history := newSmartHistory()
	now := time.Date(2026, 11, 9, 1, 0, 0, 0, time.UTC)
	stat := &smartStatistic{S: 99, W: 100, UpdatedAt: now.Unix()}
	stat.Hours[0] = [2]float64{0, 24}
	stat.HourUpdatedAt[0] = now.Unix()
	history.Stats["wifi"] = map[string]*smartStatistic{"member": stat}
	if class := history.Class([]string{"wifi"}, "member", "server", now, nil); class != 1 {
		t.Fatalf("bad hour must move class A only to B, got %d", class)
	}
	if class := history.Class([]string{"wifi"}, "member", "server", now.Add(4*time.Hour), nil); class != 0 {
		t.Fatalf("unobserved hour must retain class A, got %d", class)
	}
}
