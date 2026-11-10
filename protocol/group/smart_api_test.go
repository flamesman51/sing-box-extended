package group

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

type smartTestManager struct {
	adapter.OutboundManager
	values []adapter.Outbound
}

func (m smartTestManager) Outbounds() []adapter.Outbound { return m.values }

func TestSmartDocumentsAndHistoryReset(t *testing.T) {
	s, clock := smartTestGroup()
	ledger, err := getSmartLedger(nil)
	if err != nil {
		t.Fatal(err)
	}
	s.ledger = ledger
	s.installID, s.seed, s.directSeed = ledger.Installation()
	manager := smartTestManager{values: []adapter.Outbound{s}}
	primary := s.primary
	s.history.IdentityServers[primary.identity] = primary.serverIdentity
	s.history.Observe([]string{"wifi"}, primary.identity, true, 120, clock.now, 0)
	s.history.Networks["wifi"] = &smartHistoryNetwork{Type: "wifi", LastSeen: clock.now.Unix()}
	status, err := SmartStatus(context.Background(), manager, "")
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err = json.Unmarshal([]byte(status), &document); err != nil {
		t.Fatal(err)
	}
	if document["version"] != float64(1) || document["group"] != "" {
		t.Fatal(status)
	}
	timeline := document["timeline"].(map[string]any)
	members := timeline["members"].(map[string]any)
	if len(members[primary.tag].([]any)) != 96 {
		t.Fatal("timeline must contain 96 buckets")
	}
	exported, err := SmartExport(context.Background(), manager, false)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(exported, `"server_identity":""`) {
		t.Fatal(exported)
	}
	var export smartExportDocument
	if err = json.Unmarshal([]byte(exported), &export); err != nil {
		t.Fatal(err)
	}
	if export.InstallID == "" || !export.Redacted || len(export.Groups[""].Stats) != 1 {
		t.Fatal(exported)
	}
	lastOK, state := primary.lastOK, primary.state
	report, err := SmartImport(context.Background(), manager, exported, .25)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(report, `"applied":[{`) {
		t.Fatal(report)
	}
	if primary.lastOK != lastOK || primary.state != state {
		t.Fatal("import touched live health")
	}
	if err = SmartReset(context.Background(), manager, ""); err != nil {
		t.Fatal(err)
	}
	if len(s.history.Stats) != 0 || len(s.ledger.Metadata().Priors[s.Tag()]) != 0 || primary.lastOK != lastOK || primary.state != state {
		t.Fatal("reset did not preserve only live state")
	}
	if len(s.history.Events) != 1 || s.history.Events[0].Kind != "reset" {
		t.Fatal("reset must clear previous events")
	}
}

func TestSmartImportValidation(t *testing.T) {
	manager := smartTestManager{}
	for _, test := range []struct {
		data  string
		trust float64
		want  error
	}{
		{"{}", .25, ErrSmartUnsupportedVersion},
		{"{", .25, ErrSmartInvalid},
		{`{"version":1}`, 1, ErrSmartInvalid},
		{strings.Repeat("x", 2*1024*1024+1), .25, ErrSmartTooLarge},
	} {
		if _, err := SmartImport(context.Background(), manager, test.data, test.trust); !errors.Is(err, test.want) {
			t.Fatalf("got %v want %v", err, test.want)
		}
	}
	if _, err := SmartStatus(context.Background(), manager, ""); !errors.Is(err, ErrSmartNoGroup) {
		t.Fatal(err)
	}
	if _, err := SmartNetworks(context.Background(), manager, "missing"); !errors.Is(err, ErrSmartNotFound) {
		t.Fatal(err)
	}
}

func TestSmartImprovementNeedsDistinctObservationsAndDwell(t *testing.T) {
	s, clock := smartTestGroup()
	primary, challenger := s.primary, s.standby
	s.generationSucceeded = true
	primary.throughput = smartThroughput{Download: 10, At: clock.now, Source: "active"}
	challenger.throughput = smartThroughput{Download: 30, At: clock.now, Source: "active"}
	s.selectedAt = clock.now.Add(-9 * time.Minute)
	s.chooseLocked(clock.now)
	clock.Advance(30 * time.Second)
	challenger.lastOK = clock.now
	s.chooseLocked(clock.now)
	if s.primary != primary {
		t.Fatal("improvement switched before 10 minute dwell")
	}
	clock.Advance(30 * time.Second)
	challenger.lastOK = clock.now
	s.chooseLocked(clock.now)
	if s.primary != challenger {
		t.Fatal("confirmed throughput improvement did not switch")
	}
}

type smartHistorySave struct {
	data []byte
	err  error
}

type smartBlockingHistoryCache struct {
	adapter.CacheFile
	mu               sync.Mutex
	started, release chan struct{}
	startOnce        sync.Once
	completed        chan smartHistorySave
	data             []byte
	writes, failures int
}

func (c *smartBlockingHistoryCache) SaveSmartHistory(_ string, history *adapter.SmartHistory) error {
	if c.started != nil {
		c.startOnce.Do(func() { close(c.started) })
	}
	if c.release != nil {
		<-c.release
	}
	c.mu.Lock()
	c.writes++
	var err error
	if c.failures > 0 {
		c.failures--
		err = errors.New("history write failed")
	} else {
		c.data = append([]byte(nil), history.Data...)
	}
	c.mu.Unlock()
	if c.completed != nil {
		c.completed <- smartHistorySave{data: history.Data, err: err}
	}
	return err
}

func (c *smartBlockingHistoryCache) ResetSmartHistory(string) error {
	c.mu.Lock()
	c.data = nil
	c.mu.Unlock()
	return nil
}

func smartAwaitHistorySave(t *testing.T, s *Smart, cache *smartBlockingHistoryCache) smartHistorySave {
	t.Helper()
	select {
	case saved := <-cache.completed:
		s.persistenceMu.Lock()
		s.persistenceMu.Unlock()
		return saved
	case <-time.After(2 * time.Second):
		t.Fatal("history persistence did not finish")
		return smartHistorySave{}
	}
}

func TestSmartResetCannotBeOverwrittenByPendingSave(t *testing.T) {
	s, clock := smartTestGroup()
	cache := &smartBlockingHistoryCache{started: make(chan struct{}), release: make(chan struct{})}
	s.cache = cache
	s.recordObservationLocked(s.primary, true, time.Millisecond, clock.Now())
	var release sync.Once
	unblock := func() { release.Do(func() { close(cache.release) }) }
	t.Cleanup(func() { unblock(); _ = s.Close() })
	s.requestHistoryFlush()
	select {
	case <-cache.started:
	case <-time.After(2 * time.Second):
		t.Fatal("history save did not start")
	}
	clock.Advance(time.Minute)
	s.mu.Lock()
	s.eventLocked("switch", "a", "b", "wifi", "pending history before reset", clock.Now())
	s.mu.Unlock()
	s.requestHistoryFlush()
	reset := make(chan error, 1)
	go func() { reset <- SmartReset(context.Background(), smartTestManager{values: []adapter.Outbound{s}}, "") }()
	select {
	case err := <-reset:
		t.Fatalf("reset passed in-flight save: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-reset:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reset did not finish after persistence resumed")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cache.mu.Lock()
	data := append([]byte(nil), cache.data...)
	cache.mu.Unlock()
	history, err := decodeSmartHistory(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(history.Stats) != 0 || len(history.Events) != 1 || history.Events[0].Kind != "reset" {
		t.Fatal("a pending or in-flight snapshot overwrote reset")
	}
}

func TestSmartBlockedHistoryPersistenceDoesNotBlockOwner(t *testing.T) {
	s, clock := smartTestGroup()
	started := clock.Now()
	cache := &smartBlockingHistoryCache{
		started: make(chan struct{}), release: make(chan struct{}),
		completed: make(chan smartHistorySave, 4),
	}
	metadataStarted := smartEnableBlockedThroughput(t, s, cache)
	primary, standby := s.primary, s.standby
	s.timeout, s.udpEnabled, s.generationSucceeded = 8*time.Second, false, true
	s.lastDirect, s.lastFullRound, s.lastNetworkRead = started, started, started.Add(time.Hour)
	s.recordObservationLocked(primary, true, 80*time.Millisecond, started)
	calls := make(chan smartProbeCall, 16)
	for _, member := range s.members {
		member.lastProbe = started
		member.outbound = &smartTimedOutbound{
			smartFakeOutbound: smartFakeOutbound{tag: member.tag}, clock: clock, calls: calls,
			delay: 8 * time.Second, blackhole: member == primary,
		}
	}
	smartStartTestOwner(t, s, clock, nil)
	var release sync.Once
	unblock := func() { release.Do(func() { close(cache.release) }) }
	t.Cleanup(unblock)
	select {
	case <-cache.started:
	case <-time.After(2 * time.Second):
		t.Fatal("history save did not start")
	}
	smartLedgerWait(t, metadataStarted)
	selected := make(chan error, 1)
	go func() {
		decision, err := s.SelectMember(context.Background(), "tcp", M.Socksaddr{})
		if err == nil && decision.Outbound() != primary.outbound {
			err = errors.New("unexpected primary while persistence is blocked")
		}
		selected <- err
	}()
	select {
	case err := <-selected:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("selection waited for history persistence")
	}
	clock.Advance(15 * time.Second)
	s.request("demand")
	smartAwaitProbe(t, calls, "a", 1)
	clock.Advance(8 * time.Second)
	s.request("demand")
	confirmed := make(map[string]bool)
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for len(confirmed) < 2 {
		select {
		case call := <-calls:
			if call.operator == 2 && (call.tag == "a" || call.tag == "b") {
				confirmed[call.tag] = true
			}
		case <-deadline.C:
			t.Fatal("completed primary failure did not schedule confirmation and standby validation")
		}
	}
	clock.Advance(8 * time.Second)
	s.request("demand")
	smartAwaitGroup(t, s, clock, func() bool {
		return primary.state == "quarantined" && s.primary == standby && standby.lastOK.Equal(clock.Now())
	})
	decision, err := s.SelectMember(context.Background(), "tcp", M.Socksaddr{})
	if err != nil || decision.Outbound() != standby.outbound {
		t.Fatalf("completed failure did not remove the dead primary: %v", err)
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("close did not wait for blocked persistence: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("close did not finish after persistence resumed")
	}
	first := smartAwaitHistorySave(t, s, cache)
	latest := smartAwaitHistorySave(t, s, cache)
	oldHistory, err := decodeSmartHistory(first.data)
	if err != nil {
		t.Fatal(err)
	}
	history, err := decodeSmartHistory(latest.data)
	if err != nil {
		t.Fatal(err)
	}
	cache.mu.Lock()
	writes := cache.writes
	cache.mu.Unlock()
	if first.err != nil || latest.err != nil || writes != 2 {
		t.Fatalf("shutdown must write one final snapshot: writes=%d errors=%v, %v", writes, first.err, latest.err)
	}
	if !oldHistory.Stats["wifi"][primary.identity].LastSuccess || history.Stats["wifi"][primary.identity].LastSuccess {
		t.Fatal("shutdown did not preserve updates made during the blocked write")
	}
	if len(history.Events) == 0 || history.Events[len(history.Events)-1].Kind != "failover" {
		t.Fatal("shutdown did not persist the latest owner events")
	}
}

func TestSmartHistoryPersistenceRetriesDirtyWritesAfterDebounce(t *testing.T) {
	s, clock := smartTestGroup()
	cache := &smartBlockingHistoryCache{failures: 1, completed: make(chan smartHistorySave, 4)}
	s.cache = cache
	s.recordObservationLocked(s.primary, true, time.Millisecond, clock.Now())
	t.Cleanup(func() { _ = s.Close() })
	s.requestHistoryFlush()
	if saved := smartAwaitHistorySave(t, s, cache); saved.err == nil {
		t.Fatal("write failure was not exercised")
	}
	s.mu.Lock()
	dirty := s.dirty
	s.eventLocked("recover", "", "a", "wifi", "updated after failed write", clock.Now())
	s.mu.Unlock()
	if !dirty {
		t.Fatal("failed write lost the dirty history")
	}
	clock.Advance(59 * time.Second)
	for range 10 {
		s.requestHistoryFlush()
	}
	cache.mu.Lock()
	writes := cache.writes
	cache.mu.Unlock()
	if writes != 1 {
		t.Fatal("failed history writes bypassed the one-minute debounce")
	}
	clock.Advance(time.Second)
	s.requestHistoryFlush()
	saved := smartAwaitHistorySave(t, s, cache)
	if saved.err != nil {
		t.Fatal(saved.err)
	}
	history, err := decodeSmartHistory(saved.data)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	dirty = s.dirty
	s.mu.Unlock()
	if dirty || len(history.Events) != 1 || history.Events[0].Reason != "updated after failed write" {
		t.Fatal("retry did not persist the latest dirty history")
	}
}

func TestSmartHistoryPersistenceSnapshotIsDeep(t *testing.T) {
	s, clock := smartTestGroup()
	s.recordObservationLocked(s.primary, true, time.Millisecond, clock.Now())
	s.eventLocked("switch", "a", "b", "wifi", "snapshot", clock.Now())
	statistic := s.history.Stats["wifi"]["a"]
	statistic.Flaps = []int64{clock.Now().Unix()}
	s.history.Labels["wifi"] = "original"
	before, err := json.Marshal(s.history)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := cloneSmartHistory(s.history)
	s.history.Networks["wifi"].Label = "changed"
	s.history.Labels["wifi"] = "changed"
	s.history.IdentityServers["a"] = "changed"
	statistic.W++
	statistic.RTTs[0]++
	statistic.Flaps[0]++
	statistic.Hours[0][0]++
	s.history.Timeline["wifi"]["a"][0].RTT++
	*s.history.Events[0].From = "changed"
	*s.history.Events[0].To = "changed"
	*s.history.Events[0].Network = "changed"
	s.history.recency["wifi"]++
	s.history.sequence++
	after, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) || snapshot.recency["wifi"] == s.history.recency["wifi"] || snapshot.sequence == s.history.sequence {
		t.Fatal("history snapshot shares mutable state")
	}
}

func TestSmartHistoryPersistenceBoundsOnlySnapshot(t *testing.T) {
	s, clock := smartTestGroup()
	cache := &smartBlockingHistoryCache{completed: make(chan smartHistorySave, 2)}
	s.cache = cache
	s.recordObservationLocked(s.primary, true, time.Millisecond, clock.Now())
	s.eventLocked("switch", "a", "b", "wifi", strings.Repeat("x", smartHistoryMaxBytes), clock.Now())
	t.Cleanup(func() { _ = s.Close() })
	s.requestHistoryFlush()
	saved := smartAwaitHistorySave(t, s, cache)
	if saved.err != nil || len(saved.data) > smartHistoryMaxBytes {
		t.Fatalf("persisted snapshot exceeded the history bound: bytes=%d error=%v", len(saved.data), saved.err)
	}
	history, err := decodeSmartHistory(saved.data)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	events := len(s.history.Events)
	s.mu.Unlock()
	if events != 1 || len(history.Events) != 0 || len(history.Stats) != 1 {
		t.Fatal("serialization eviction mutated live history or discarded bounded statistics")
	}
}

func TestSmartImportedPriorsListNetworksWithoutLiveEvidence(t *testing.T) {
	s, clock := smartTestGroup()
	ledger, err := getSmartLedger(nil)
	if err != nil {
		t.Fatal(err)
	}
	s.ledger = ledger
	s.installID, s.seed, s.directSeed = ledger.Installation()
	label, carrier := "imported network", "carrier"
	document := smartExportDocument{Version: 1, ExportedAt: clock.now.Unix(), InstallID: s.installID, Groups: map[string]smartExportGroup{
		"source": {Networks: map[string]smartExportNetwork{"wifi": {Type: "wifi", Label: &label, Carrier: &carrier, LastSeen: clock.now.Unix()}}, Stats: []smartExportStatistic{{Network: "wifi", ServerIdentity: s.primary.serverIdentity, S: 9, W: 10}}},
	}}
	data, err := smartJSON(document)
	if err != nil {
		t.Fatal(err)
	}
	manager := smartTestManager{values: []adapter.Outbound{s}}
	if _, err = SmartImport(context.Background(), manager, data, .25); err != nil {
		t.Fatal(err)
	}
	networks, err := SmartNetworks(context.Background(), manager, "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(networks, label) || !strings.Contains(networks, carrier) || len(s.history.Stats) != 0 {
		t.Fatalf("imported history projection: %s", networks)
	}
	if s.primary.state != "healthy" || s.primary.lastOK != clock.now {
		t.Fatal("import touched live health")
	}
	if err = SmartReset(context.Background(), manager, ""); err != nil {
		t.Fatal(err)
	}
}

func TestSmartHistorySnapshotEncodeDoesNotShareOutageBaselines(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "stability-downtime")
	member := s.primary
	s.observeOutagesLocked(member, clock.Now())
	s.history.Networks[s.network.Key].Label = strings.Repeat("x", smartHistoryMaxBytes)
	snapshot := cloneSmartHistory(s.history)
	if snapshot.outageObservations != nil {
		t.Fatal("persistence snapshot retained runtime outage observations")
	}
	type encodedSnapshot struct {
		data []byte
		err  error
	}
	start := make(chan struct{})
	done := make(chan encodedSnapshot, 1)
	go func() {
		<-start
		data, err := snapshot.Encode()
		done <- encodedSnapshot{data: data, err: err}
	}()
	close(start)
	for range 1000 {
		clock.Advance(time.Second)
		s.observeOutagesLocked(member, clock.Now())
	}
	result := <-done
	if result.err != nil || len(result.data) > smartHistoryMaxBytes || len(snapshot.Networks) != 0 {
		t.Fatalf("oversized snapshot was not safely evicted: bytes=%d networks=%d err=%v", len(result.data), len(snapshot.Networks), result.err)
	}
	baseline, exists := s.history.outageObservations[member.identity]
	if !exists || !baseline.at.Equal(clock.Now()) || s.history.Stats[s.network.Key][member.identity].OutageExposureHours <= 0 {
		t.Fatal("snapshot eviction erased or reset the live outage baseline")
	}
}
