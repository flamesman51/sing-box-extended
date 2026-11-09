package group

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
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
