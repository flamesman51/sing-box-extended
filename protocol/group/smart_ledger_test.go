package group

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"
)

type smartLedgerTestCache struct {
	adapter.CacheFile
	data []byte
	fail bool
}

func (c *smartLedgerTestCache) LoadSmartMetadata() (*adapter.SmartMetadata, error) {
	if len(c.data) == 0 {
		return nil, nil
	}
	return &adapter.SmartMetadata{Data: append([]byte(nil), c.data...)}, nil
}

func (c *smartLedgerTestCache) SaveSmartMetadata(m *adapter.SmartMetadata) error {
	if c.fail {
		return errors.New("save failed")
	}
	c.data = append([]byte(nil), m.Data...)
	return nil
}

func TestSmartLedgerMinimumSerialRateAndRollback(t *testing.T) {
	cache := new(smartLedgerTestCache)
	ledger, err := getSmartLedger(cache)
	if err != nil {
		t.Fatal(err)
	}
	ledger.Register("a", map[string]float64{"wifi": 300})
	ledger.Register("b", map[string]float64{"wifi": 50})
	now := time.Date(2026, 11, 9, 23, 0, 0, 0, time.UTC)
	for i := range 4 {
		id, capBytes, reserveErr := ledger.Reserve(context.Background(), "wifi", now.Add(time.Duration(i)*time.Minute), 0)
		if reserveErr != nil {
			t.Fatal(reserveErr)
		}
		if i == 0 && capBytes != 10_000_000 {
			t.Fatalf("cap = %d", capBytes)
		}
		if _, _, reserveErr = ledger.Reserve(context.Background(), "wifi", now, 0); !errors.Is(reserveErr, errSmartTestBusy) {
			t.Fatalf("serial admission: %v", reserveErr)
		}
		if err = ledger.Reconcile(id, 1_000_000, false); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err = ledger.Reserve(context.Background(), "wifi", now.Add(5*time.Minute), 0); !errors.Is(err, errSmartTestRate) {
		t.Fatalf("hourly admission: %v", err)
	}
	snapshot := ledger.Snapshot("wifi", now.Add(-24*time.Hour), 0)
	if snapshot.Day != "2026-11-09" || snapshot.UsedMB != 4 || snapshot.LimitMB != 50 {
		t.Fatalf("rollback snapshot: %+v", snapshot)
	}
	shared, err := getSmartLedger(cache)
	if err != nil || shared != ledger {
		t.Fatal("installation ledger not shared")
	}
}

func TestSmartLedgerRestartChargesOrphansAndPreservesMetadata(t *testing.T) {
	cache := &smartLedgerTestCache{data: []byte(`{"unrelated":{"keep":true}}`)}
	ledger, err := getSmartLedger(cache)
	if err != nil {
		t.Fatal(err)
	}
	ledger.Register("g", map[string]float64{"wifi": 100, "cellular": 50})
	now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	_, reserved, err := ledger.Reserve(context.Background(), "wifi", now, 0)
	if err != nil {
		t.Fatal(err)
	}
	restartedCache := &smartLedgerTestCache{data: append([]byte(nil), cache.data...)}
	restarted, err := getSmartLedger(restartedCache)
	if err != nil {
		t.Fatal(err)
	}
	restarted.Register("g", map[string]float64{"wifi": 100})
	snapshot := restarted.Snapshot("wifi", now, 0)
	if snapshot.UsedMB != float64(reserved)/1_000_000 || snapshot.ReservedMB != 0 {
		t.Fatalf("orphan snapshot: %+v", snapshot)
	}
	if string(restarted.raw["unrelated"]) != `{"keep":true}` {
		t.Fatal("unrelated metadata lost")
	}
	before := restarted.Metadata()
	failureCache := new(smartLedgerTestCache)
	failureLedger, err := getSmartLedger(failureCache)
	if err != nil {
		t.Fatal(err)
	}
	failureLedger.Register("g", map[string]float64{"wifi": 100})
	failureCache.fail = true
	if _, _, err = failureLedger.Reserve(context.Background(), "wifi", now.Add(2*time.Hour), 0); err == nil {
		t.Fatal("save failure admitted test")
	}
	if failureLedger.Snapshot("wifi", now, 0).ReservedMB != 0 {
		t.Fatal("save failure retained reservation")
	}
	if before.InstallID != ledger.Metadata().InstallID {
		t.Fatal("identity changed on restart")
	}
}

func TestSmartLedgerUncertainChargesCellularAndDisabledCache(t *testing.T) {
	ledger, err := getSmartLedger(new(smartLedgerTestCache))
	if err != nil {
		t.Fatal(err)
	}
	ledger.Register("g", map[string]float64{"wifi": 100, "cellular": 50})
	now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	id, _, err := ledger.Reserve(context.Background(), "wifi", now, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = ledger.Reconcile(id, 1_100_000, true); err != nil {
		t.Fatal(err)
	}
	if ledger.Snapshot("wifi", now, 0).UsedMB != 0 || math.Abs(ledger.Snapshot("cellular", now, 0).UsedMB-1.1) > 1e-9 {
		t.Fatal("uncertain bytes not charged to cellular")
	}
	disabled, err := getSmartLedger(nil)
	if err != nil {
		t.Fatal(err)
	}
	disabled.Register("g", map[string]float64{"wifi": 300})
	if _, _, err = disabled.Reserve(context.Background(), "wifi", now, 0); !errors.Is(err, errSmartTestsDisabled) {
		t.Fatalf("disabled cache: %v", err)
	}
	again, err := getSmartLedger(nil)
	if err != nil || again != disabled {
		t.Fatal("disabled installation not shared")
	}
	idValue, hmacSeed, directSeed := disabled.Installation()
	if idValue == "" || len(hmacSeed) != 32 || len(directSeed) != 32 {
		t.Fatal("disabled cache lost installation identity")
	}
}

func TestSmartLedgerConcurrentAdmissionAndMinimumCap(t *testing.T) {
	ledger, err := getSmartLedger(new(smartLedgerTestCache))
	if err != nil {
		t.Fatal(err)
	}
	ledger.Register("g", map[string]float64{"wifi": 5.5})
	now := time.Date(2026, 11, 9, 23, 0, 0, 0, time.UTC)
	type admission struct {
		id    string
		bytes int64
		err   error
	}
	results := make(chan admission, 16)
	for range 16 {
		go func() {
			id, bytes, reserveErr := ledger.Reserve(context.Background(), "wifi", now, 10800)
			results <- admission{id, bytes, reserveErr}
		}()
	}
	var winner admission
	for range 16 {
		result := <-results
		if result.err == nil {
			if winner.id != "" {
				t.Fatal("concurrent tests admitted")
			}
			winner = result
		} else if !errors.Is(result.err, errSmartTestBusy) {
			t.Fatal(result.err)
		}
	}
	if winner.id == "" || winner.bytes != 1_100_000 {
		t.Fatalf("minimum reservation: %+v", winner)
	}
	if err = ledger.Reconcile(winner.id, 1_100_000, false); err != nil {
		t.Fatal(err)
	}
	if _, _, err = ledger.Reserve(context.Background(), "wifi", now.Add(2*time.Hour), 0); !errors.Is(err, errSmartBudget) {
		t.Fatalf("minimum cap not enforced: %v", err)
	}
	snapshot := ledger.Snapshot("wifi", now.Add(-24*time.Hour), 0)
	if snapshot.Day != "2026-11-10" || snapshot.UsedMB != 1.1 {
		t.Fatalf("timezone rollback reset budget: %+v", snapshot)
	}
}

func TestSmartLedgerReconcileSaveFailureKeepsReservation(t *testing.T) {
	cache := new(smartLedgerTestCache)
	ledger, err := getSmartLedger(cache)
	if err != nil {
		t.Fatal(err)
	}
	ledger.Register("g", map[string]float64{"wifi": 100})
	now := time.Now()
	id, reserved, err := ledger.Reserve(context.Background(), "wifi", now, 0)
	if err != nil {
		t.Fatal(err)
	}
	cache.fail = true
	if err = ledger.Reconcile(id, 1_000_000, false); err == nil {
		t.Fatal("failed reconciliation save accepted")
	}
	snapshot := ledger.Snapshot("wifi", now, 0)
	if snapshot.UsedMB != 0 || snapshot.ReservedMB != float64(reserved)/1_000_000 {
		t.Fatalf("reconciliation did not roll back: %+v", snapshot)
	}
	cache.fail = false
	if err = ledger.Reconcile(id, 1_000_000, false); err != nil {
		t.Fatal(err)
	}
}

type smartLedgerPowerSavePlatform struct{ adapter.PlatformInterface }

func (*smartLedgerPowerSavePlatform) IsPowerSaveMode() bool { return true }

func TestSmartLedgerPauseAndPowerSaveDenyAdmission(t *testing.T) {
	ledger, err := getSmartLedger(new(smartLedgerTestCache))
	if err != nil {
		t.Fatal(err)
	}
	ledger.Register("g", map[string]float64{"wifi": 100})
	ctx := service.ContextWith[adapter.PlatformInterface](context.Background(), &smartLedgerPowerSavePlatform{})
	if _, _, err = ledger.Reserve(ctx, "wifi", time.Now(), 0); !errors.Is(err, errSmartTestsDisabled) {
		t.Fatalf("power-save admission: %v", err)
	}
	ctx = pause.WithDefaultManager(context.Background())
	service.FromContext[pause.Manager](ctx).DevicePause()
	if _, _, err = ledger.Reserve(ctx, "wifi", time.Now(), 0); !errors.Is(err, errSmartTestsDisabled) {
		t.Fatalf("paused admission: %v", err)
	}
}

func TestSmartLedgerMetadataUpdatesPreserveGroupsAndReservations(t *testing.T) {
	cache := new(smartLedgerTestCache)
	ledger, err := getSmartLedger(cache)
	if err != nil {
		t.Fatal(err)
	}
	ledger.Register("a", map[string]float64{"wifi": 100})
	id, reserved, err := ledger.Reserve(context.Background(), "wifi", time.Now(), 0)
	if err != nil {
		t.Fatal(err)
	}
	priors := []smartPrior{{Network: "wifi", ServerIdentity: "server", Source: "source", S: 1, W: 2, At: 10}}
	sources := []smartPriorSource{{ID: "source", At: 10}}
	if err = ledger.UpdatePriors("a", priors, sources); err != nil {
		t.Fatal(err)
	}
	if err = ledger.UpdatePriors("b", priors, sources); err != nil {
		t.Fatal(err)
	}
	if err = ledger.UpdatePriors("a", nil, nil); err != nil {
		t.Fatal(err)
	}
	metadata := ledger.Metadata()
	if len(metadata.Priors["a"]) != 0 || len(metadata.Priors["b"]) != 1 || metadata.Ledger.Reservations[id].Bytes != reserved {
		t.Fatal("metadata update erased another owner")
	}
	metadata.Priors["b"][0].S = 100
	if ledger.Metadata().Priors["b"][0].S != 1 {
		t.Fatal("metadata snapshot mutated owner")
	}
	cache.fail = true
	if err = ledger.UpdatePriors("b", nil, nil); err == nil {
		t.Fatal("prior save failure ignored")
	}
	if len(ledger.Metadata().Priors["b"]) != 1 {
		t.Fatal("prior save failure did not roll back")
	}
}

func TestSmartLedgerRemovedGroupDoesNotLimitSurvivors(t *testing.T) {
	cache := new(smartLedgerTestCache)
	ledger, err := getSmartLedger(cache)
	if err != nil {
		t.Fatal(err)
	}
	ledger.Register("a", map[string]float64{"wifi": 50})
	ledger.Register("b", map[string]float64{"wifi": 300})
	ledger.Unregister("a")
	if got := ledger.Snapshot("wifi", time.Unix(1800000000, 0), 0).LimitMB; got != 300 {
		t.Fatalf("removed group kept budget limit %v", got)
	}
	ledger.Unregister("b")
}
