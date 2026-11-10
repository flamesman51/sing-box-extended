package group

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"
)

func smartDegradationTestLedger(t *testing.T, limits map[string]float64) (*smartLedger, *smartLedgerTestCache) {
	t.Helper()
	cache := new(smartLedgerTestCache)
	ledger, err := getSmartLedger(cache)
	if err != nil {
		t.Fatal(err)
	}
	ledger.Register("degradation", limits)
	t.Cleanup(func() { ledger.Unregister("degradation") })
	return ledger, cache
}

func TestSmartLedgerRejectsNegativePlanEstimate(t *testing.T) {
	ledger, cache := smartDegradationTestLedger(t, map[string]float64{"wifi": 300})
	before := string(cache.data)
	id, reserved, err := ledger.ReservePlan(context.Background(), "wifi", time.Now(), 0, -1)
	if err == nil || id != "" || reserved != 0 || string(cache.data) != before {
		t.Fatalf("negative estimate changed admission state: id=%q reserved=%d err=%v", id, reserved, err)
	}
}

func TestSmartDegradationLedgerWholePlanAdmission(t *testing.T) {
	ledger, cache := smartDegradationTestLedger(t, map[string]float64{"wifi": 300})
	now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	ledger.mu.Lock()
	ledger.state.Days["wifi/2026-11-09"] = 285_000_000
	ledger.state.Days["degradation/wifi/2026-11-09"] = 30_000_000
	ledger.mu.Unlock()
	before := string(cache.data)
	if _, _, err := ledger.ReservePlan(context.Background(), "wifi", now, 0, 20_000_000); !errors.Is(err, errSmartBudget) {
		t.Fatalf("two-phase plan that exceeds the remainder = %v", err)
	}
	if state := ledger.Metadata().Ledger; len(state.Starts) != 0 || len(state.Reservations) != 0 || string(cache.data) != before {
		t.Fatalf("skipped episode changed admission state: %+v", state)
	}
	id, reserved, err := ledger.ReservePlan(context.Background(), "wifi", now, 0, 10_000_000)
	if err != nil || reserved != 15_000_000 {
		t.Fatalf("one-phase plan after episode denial = %d, %v", reserved, err)
	}
	if err = ledger.Reconcile(context.Background(), id, 11_000_000, false); err != nil {
		t.Fatal(err)
	}
	if state := ledger.Metadata().Ledger; state.Days["wifi/2026-11-09"] != 296_000_000 || state.Days["degradation/wifi/2026-11-09"] != 30_000_000 {
		t.Fatalf("plan used a separate degradation allowance: %+v", state)
	}
}

func TestSmartLedgerChargedAdmissionBoundaries(t *testing.T) {
	for _, test := range []struct {
		network string
		limitMB float64
	}{
		{"wifi", 300},
		{"cellular", 50},
		{"wifi", 5000},
		{"cellular", 1000},
		{"unknown", 1000},
	} {
		t.Run(fmt.Sprintf("%s_%g", test.network, test.limitMB), func(t *testing.T) {
			ledger, cache := smartDegradationTestLedger(t, map[string]float64{smartBudgetType(test.network): test.limitMB})
			now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
			limit := int64(test.limitMB * 1_000_000)
			for _, remaining := range []int64{limit, 1_099_999, 11, 1} {
				ledger.mu.Lock()
				ledger.state.Days[smartBudgetType(test.network)+"/2026-11-09"] = limit - remaining
				ledger.mu.Unlock()
				if _, _, err := ledger.ReservePlan(context.Background(), test.network, now, 0, remaining+1); !errors.Is(err, errSmartBudget) {
					t.Fatalf("remaining %d admitted estimate %d: %v", remaining, remaining+1, err)
				}
				id, reserved, err := ledger.ReservePlan(context.Background(), test.network, now, 0, remaining)
				if err != nil || reserved != remaining {
					t.Fatalf("exact remaining %d = %d, %v", remaining, reserved, err)
				}
				var durable smartInstallation
				if err = json.Unmarshal(cache.data, &durable); err != nil {
					t.Fatal(err)
				}
				if reservation := durable.Ledger.Reservations[id]; reservation.Bytes != remaining || reservation.Format != "charged-v2" || reservation.Purpose != "" {
					t.Fatalf("full charged reservation was not durable: %+v", reservation)
				}
				if err = ledger.Reconcile(context.Background(), id, 0, false); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestSmartDegradationLedgerRestartOrphansAndDayRollback(t *testing.T) {
	ledger, cache := smartDegradationTestLedger(t, map[string]float64{"wifi": 5000})
	now := time.Date(2026, 11, 9, 23, 30, 0, 0, time.UTC)
	_, reserved, err := ledger.ReservePlan(context.Background(), "wifi", now, 0, 10_000_000)
	if err != nil || reserved != 5_000_000_000 {
		t.Fatalf("episode reservation = %d, %v", reserved, err)
	}
	for i := range 2 {
		restartedCache := &smartLedgerTestCache{data: append([]byte(nil), cache.data...)}
		restarted, restartErr := getSmartLedger(restartedCache)
		if restartErr != nil {
			t.Fatal(restartErr)
		}
		restarted.Register("restart", map[string]float64{"wifi": 5000})
		if got := restarted.Snapshot("wifi", now.Add(-24*time.Hour), 0); got.Day != "2026-11-09" || got.UsedMB != 5000 || got.ReservedMB != 0 {
			t.Fatalf("restart %d charged orphan incorrectly: %+v", i, got)
		}
		restarted.Unregister("restart")
		cache = restartedCache
	}
	restarted, err := getSmartLedger(cache)
	if err != nil {
		t.Fatal(err)
	}
	restarted.Register("restart", map[string]float64{"wifi": 5000})
	defer restarted.Unregister("restart")
	if got := restarted.Snapshot("wifi", now.Add(2*time.Hour), 0); got.Day != "2026-11-10" || got.UsedMB != 0 {
		t.Fatalf("daily rollover: %+v", got)
	}
	id, _, err := restarted.ReservePlan(context.Background(), "wifi", now, 0, 10_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if err = restarted.Reconcile(context.Background(), id, 2_750_000, false); err != nil {
		t.Fatal(err)
	}
	if got := restarted.Snapshot("wifi", now.Add(-48*time.Hour), 0); got.Day != "2026-11-10" || got.UsedMB != 2.75 {
		t.Fatalf("rolled-back reservation reset the day: %+v", got)
	}
}

func TestSmartDegradationLedgerUncertainCellularAttribution(t *testing.T) {
	ledger, cache := smartDegradationTestLedger(t, map[string]float64{"wifi": 5000, "cellular": 50})
	now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	id, _, err := ledger.ReservePlan(context.Background(), "wifi", now, 0, 10_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if cell := ledger.Snapshot("cellular", now, 0); cell.ReservedMB != 0 {
		t.Fatalf("cellular exposure pre-reserved: %+v", cell)
	}
	cache.fail = true
	if err = ledger.Reconcile(context.Background(), id, 55_000_000, true); err == nil {
		t.Fatal("failed save accepted reconciliation")
	}
	if got := ledger.Snapshot("wifi", now, 0); got.ReservedMB != 5000 || got.UsedMB != 0 {
		t.Fatalf("failed save lost reserved headroom: %+v", got)
	}
	cache.fail = false
	if err = ledger.Reconcile(context.Background(), id, 55_000_000, true); err != nil {
		t.Fatal(err)
	}
	if wifi := ledger.Snapshot("wifi", now, 0); wifi.UsedMB != 0 || wifi.ReservedMB != 0 {
		t.Fatalf("uncertain transfer retained wifi charge: %+v", wifi)
	}
	if cell := ledger.Snapshot("cellular", now, 0); cell.UsedMB != 55 {
		t.Fatalf("uncertain actual charge was capped at the cellular budget: %+v", cell)
	}
	restarted, err := getSmartLedger(&smartLedgerTestCache{data: append([]byte(nil), cache.data...)})
	if err != nil {
		t.Fatal(err)
	}
	if cell := restarted.Snapshot("cellular", now, 0); cell.UsedMB != 55 {
		t.Fatalf("restart lost reattributed charge: %+v", cell)
	}
}

func TestSmartDegradationLedgerCooldownPersistenceAndPruning(t *testing.T) {
	ledger, cache := smartDegradationTestLedger(t, map[string]float64{"wifi": 5000})
	ctx := context.Background()
	now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	until := now.Add(time.Hour)
	if err := ledger.SetDegradationCooldown(ctx, "hashed-primary-network", now, until); err != nil {
		t.Fatal(err)
	}
	if err := ledger.SetDegradationCooldown(ctx, "hashed-group-challenger", now, now.Add(30*time.Minute)); err != nil {
		t.Fatal(err)
	}
	metadata := ledger.Metadata()
	delete(metadata.Ledger.DegradationCooldowns, "hashed-primary-network")
	if got := ledger.DegradationCooldown("hashed-primary-network"); !got.Equal(until) {
		t.Fatalf("metadata clone changed cooldown: %v", got)
	}
	if err := ledger.UpdatePriors("degradation", nil, nil); err != nil {
		t.Fatal(err)
	}
	cache.fail = true
	if err := ledger.SetDegradationCooldown(ctx, "hashed-primary-network", now, now.Add(2*time.Hour)); err == nil {
		t.Fatal("cooldown save failure ignored")
	}
	if got := ledger.DegradationCooldown("hashed-primary-network"); !got.Equal(until) {
		t.Fatalf("failed cooldown write changed memory: %v", got)
	}
	cache.fail = false
	restarted, err := getSmartLedger(&smartLedgerTestCache{data: append([]byte(nil), cache.data...)})
	if err != nil {
		t.Fatal(err)
	}
	restarted.Register("restart", map[string]float64{"wifi": 5000})
	defer restarted.Unregister("restart")
	if got := restarted.DegradationCooldown("hashed-primary-network"); !got.Equal(until) {
		t.Fatalf("restart lost cooldown: %v", got)
	}
	if err = restarted.SetDegradationCooldown(ctx, "hashed-next", until, until.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if !restarted.DegradationCooldown("hashed-primary-network").IsZero() || !restarted.DegradationCooldown("hashed-group-challenger").IsZero() {
		t.Fatal("cooldown update retained expired entries")
	}
	if err = restarted.SetDegradationCooldown(ctx, "hashed-next", now, now.Add(30*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := restarted.DegradationCooldown("hashed-next"); !got.Equal(until.Add(time.Hour)) {
		t.Fatalf("clock rollback shortened cooldown: %v", got)
	}
	id, _, err := restarted.ReservePlan(ctx, "wifi", until.Add(time.Hour), 0, 10_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if !restarted.DegradationCooldown("hashed-next").IsZero() {
		t.Fatal("reservation update retained expired cooldown")
	}
	if err = restarted.Reconcile(ctx, id, 0, false); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(restarted.Snapshot("wifi", now, 0))
	if err != nil {
		t.Fatal(err)
	}
	var status map[string]json.RawMessage
	if err = json.Unmarshal(encoded, &status); err != nil {
		t.Fatal(err)
	}
	if _, exists := status["degradation"]; exists {
		t.Fatalf("removed allowance still appears in status: %s", encoded)
	}
}

func TestSmartLedgerOldReservationRecovery(t *testing.T) {
	for _, test := range []struct {
		name      string
		bytes     int64
		purpose   string
		format    string
		charged   int64
		wantError bool
	}{
		{name: "legacy_ordinary", bytes: 20_000_000, charged: 20_000_000},
		{name: "legacy_confirmation", bytes: 10_000_000, purpose: "degradation_confirmation", charged: 11_000_000},
		{name: "legacy_unknown_purpose", bytes: 10_000_001, purpose: "unknown-purpose", charged: 11_000_002},
		{name: "legacy_invalid_cap", bytes: 25_000_001, purpose: "unknown-purpose", wantError: true},
		{name: "charged_v2", bytes: 5_000_000_000, format: "charged-v2", charged: 5_000_000_000},
		{name: "charged_marker_overrides_legacy_purpose", bytes: 50_000_000, purpose: "unknown-purpose", format: "charged-v2", charged: 50_000_000},
		{name: "negative", bytes: -1, wantError: true},
		{name: "unknown_format", bytes: 1, format: "future", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := json.Marshal(smartInstallation{Ledger: smartLedgerState{
				Latest: map[string]string{"wifi": "2026-11-09"},
				Days:   map[string]int64{"wifi/2026-11-09": 123, "degradation/wifi/2026-11-09": 7},
				Reservations: map[string]smartReservation{
					"orphan": {NetworkType: "wifi", Day: "2026-11-09", Bytes: test.bytes, Purpose: test.purpose, Format: test.format},
				},
			}})
			if err != nil {
				t.Fatal(err)
			}
			cache := &smartLedgerTestCache{data: encoded}
			ledger, err := getSmartLedger(cache)
			if test.wantError {
				if err == nil {
					t.Fatal("invalid orphan admitted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			state := ledger.Metadata().Ledger
			if state.Days["wifi/2026-11-09"] != 123+test.charged || state.Days["degradation/wifi/2026-11-09"] != 7 || len(state.Reservations) != 0 {
				t.Fatalf("orphan lost its original accounting meaning: %+v", state)
			}
			restarted, err := getSmartLedger(&smartLedgerTestCache{data: append([]byte(nil), cache.data...)})
			if err != nil {
				t.Fatal(err)
			}
			if got := restarted.Metadata().Ledger.Days["wifi/2026-11-09"]; got != 123+test.charged {
				t.Fatalf("second restart charged the orphan twice: %d", got)
			}
		})
	}
}

func TestSmartLedgerAccountingPanicBeyondEstimateRecoversFullReservation(t *testing.T) {
	cache := new(smartPanicMetadataCache)
	ledger, err := getSmartLedger(cache)
	if err != nil {
		t.Fatal(err)
	}
	ledger.Register("panic", map[string]float64{"wifi": 300})
	defer ledger.Unregister("panic")
	now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	id, reserved, err := ledger.ReservePlan(context.Background(), "wifi", now, 0, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	cache.panicSave.Store(true)
	if err = ledger.Reconcile(context.Background(), id, 2_200_000, false); !errors.Is(err, errSmartContained) {
		t.Fatalf("accounting panic = %v", err)
	}
	restarted, err := getSmartLedger(&smartLedgerTestCache{data: append([]byte(nil), cache.data...)})
	if err != nil {
		t.Fatal(err)
	}
	if state := restarted.Metadata().Ledger; state.Days["wifi/2026-11-09"] != reserved || len(state.Reservations) != 0 {
		t.Fatalf("panic beyond the admission estimate lost its full reservation: %+v", state)
	}
}
