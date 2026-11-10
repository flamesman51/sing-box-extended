package group

import (
	"context"
	"encoding/json"
	"errors"
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

func TestSmartDegradationLedgerAllowanceDoesNotBlockOrdinary(t *testing.T) {
	ledger, _ := smartDegradationTestLedger(t, map[string]float64{"cellular": 50})
	now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	if _, _, err := ledger.ReserveDegradation(context.Background(), "cellular", now, 0, smartDegradationConfirmation, 10_000_000); !errors.Is(err, errSmartDegradationBudget) || errors.Is(err, errSmartBudget) {
		t.Fatalf("confirmation allowance denial = %v", err)
	}
	ledger.mu.Lock()
	ledger.state.Days["cellular/2026-11-09"] = 5_000_000
	ledger.state.Days[smartDegradationDayKey("cellular", "2026-11-09")] = 5_000_000
	ledger.mu.Unlock()
	if _, _, err := ledger.ReserveDegradation(context.Background(), "cellular", now, 0, smartDegradationChallenger, 10_000_000); !errors.Is(err, errSmartDegradationBudget) {
		t.Fatalf("exhausted challenger allowance = %v", err)
	}
	id, capBytes, err := ledger.Reserve(context.Background(), "cellular", now, 0)
	if err != nil || capBytes != 9_000_000 {
		t.Fatalf("ordinary admission after purpose exhaustion = %d, %v", capBytes, err)
	}
	if got := ledger.Metadata().Ledger.Reservations[id].Purpose; got != "" {
		t.Fatalf("ordinary reservation purpose = %q", got)
	}
	if err = ledger.Reconcile(context.Background(), id, 1_000_000, false); err != nil {
		t.Fatal(err)
	}
	budget := ledger.Snapshot("cellular", now, 0)
	if budget.UsedMB != 6 || budget.Degradation.UsedMB != 5 || budget.Degradation.LimitMB != 5 {
		t.Fatalf("ordinary bytes consumed purpose allowance: %+v", budget)
	}
}

func TestSmartDegradationLedgerRunwayArithmetic(t *testing.T) {
	if got := smartDegradationWorstCase(10_000_000); got != 11_000_000 {
		t.Fatalf("confirmation worst case = %d", got)
	}
	if got := smartDegradationWorstCase(10_000_000 + 25_000_000); got != 38_500_000 {
		t.Fatalf("full pair worst case = %d", got)
	}
	if got := smartDegradationWorstCase(2*10_000_000 + 25_000_000); got != 49_500_000 {
		t.Fatalf("confirmation admission threshold = %d", got)
	}
	for _, test := range []struct {
		name      string
		limit     float64
		count     int
		used      float64
		remaining float64
		denyNext  bool
	}{
		{"wifi", 5000, 24, 264, 236, false},
		{"cellular", 1000, 5, 55, 45, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ledger, _ := smartDegradationTestLedger(t, map[string]float64{test.name: test.limit})
			now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
			for i := range test.count {
				at := now.Add(time.Duration(i) * time.Hour)
				id, capBytes, err := ledger.ReserveDegradation(context.Background(), test.name, at, 0, smartDegradationConfirmation, 10_000_000)
				if err != nil || capBytes != 10_000_000 {
					t.Fatalf("confirmation %d = %d, %v", i+1, capBytes, err)
				}
				if got := ledger.Snapshot(test.name, at, 0); got.ReservedMB != 11 || got.Degradation.ReservedMB != 11 {
					t.Fatalf("worst-case outstanding confirmation: %+v", got)
				}
				if err = ledger.Reconcile(context.Background(), id, 11_000_000, false); err != nil {
					t.Fatal(err)
				}
			}
			at := now.Add(time.Duration(test.count-1) * time.Hour)
			budget := ledger.Snapshot(test.name, at, 0)
			if budget.UsedMB != test.used || budget.Degradation.UsedMB != test.used || budget.Degradation.LimitMB-budget.Degradation.UsedMB != test.remaining {
				t.Fatalf("hourly confirmation runway: %+v", budget)
			}
			id, _, err := ledger.ReserveDegradation(context.Background(), test.name, at, 0, smartDegradationConfirmation, 10_000_000)
			if test.denyNext {
				if !errors.Is(err, errSmartDegradationBudget) {
					t.Fatalf("sixth cellular confirmation = %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if err = ledger.Reconcile(context.Background(), id, 0, false); err != nil {
					t.Fatal(err)
				}
			}
			id, capBytes, err := ledger.ReserveDegradation(context.Background(), test.name, at, 0, smartDegradationChallenger, 10_000_000)
			if err != nil || capBytes != 25_000_000 {
				t.Fatalf("preserved full challenger = %d, %v", capBytes, err)
			}
			if err = ledger.Reconcile(context.Background(), id, 27_500_000, false); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSmartDegradationLedgerConfirmationThresholdAndCap(t *testing.T) {
	now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name    string
		limit   float64
		used    int64
		cap     int64
		wantErr error
	}{
		{"exact_runway", 1000, 50_500_000, 10_000_000, nil},
		{"one_byte_short", 1000, 50_500_001, 10_000_000, errSmartDegradationBudget},
		{"ordinary_base_too_small", 1000, 951_000_000, 10_000_000, errSmartDegradationBudget},
		{"ordinary_exhausted", 5, 0, 1_100_000, errSmartBudget},
		{"confirmation_never_shrinks", 50, 0, 10_000_000, errSmartDegradationBudget},
		{"confirmation_over_maximum", 5000, 0, 25_000_001, errSmartDegradationBudget},
	} {
		t.Run(test.name, func(t *testing.T) {
			ledger, _ := smartDegradationTestLedger(t, map[string]float64{"wifi": test.limit})
			ledger.mu.Lock()
			if test.name == "ordinary_base_too_small" {
				ledger.state.Days["wifi/2026-11-09"] = test.used
			} else {
				ledger.state.Days["degradation/wifi/2026-11-09"] = test.used
			}
			ledger.mu.Unlock()
			id, capBytes, err := ledger.ReserveDegradation(context.Background(), "wifi", now, 0, smartDegradationConfirmation, test.cap)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("confirmation admission = %v, want %v", err, test.wantErr)
			}
			if err == nil {
				if capBytes != test.cap {
					t.Fatalf("confirmation shrank to %d", capBytes)
				}
				if err = ledger.Reconcile(context.Background(), id, 0, false); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestSmartDegradationLedgerChallengerMinimumAndSharedLimit(t *testing.T) {
	now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	ledger, _ := smartDegradationTestLedger(t, map[string]float64{"wifi": 5000})
	ledger.Register("smaller", map[string]float64{"wifi": 1000})
	defer ledger.Unregister("smaller")
	if got := ledger.Snapshot("wifi", now, 0).Degradation.LimitMB; got != 100 {
		t.Fatalf("shared effective allowance = %v", got)
	}
	ledger.mu.Lock()
	ledger.state.Days["degradation/wifi/2026-11-09"] = 89_000_000
	ledger.mu.Unlock()
	id, capBytes, err := ledger.ReserveDegradation(context.Background(), "wifi", now, 0, smartDegradationChallenger, 10_000_000)
	if err != nil || capBytes != 10_000_000 {
		t.Fatalf("target-sized minimum challenger = %d, %v", capBytes, err)
	}
	if err = ledger.Reconcile(context.Background(), id, 1, false); err != nil {
		t.Fatal(err)
	}
	if _, _, err = ledger.ReserveDegradation(context.Background(), "wifi", now, 0, smartDegradationChallenger, 10_000_000); !errors.Is(err, errSmartDegradationBudget) {
		t.Fatalf("one byte below minimum headroom = %v", err)
	}
}

func TestSmartDegradationLedgerRestartOrphansAndDayRollback(t *testing.T) {
	ledger, cache := smartDegradationTestLedger(t, map[string]float64{"wifi": 5000})
	now := time.Date(2026, 11, 9, 23, 30, 0, 0, time.UTC)
	id, capBytes, err := ledger.ReserveDegradation(context.Background(), "wifi", now, 0, smartDegradationConfirmation, 10_000_000)
	if err != nil || capBytes != 10_000_000 {
		t.Fatalf("confirmation = %d, %v", capBytes, err)
	}
	if reservation := ledger.Metadata().Ledger.Reservations[id]; reservation.Purpose != smartDegradationConfirmation {
		t.Fatalf("durable reservation purpose = %q", reservation.Purpose)
	}
	for i := range 2 {
		restartedCache := &smartLedgerTestCache{data: append([]byte(nil), cache.data...)}
		restarted, restartErr := getSmartLedger(restartedCache)
		if restartErr != nil {
			t.Fatal(restartErr)
		}
		restarted.Register("restart", map[string]float64{"wifi": 5000})
		if got := restarted.Snapshot("wifi", now.Add(-24*time.Hour), 0); got.Day != "2026-11-09" || got.UsedMB != 11 || got.Degradation.UsedMB != 11 || got.Degradation.ReservedMB != 0 {
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
	tomorrow := now.Add(2 * time.Hour)
	if got := restarted.Snapshot("wifi", tomorrow, 0); got.Day != "2026-11-10" || got.Degradation.UsedMB != 0 || got.UsedMB != 0 {
		t.Fatalf("daily purpose rollover: %+v", got)
	}
	id, _, err = restarted.ReserveDegradation(context.Background(), "wifi", now, 0, smartDegradationChallenger, 10_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if err = restarted.Reconcile(context.Background(), id, 2_750_000, false); err != nil {
		t.Fatal(err)
	}
	if got := restarted.Snapshot("wifi", now.Add(-48*time.Hour), 0); got.Day != "2026-11-10" || got.UsedMB != 2.75 || got.Degradation.UsedMB != 2.75 {
		t.Fatalf("rolled-back reservation reset purpose day: %+v", got)
	}
}

func TestSmartDegradationLedgerUncertainCellularAttribution(t *testing.T) {
	ledger, cache := smartDegradationTestLedger(t, map[string]float64{"wifi": 5000, "cellular": 50})
	now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	id, _, err := ledger.ReserveDegradation(context.Background(), "wifi", now, 0, smartDegradationChallenger, 10_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if cell := ledger.Snapshot("cellular", now, 0); cell.ReservedMB != 0 || cell.Degradation.ReservedMB != 0 {
		t.Fatalf("cellular exposure pre-reserved: %+v", cell)
	}
	cache.fail = true
	if err = ledger.Reconcile(context.Background(), id, 27_500_000, true); err == nil {
		t.Fatal("failed save accepted purpose reconciliation")
	}
	if got := ledger.Snapshot("wifi", now, 0); got.Degradation.ReservedMB != 27.5 || got.Degradation.UsedMB != 0 {
		t.Fatalf("failed save lost purpose headroom: %+v", got)
	}
	cache.fail = false
	if err = ledger.Reconcile(context.Background(), id, 27_500_000, true); err != nil {
		t.Fatal(err)
	}
	if wifi := ledger.Snapshot("wifi", now, 0); wifi.UsedMB != 0 || wifi.Degradation.UsedMB != 0 || wifi.Degradation.ReservedMB != 0 {
		t.Fatalf("uncertain transfer retained wifi charge: %+v", wifi)
	}
	if cell := ledger.Snapshot("cellular", now, 0); cell.UsedMB != 27.5 || cell.Degradation.UsedMB != 27.5 || cell.Degradation.LimitMB != 5 {
		t.Fatalf("uncertain actual charge capped at cellular allowance: %+v", cell)
	}
	restarted, err := getSmartLedger(&smartLedgerTestCache{data: append([]byte(nil), cache.data...)})
	if err != nil {
		t.Fatal(err)
	}
	restarted.Register("restart", map[string]float64{"cellular": 50})
	defer restarted.Unregister("restart")
	if cell := restarted.Snapshot("cellular", now, 0); cell.Degradation.UsedMB != 27.5 {
		t.Fatalf("restart lost reattributed purpose charge: %+v", cell)
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
	id, _, err := restarted.ReserveDegradation(ctx, "wifi", until.Add(time.Hour), 0, smartDegradationConfirmation, 10_000_000)
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
	var status struct {
		Degradation map[string]float64 `json:"degradation"`
	}
	if err = json.Unmarshal(encoded, &status); err != nil {
		t.Fatal(err)
	}
	if len(status.Degradation) != 3 || status.Degradation["used_mb"] != 0 || status.Degradation["reserved_mb"] != 0 || status.Degradation["limit_mb"] != 500 {
		t.Fatalf("additive purpose status = %s", encoded)
	}
}

func TestSmartDegradationLedgerUnknownOrphanPurpose(t *testing.T) {
	for _, test := range []struct {
		name      string
		capBytes  int64
		wantError bool
	}{
		{"unknown_purpose_charged_conservatively", 10_000_001, false},
		{"invalid_purpose_cap_rejected", 25_000_001, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := json.Marshal(smartInstallation{Ledger: smartLedgerState{
				Latest: map[string]string{"wifi": "2026-11-09"},
				Reservations: map[string]smartReservation{
					"orphan": {NetworkType: "wifi", Day: "2026-11-09", Bytes: test.capBytes, Purpose: "unknown-purpose"},
				},
			}})
			if err != nil {
				t.Fatal(err)
			}
			ledger, err := getSmartLedger(&smartLedgerTestCache{data: encoded})
			if test.wantError {
				if err == nil {
					t.Fatal("invalid orphan cap admitted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			state := ledger.Metadata().Ledger
			if state.Days["wifi/2026-11-09"] != 11_000_002 || state.Days["degradation/wifi/2026-11-09"] != 11_000_002 || len(state.Reservations) != 0 {
				t.Fatalf("unknown purpose lost worst-case charge: %+v", state)
			}
		})
	}
}

func TestSmartDegradationChallengerRequiresTargetSizedCap(t *testing.T) {
	ledger, _ := smartDegradationTestLedger(t, map[string]float64{"wifi": 1000})
	now := smartDegradationTestClock().Now()
	day := now.Format("2006-01-02")
	ledger.mu.Lock()
	ledger.state.Days[smartDegradationDayKey("wifi", day)] = 98_790_000
	ledger.mu.Unlock()
	if _, _, err := ledger.ReserveDegradation(context.Background(), "wifi", now, 0, smartDegradationChallenger, 10_000_000); !errors.Is(err, errSmartDegradationBudget) {
		t.Fatalf("unmeasurable challenger admitted: %v", err)
	}
	if state := ledger.Metadata().Ledger; len(state.Starts) != 0 || len(state.Reservations) != 0 {
		t.Fatal("rejected challenger consumed hourly attempt")
	}
}

func TestSmartDegradationConfirmationRequiresPostTestChallengerCap(t *testing.T) {
	for _, test := range []struct {
		remaining int64
		wantErr   error
	}{
		{60_000_000, errSmartDegradationBudget},
		{61_000_000, nil},
	} {
		ledger, _ := smartDegradationTestLedger(t, map[string]float64{"wifi": 1000})
		clock := smartDegradationTestClock()
		now := clock.Now()
		ledger.mu.Lock()
		ledger.state.Days["wifi/"+now.Format("2006-01-02")] = 1_000_000_000 - test.remaining
		ledger.mu.Unlock()
		id, _, err := ledger.ReserveDegradation(context.Background(), "wifi", now, 0, smartDegradationConfirmation, 10_000_000)
		if !errors.Is(err, test.wantErr) {
			t.Fatalf("remaining=%d: confirmation error=%v want=%v", test.remaining, err, test.wantErr)
		}
		if err == nil {
			if err = ledger.Reconcile(context.Background(), id, 11_000_000, false); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			id, capBytes, err := ledger.ReserveDegradation(context.Background(), "wifi", clock.Now(), 0, smartDegradationChallenger, 10_000_000)
			if err != nil || capBytes < 10_000_000 {
				t.Fatalf("accepted episode cannot measure challenger: cap=%d err=%v", capBytes, err)
			}
			if err = ledger.Reconcile(context.Background(), id, 0, false); err != nil {
				t.Fatal(err)
			}
		}
	}
}
