package group

import (
	"context"
	"errors"
	"os"
	"testing"
	"testing/synctest"
	"time"
)

func TestSmartLedgerAcquireSurvivesLastCleanupBetweenLookupAndRegistration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cache := new(smartLedgerBlockingCache)
		ledger, err := acquireSmartLedger(cache, "old", map[string]float64{"wifi": 100})
		if err != nil {
			t.Fatal(err)
		}
		now := time.Date(2026, 11, 9, 23, 0, 0, 0, time.UTC)
		gate := cache.block(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		admission := make(chan error, 1)
		go func() {
			_, _, reserveErr := ledger.Reserve(ctx, "wifi", now, 0)
			admission <- reserveErr
		}()
		smartLedgerWait(t, gate.entered)
		type acquisition struct {
			ledger *smartLedger
			err    error
		}
		responsive := make(chan acquisition, 1)
		go func() {
			shared, acquireErr := acquireSmartLedger(cache, "temporary", map[string]float64{"wifi": 100})
			responsive <- acquisition{ledger: shared, err: acquireErr}
		}()
		if shared := smartLedgerWait(t, responsive); shared.err != nil || shared.ledger != ledger {
			t.Fatalf("acquisition blocked or lost owner during metadata I/O: %p, %v", shared.ledger, shared.err)
		}
		smartLedgerCheckResponsive(t, ledger, now)
		ledger.Unregister("temporary")
		ledger.Unregister("old")
		cancel()
		if err = smartLedgerWait(t, admission); !errors.Is(err, context.Canceled) {
			t.Fatalf("admission cancellation = %v", err)
		}
		cleanup := cache.block(t)
		gate.unblock()
		smartLedgerWait(t, cleanup.entered)

		// Hold the completed readiness handoff to stop acquisition after lookup, before registration.
		lookup := &smartLedgerWriteGate{release: make(chan struct{})}
		t.Cleanup(lookup.unblock)
		ledger.ready = lookup.release
		acquired := make(chan acquisition, 1)
		go func() {
			replacement, acquireErr := acquireSmartLedger(cache, "replacement", map[string]float64{"wifi": 100})
			acquired <- acquisition{ledger: replacement, err: acquireErr}
		}()
		synctest.Wait()
		cleanup.unblock()
		synctest.Wait()
		if metadata := ledger.Metadata(); len(metadata.Ledger.Reservations) != 0 || len(metadata.Ledger.Days) != 0 {
			t.Fatalf("old group's cleanup did not finish: %+v", metadata.Ledger)
		}
		lookup.unblock()
		result := smartLedgerWait(t, acquired)
		if result.err != nil {
			t.Fatal(result.err)
		}
		replacement := result.ledger
		defer replacement.Unregister("replacement")
		other, err := acquireSmartLedger(cache, "other", map[string]float64{"wifi": 50})
		if err != nil {
			t.Fatal(err)
		}
		defer other.Unregister("other")
		if replacement != other {
			t.Fatalf("replacement and new group acquired independent owners: %p != %p", replacement, other)
		}
		id, reserved, err := replacement.Reserve(context.Background(), "wifi", now.Add(time.Minute), 0)
		if err != nil || reserved != 10_000_000 {
			t.Fatalf("shared minimum reservation = %d, %v", reserved, err)
		}
		if _, _, err = other.Reserve(context.Background(), "wifi", now.Add(time.Minute), 0); !errors.Is(err, errSmartTestBusy) {
			t.Fatalf("second group independently admitted a test: %v", err)
		}
		if err = replacement.UpdatePriors("replacement", []smartPrior{{Network: "wifi", ServerIdentity: "a", S: 1, W: 2}}, nil); err != nil {
			t.Fatal(err)
		}
		if err = other.UpdatePriors("other", []smartPrior{{Network: "wifi", ServerIdentity: "b", S: 2, W: 3}}, nil); err != nil {
			t.Fatal(err)
		}
		persisted := cache.metadata(t)
		if len(persisted.Ledger.Reservations) != 1 || persisted.Ledger.Reservations[id].Bytes != reserved || len(persisted.Priors["replacement"]) != 1 || len(persisted.Priors["other"]) != 1 {
			t.Fatalf("groups overwrote each other's metadata: %+v", persisted)
		}
		if err = replacement.Reconcile(context.Background(), id, 1_250_000, false); err != nil {
			t.Fatal(err)
		}
		if snapshot := other.Snapshot("wifi", now, 0); snapshot.UsedMB != 1.25 || snapshot.ReservedMB != 0 {
			t.Fatalf("new group lost shared reconciliation: %+v", snapshot)
		}
	})
}

type smartLedgerLifetimeCache struct {
	*smartLedgerBlockingCache
	done chan struct{}
}

func (c *smartLedgerLifetimeCache) SmartCacheDone() <-chan struct{} {
	return c.done
}

func TestSmartLedgerCacheRetirementDropsFailedReconciliationOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cache := &smartLedgerLifetimeCache{smartLedgerBlockingCache: new(smartLedgerBlockingCache), done: make(chan struct{})}
		defer func() {
			select {
			case <-cache.done:
			default:
				close(cache.done)
			}
		}()
		ledger, err := acquireSmartLedger(cache, "old", map[string]float64{"wifi": 100})
		if err != nil {
			t.Fatal(err)
		}
		now := time.Date(2026, 11, 9, 23, 0, 0, 0, time.UTC)
		id, reserved, err := ledger.Reserve(context.Background(), "wifi", now, 0)
		if err != nil {
			t.Fatal(err)
		}
		gate := cache.block(t)
		gate.err = os.ErrClosed
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		reconciled := make(chan error, 1)
		go func() { reconciled <- ledger.Reconcile(ctx, id, 750_000, false) }()
		smartLedgerWait(t, gate.entered)
		if err = smartLedgerWait(t, reconciled); !errors.Is(err, context.Canceled) {
			t.Fatalf("shutdown reconciliation cancellation = %v", err)
		}
		ledger.Unregister("old")
		close(cache.done)
		synctest.Wait()
		smartLedgers.Lock()
		retained := smartLedgers.byCache[cache]
		smartLedgers.Unlock()
		if retained != nil {
			t.Fatal("closed cache remains registered while reconciliation I/O is blocked")
		}
		gate.unblock()
		synctest.Wait()
		if metadata := ledger.Metadata(); metadata.Ledger.Reservations[id].Bytes != reserved {
			t.Fatalf("failed shutdown reconciliation lost its durable orphan: %+v", metadata.Ledger)
		}
		smartLedgers.Lock()
		retained = smartLedgers.byCache[cache]
		smartLedgers.Unlock()
		if retained != nil {
			t.Fatal("failed reconciliation restored the closed cache registry entry")
		}
		if closed, acquireErr := acquireSmartLedger(cache, "closed", map[string]float64{"wifi": 100}); !errors.Is(acquireErr, os.ErrClosed) || closed != nil {
			t.Fatalf("closed cache reacquisition = %p, %v", closed, acquireErr)
		}
		fresh := &smartLedgerLifetimeCache{smartLedgerBlockingCache: cache.smartLedgerBlockingCache, done: make(chan struct{})}
		defer close(fresh.done)
		recovered, err := acquireSmartLedger(fresh, "new", map[string]float64{"wifi": 100})
		if err != nil {
			t.Fatal(err)
		}
		defer recovered.Unregister("new")
		if recovered == ledger || recovered.Metadata().InstallID != ledger.Metadata().InstallID {
			t.Fatal("fresh cache did not recover the installation through a new owner")
		}
		if snapshot := recovered.Snapshot("wifi", now, 0); snapshot.UsedMB != 20 || snapshot.ReservedMB != 0 {
			t.Fatalf("fresh cache did not charge the durable orphan in full: %+v", snapshot)
		}
		if persisted := fresh.metadata(t); len(persisted.Ledger.Reservations) != 0 || persisted.Ledger.Days["wifi/2026-11-09"] != 20_000_000 {
			t.Fatalf("fresh cache orphan recovery was not durable: %+v", persisted.Ledger)
		}
	})
}

func TestSmartLedgerPruneStopsCacheRetirementWatcher(t *testing.T) {
	cache := &smartLedgerLifetimeCache{smartLedgerBlockingCache: new(smartLedgerBlockingCache), done: make(chan struct{})}
	defer close(cache.done)
	for range 10 {
		ledger, err := acquireSmartLedger(cache, "auto", map[string]float64{"wifi": 100})
		if err != nil {
			t.Fatal(err)
		}
		ledger.Unregister("auto")
		smartLedgerWait(t, ledger.retireDone)
		select {
		case <-cache.done:
			t.Fatal("watcher stopped only because cache closed")
		default:
		}
		smartLedgers.Lock()
		retained := smartLedgers.byCache[cache]
		smartLedgers.Unlock()
		if retained != nil {
			t.Fatal("pruned ledger remains registered")
		}
	}
}
