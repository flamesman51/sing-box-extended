package group

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
)

type smartLedgerWriteGate struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	err     error
}

func (g *smartLedgerWriteGate) unblock() {
	g.once.Do(func() { close(g.release) })
}

type smartLedgerObservedContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *smartLedgerObservedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

type smartLedgerBlockingCache struct {
	adapter.CacheFile
	mu   sync.Mutex
	data []byte
	gate *smartLedgerWriteGate
}

func (c *smartLedgerBlockingCache) LoadSmartMetadata() (*adapter.SmartMetadata, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return &adapter.SmartMetadata{Data: append([]byte(nil), c.data...)}, nil
}

func (c *smartLedgerBlockingCache) SaveSmartMetadata(metadata *adapter.SmartMetadata) error {
	c.mu.Lock()
	gate := c.gate
	c.gate = nil
	c.mu.Unlock()
	if gate != nil {
		close(gate.entered)
		<-gate.release
		if gate.err != nil {
			return gate.err
		}
	}
	c.mu.Lock()
	c.data = append([]byte(nil), metadata.Data...)
	c.mu.Unlock()
	return nil
}

func (c *smartLedgerBlockingCache) block(t *testing.T) *smartLedgerWriteGate {
	t.Helper()
	gate := &smartLedgerWriteGate{entered: make(chan struct{}), release: make(chan struct{})}
	c.mu.Lock()
	c.gate = gate
	c.mu.Unlock()
	t.Cleanup(gate.unblock)
	return gate
}

func (c *smartLedgerBlockingCache) metadata(t *testing.T) smartInstallation {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	var metadata smartInstallation
	if err := json.Unmarshal(c.data, &metadata); err != nil {
		t.Fatal(err)
	}
	return metadata
}

func smartLedgerWait[T any](t *testing.T, result <-chan T) T {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-time.After(time.Second):
		t.Fatal("ledger operation blocked on metadata I/O")
		var zero T
		return zero
	}
}

func smartLedgerCheckResponsive(t *testing.T, ledger *smartLedger, now time.Time) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		ledger.Snapshot("wifi", now, 0)
		ledger.Metadata()
		ledger.Installation()
		ledger.Register("temporary", map[string]float64{"wifi": 100})
		ledger.Unregister("temporary")
		close(done)
	}()
	smartLedgerWait(t, done)
}

func TestSmartLedgerCanceledAdmissionCleansSharedReservation(t *testing.T) {
	cache := new(smartLedgerBlockingCache)
	ledger, err := getSmartLedger(cache)
	if err != nil {
		t.Fatal(err)
	}
	ledger.Register("a", map[string]float64{"wifi": 100})
	ledger.Register("b", map[string]float64{"wifi": 50})
	now := time.Date(2026, 11, 9, 23, 0, 0, 0, time.UTC)
	gate := cache.block(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admission := make(chan error, 1)
	go func() {
		id, bytes, reserveErr := ledger.Reserve(ctx, "wifi", now, 0)
		if reserveErr != nil && (id != "" || bytes != 0) {
			reserveErr = errors.New("canceled admission returned a reservation")
		}
		admission <- reserveErr
	}()
	smartLedgerWait(t, gate.entered)
	select {
	case reserveErr := <-admission:
		t.Fatalf("admission returned before reservation was durable: %v", reserveErr)
	default:
	}
	if len(cache.metadata(t).Ledger.Reservations) != 0 {
		t.Fatal("blocked reservation was already durable")
	}
	smartLedgerCheckResponsive(t, ledger, now)
	unregistered := make(chan struct{})
	go func() {
		ledger.Unregister("a")
		ledger.Unregister("b")
		close(unregistered)
	}()
	smartLedgerWait(t, unregistered)
	shared, err := getSmartLedger(cache)
	if err != nil || shared != ledger {
		t.Fatalf("blocked operation lost shared owner: %p, %v", shared, err)
	}
	shared.Register("survivor", map[string]float64{"wifi": 50})
	queuedCtx, queuedCancel := context.WithCancel(context.Background())
	defer queuedCancel()
	observed := &smartLedgerObservedContext{Context: queuedCtx, waiting: make(chan struct{})}
	queued := make(chan error, 1)
	go func() {
		_, _, reserveErr := shared.Reserve(observed, "wifi", now, 0)
		queued <- reserveErr
	}()
	smartLedgerWait(t, observed.waiting)
	queuedCancel()
	cancel()
	if err = smartLedgerWait(t, admission); !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked admission cancellation = %v", err)
	}
	if err = smartLedgerWait(t, queued); !errors.Is(err, context.Canceled) {
		t.Fatalf("queued shared admission cancellation = %v", err)
	}
	cleanup := cache.block(t)
	gate.unblock()
	smartLedgerWait(t, cleanup.entered)
	if len(cache.metadata(t).Ledger.Reservations) != 1 {
		t.Fatal("canceled admission did not persist before cleanup")
	}
	smartLedgerCheckResponsive(t, ledger, now)
	cleanup.unblock()
	recoveredCtx, recoveredCancel := context.WithTimeout(context.Background(), time.Second)
	defer recoveredCancel()
	id, reserved, err := shared.Reserve(recoveredCtx, "wifi", now.Add(time.Minute), 0)
	if err != nil {
		t.Fatalf("canceled reservation prevented recovery: %v", err)
	}
	persisted := cache.metadata(t)
	if len(persisted.Ledger.Reservations) != 1 || persisted.Ledger.Reservations[id].Bytes != reserved || len(persisted.Ledger.Days) != 0 {
		t.Fatalf("canceled admission was orphaned or charged: %+v", persisted.Ledger)
	}
	if reserved != 10_000_000 {
		t.Fatalf("shared minimum cap = %d", reserved)
	}
	if err = shared.Reconcile(context.Background(), id, 0, false); err != nil {
		t.Fatal(err)
	}
	if persisted = cache.metadata(t); len(persisted.Ledger.Reservations) != 0 || persisted.Ledger.Days["wifi/2026-11-09"] != 0 {
		t.Fatalf("canceled admission cleanup = %+v", persisted.Ledger)
	}
}

func TestSmartLedgerCanceledReconciliationRetainsMeasuredBytes(t *testing.T) {
	for _, test := range []struct {
		name            string
		alreadyCanceled bool
		saveFails       bool
	}{
		{name: "blocked"},
		{name: "already_canceled", alreadyCanceled: true},
		{name: "failed_save", saveFails: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cache := new(smartLedgerBlockingCache)
			ledger, err := getSmartLedger(cache)
			if err != nil {
				t.Fatal(err)
			}
			ledger.Register("a", map[string]float64{"wifi": 100, "cellular": 50})
			ledger.Register("b", map[string]float64{"wifi": 100, "cellular": 50})
			now := time.Date(2026, 11, 9, 23, 0, 0, 0, time.UTC)
			id, _, err := ledger.Reserve(context.Background(), "wifi", now, 0)
			if err != nil {
				t.Fatal(err)
			}
			gate := cache.block(t)
			if test.saveFails {
				gate.err = errors.New("reconciliation save failed")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if test.alreadyCanceled {
				cancel()
			}
			reconciled := make(chan error, 1)
			go func() { reconciled <- ledger.Reconcile(ctx, id, 1_250_000, true) }()
			smartLedgerWait(t, gate.entered)
			smartLedgerCheckResponsive(t, ledger, now)
			cancel()
			if err = smartLedgerWait(t, reconciled); !errors.Is(err, context.Canceled) {
				t.Fatalf("reconciliation cancellation = %v", err)
			}
			if durable := cache.metadata(t); durable.Ledger.Reservations[id].Bytes == 0 || len(durable.Ledger.Days) != 0 {
				t.Fatalf("blocked reconciliation changed durable accounting: %+v", durable.Ledger)
			}
			gate.unblock()
			recoveredCtx, recoveredCancel := context.WithTimeout(context.Background(), time.Second)
			defer recoveredCancel()
			nextID, _, err := ledger.Reserve(recoveredCtx, "wifi", now.Add(time.Minute), 0)
			if err != nil {
				t.Fatal(err)
			}
			if err = ledger.Reconcile(context.Background(), nextID, 0, false); err != nil {
				t.Fatal(err)
			}
			persisted := cache.metadata(t)
			if len(persisted.Ledger.Reservations) != 0 || persisted.Ledger.Days["cellular/2026-11-09"] != 1_250_000 || persisted.Ledger.Days["wifi/2026-11-09"] != 0 {
				t.Fatalf("canceled reconciliation lost or repeated measured accounting: %+v", persisted.Ledger)
			}
		})
	}
}

func TestSmartLedgerCanceledAdmissionRetriesFailedCleanup(t *testing.T) {
	cache := new(smartLedgerBlockingCache)
	ledger, err := getSmartLedger(cache)
	if err != nil {
		t.Fatal(err)
	}
	ledger.Register("a", map[string]float64{"wifi": 100})
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
	cancel()
	if err = smartLedgerWait(t, admission); !errors.Is(err, context.Canceled) {
		t.Fatalf("admission cancellation = %v", err)
	}
	cleanup := cache.block(t)
	cleanup.err = errors.New("cleanup save failed")
	gate.unblock()
	smartLedgerWait(t, cleanup.entered)
	cleanup.unblock()
	recoveredCtx, recoveredCancel := context.WithTimeout(context.Background(), time.Second)
	defer recoveredCancel()
	id, _, err := ledger.Reserve(recoveredCtx, "wifi", now.Add(time.Minute), 0)
	if err != nil {
		t.Fatalf("failed cancellation cleanup was not retried: %v", err)
	}
	persisted := cache.metadata(t)
	if len(persisted.Ledger.Reservations) != 1 || persisted.Ledger.Reservations[id].Bytes == 0 || len(persisted.Ledger.Days) != 0 {
		t.Fatalf("cancellation cleanup lost accounting: %+v", persisted.Ledger)
	}
	if err = ledger.Reconcile(context.Background(), id, 0, false); err != nil {
		t.Fatal(err)
	}
}

func TestSmartLedgerCanceledReconciliationPreservesPriorResetOrdering(t *testing.T) {
	cache := new(smartLedgerBlockingCache)
	ledger, err := getSmartLedger(cache)
	if err != nil {
		t.Fatal(err)
	}
	ledger.Register("a", map[string]float64{"wifi": 100})
	now := time.Date(2026, 11, 9, 23, 0, 0, 0, time.UTC)
	id, _, err := ledger.Reserve(context.Background(), "wifi", now, 0)
	if err != nil {
		t.Fatal(err)
	}
	gate := cache.block(t)
	imported := make(chan error, 1)
	go func() {
		imported <- ledger.UpdatePriors("a", []smartPrior{{Network: "wifi", ServerIdentity: "server", Source: "source", S: 1, W: 2, At: 10}}, []smartPriorSource{{ID: "source", At: 10}})
	}()
	smartLedgerWait(t, gate.entered)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reconciled := make(chan error, 1)
	go func() { reconciled <- ledger.Reconcile(ctx, id, 750_000, false) }()
	if err = smartLedgerWait(t, reconciled); !errors.Is(err, context.Canceled) {
		t.Fatalf("queued reconciliation cancellation = %v", err)
	}
	smartLedgerCheckResponsive(t, ledger, now)
	gate.unblock()
	if err = smartLedgerWait(t, imported); err != nil {
		t.Fatal(err)
	}
	reset := make(chan error, 1)
	go func() { reset <- ledger.UpdatePriors("a", nil, nil) }()
	if err = smartLedgerWait(t, reset); err != nil {
		t.Fatal(err)
	}
	recoveredCtx, recoveredCancel := context.WithTimeout(context.Background(), time.Second)
	defer recoveredCancel()
	nextID, _, err := ledger.Reserve(recoveredCtx, "wifi", now.Add(time.Minute), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = ledger.Reconcile(context.Background(), nextID, 0, false); err != nil {
		t.Fatal(err)
	}
	persisted := cache.metadata(t)
	if len(persisted.Priors) != 0 || len(persisted.PriorSources) != 0 || len(persisted.Ledger.Reservations) != 0 || persisted.Ledger.Days["wifi/2026-11-09"] != 750_000 {
		t.Fatalf("prior reset reordered or lost reconciliation: %+v", persisted)
	}
}

func TestSmartLedgerReserveSaveFailurePreservesDayAndRate(t *testing.T) {
	cache := new(smartLedgerTestCache)
	ledger, err := getSmartLedger(cache)
	if err != nil {
		t.Fatal(err)
	}
	ledger.Register("a", map[string]float64{"wifi": 100})
	now := time.Date(2026, 11, 9, 23, 0, 0, 0, time.UTC)
	id, _, err := ledger.Reserve(context.Background(), "wifi", now, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = ledger.Reconcile(context.Background(), id, 0, false); err != nil {
		t.Fatal(err)
	}
	cache.fail = true
	if _, _, err = ledger.Reserve(context.Background(), "wifi", now.Add(48*time.Hour), 0); err == nil {
		t.Fatal("failed reservation persistence admitted a test")
	}
	metadata := ledger.Metadata()
	if metadata.Ledger.Latest["wifi"] != "2026-11-09" || metadata.Ledger.LatestTime != now.Unix() || len(metadata.Ledger.Starts) != 1 || metadata.Ledger.Starts[0] != now.Unix() || len(metadata.Ledger.Reservations) != 0 {
		t.Fatalf("failed reservation changed day or hourly accounting: %+v", metadata.Ledger)
	}
	cache.fail = false
	id, _, err = ledger.Reserve(context.Background(), "wifi", now.Add(time.Minute), 0)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot := ledger.Snapshot("wifi", now, 0); snapshot.Day != "2026-11-09" || snapshot.UsedMB != 0 {
		t.Fatalf("failed reservation advanced local day: %+v", snapshot)
	}
	if err = ledger.Reconcile(context.Background(), id, 0, false); err != nil {
		t.Fatal(err)
	}
}
