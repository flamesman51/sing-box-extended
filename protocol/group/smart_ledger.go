package group

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"
)

var (
	errSmartTestsDisabled     = errors.New("smart active tests disabled")
	errSmartBudget            = errors.New("smart daily budget exhausted")
	errSmartDegradationBudget = errors.New("smart degradation allowance exhausted")
	errSmartTestBusy          = errors.New("smart throughput test already running")
	errSmartTestRate          = errors.New("smart throughput hourly limit reached")
)

type smartBudget struct {
	NetworkType string                 `json:"network_type"`
	Day         string                 `json:"day"`
	DayEndsAt   time.Time              `json:"day_ends_at"`
	UsedMB      float64                `json:"used_mb"`
	ReservedMB  float64                `json:"reserved_mb"`
	LimitMB     float64                `json:"limit_mb"`
	ActiveTests bool                   `json:"active_tests"`
	Degradation smartDegradationBudget `json:"degradation"`
}

type smartDegradationBudget struct {
	UsedMB     float64 `json:"used_mb"`
	ReservedMB float64 `json:"reserved_mb"`
	LimitMB    float64 `json:"limit_mb"`
}

type smartInstallation struct {
	InstallID    string                        `json:"install_id"`
	HMACSeed     []byte                        `json:"hmac_seed"`
	DirectSeed   []byte                        `json:"direct_seed"`
	Ledger       smartLedgerState              `json:"ledger"`
	Priors       map[string][]smartPrior       `json:"priors,omitempty"`
	PriorSources map[string][]smartPriorSource `json:"prior_sources,omitempty"`
}

type smartLedgerState struct {
	Days                 map[string]int64            `json:"days"`
	Latest               map[string]string           `json:"latest"`
	Reservations         map[string]smartReservation `json:"reservations"`
	Starts               []int64                     `json:"starts"`
	LatestTime           int64                       `json:"latest_time"`
	DegradationCooldowns map[string]time.Time        `json:"degradation_cooldowns,omitempty"`
}

type smartReservation struct {
	NetworkType string `json:"network_type"`
	Day         string `json:"day"`
	Bytes       int64  `json:"bytes"`
	Purpose     string `json:"purpose,omitempty"`
}

type smartReconciliation struct {
	id        string
	bytes     int64
	uncertain bool
}

type smartReconciliationAttempt struct {
	id   string
	done chan struct{}
	err  error
}

type smartLedger struct {
	mu           sync.Mutex
	operation    chan struct{}
	operations   int
	ready        chan struct{}
	retireStop   chan struct{}
	retireDone   chan struct{}
	loadErr      error
	cache        adapter.CacheFile
	installation smartInstallation
	state        smartLedgerState
	pending      smartReconciliation
	attempt      *smartReconciliationAttempt
	raw          map[string]json.RawMessage
	limits       map[string]map[string]float64
}

var smartLedgers = struct {
	sync.Mutex
	byCache map[adapter.CacheFile]*smartLedger
}{byCache: make(map[adapter.CacheFile]*smartLedger)}

func acquireSmartLedger(cache adapter.CacheFile, group string, limits map[string]float64) (*smartLedger, error) {
	for {
		ledger, err := getSmartLedger(cache)
		if err != nil {
			return nil, err
		}
		smartLedgers.Lock()
		// Lookup alone does not pin an owner against asynchronous cleanup.
		if smartLedgers.byCache[cache] == ledger {
			ledger.Register(group, limits)
			smartLedgers.Unlock()
			return ledger, nil
		}
		smartLedgers.Unlock()
	}
}

func getSmartLedger(cache adapter.CacheFile) (*smartLedger, error) {
	var cacheDone <-chan struct{}
	if lifetime, ok := cache.(interface{ SmartCacheDone() <-chan struct{} }); ok {
		cacheDone = lifetime.SmartCacheDone()
	}
	smartLedgers.Lock()
	select {
	case <-cacheDone:
		smartLedgers.Unlock()
		return nil, os.ErrClosed
	default:
	}
	if ledger := smartLedgers.byCache[cache]; ledger != nil {
		smartLedgers.Unlock()
		<-ledger.ready
		if ledger.loadErr != nil {
			return nil, ledger.loadErr
		}
		return ledger, nil
	}
	ledger := &smartLedger{
		cache: cache, operation: make(chan struct{}, 1), ready: make(chan struct{}),
		retireStop: make(chan struct{}), retireDone: make(chan struct{}),
		limits: make(map[string]map[string]float64), raw: make(map[string]json.RawMessage),
	}
	smartLedgers.byCache[cache] = ledger
	smartLedgers.Unlock()
	if cacheDone != nil {
		go ledger.retireOnCacheClose(cacheDone)
	} else {
		close(ledger.retireDone)
	}
	ledger.loadErr = ledger.initialize()
	if ledger.loadErr != nil {
		smartLedgers.Lock()
		delete(smartLedgers.byCache, cache)
		smartLedgers.Unlock()
	}
	close(ledger.ready)
	if ledger.loadErr != nil {
		return nil, ledger.loadErr
	}
	return ledger, nil
}

func (l *smartLedger) retireOnCacheClose(done <-chan struct{}) {
	defer close(l.retireDone)
	select {
	case <-l.retireStop:
		return
	case <-done:
	case <-l.ready:
		if l.loadErr != nil {
			return
		}
		select {
		case <-done:
		case <-l.retireStop:
			return
		}
	}
	smartLedgers.Lock()
	if smartLedgers.byCache[l.cache] == l {
		delete(smartLedgers.byCache, l.cache)
	}
	smartLedgers.Unlock()
}

func (ledger *smartLedger) initialize() error {
	cache := ledger.cache
	if cache != nil {
		metadata, err := cache.LoadSmartMetadata()
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if metadata != nil && len(metadata.Data) != 0 {
			if err = json.Unmarshal(metadata.Data, &ledger.raw); err != nil {
				return fmt.Errorf("decode smart metadata: %w", err)
			}
			if err = json.Unmarshal(metadata.Data, &ledger.installation); err != nil {
				return fmt.Errorf("decode smart installation: %w", err)
			}
		}
	}
	if ledger.installation.InstallID == "" {
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			return err
		}
		ledger.installation.InstallID = hex.EncodeToString(id[:])
	}
	for _, seed := range []*[]byte{&ledger.installation.HMACSeed, &ledger.installation.DirectSeed} {
		if len(*seed) == 0 {
			*seed = make([]byte, 32)
			if _, err := rand.Read(*seed); err != nil {
				return err
			}
		}
	}
	ledger.state = ledger.installation.Ledger
	if ledger.state.Days == nil {
		ledger.state.Days = make(map[string]int64)
	}
	if ledger.state.Latest == nil {
		ledger.state.Latest = make(map[string]string)
	}
	for _, reservation := range ledger.state.Reservations {
		if reservation.Bytes < 0 {
			return errors.New("negative smart reservation")
		}
		charged := reservation.Bytes
		if reservation.Purpose != "" {
			if reservation.Bytes > 25_000_000 {
				return errors.New("invalid smart degradation reservation")
			}
			charged = smartDegradationWorstCase(charged)
			ledger.state.Days[smartDegradationDayKey(reservation.NetworkType, reservation.Day)] += charged
		}
		ledger.state.Days[reservation.NetworkType+"/"+reservation.Day] += charged
	}
	ledger.state.Reservations = make(map[string]smartReservation)
	return ledger.save(ledger.state, ledger.installation)
}

func (l *smartLedger) Register(group string, limits map[string]float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.limits[group] = maps.Clone(limits)
}

func (l *smartLedger) Unregister(group string) {
	l.mu.Lock()
	delete(l.limits, group)
	l.mu.Unlock()
	l.prune()
}

func (l *smartLedger) prune() {
	smartLedgers.Lock()
	defer smartLedgers.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cache != nil && len(l.limits) == 0 && l.operations == 0 && len(l.state.Reservations) == 0 && l.pending.id == "" && smartLedgers.byCache[l.cache] == l {
		delete(smartLedgers.byCache, l.cache)
		close(l.retireStop)
	}
}

func (l *smartLedger) releaseOperation() {
	<-l.operation
	l.finishOperation()
}

func (l *smartLedger) finishOperation() {
	l.mu.Lock()
	l.operations--
	l.mu.Unlock()
	l.prune()
}

func smartBudgetType(networkType string) string {
	switch networkType {
	case "wifi", "cellular":
		return networkType
	default:
		return "other"
	}
}

func (l *smartLedger) limitLocked(networkType string) float64 {
	limit := math.Inf(1)
	for _, group := range l.limits {
		value := group[networkType]
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			value = 0
		}
		limit = min(limit, value)
	}
	if math.IsInf(limit, 1) {
		return 0
	}
	return limit
}

func (l *smartLedger) dayLocked(networkType string, now time.Time, offset int) string {
	day := now.In(time.FixedZone("smart-local", offset)).Format("2006-01-02")
	return max(day, l.state.Latest[networkType])
}

func smartBudgetDayEnd(day string, offset int) time.Time {
	start, err := time.Parse("2006-01-02", day)
	if err != nil {
		return time.Time{}
	}
	return start.AddDate(0, 0, 1).Add(-time.Duration(offset) * time.Second)
}

func smartDegradationDayKey(networkType, day string) string {
	return "degradation/" + networkType + "/" + day
}

func smartDegradationWorstCase(capBytes int64) int64 {
	return capBytes + (capBytes+9)/10
}

func (l *smartLedger) snapshotLocked(networkType, day string) smartBudget {
	limit := l.limitLocked(networkType)
	var reserved, degradationReserved int64
	for _, reservation := range l.state.Reservations {
		if reservation.NetworkType == networkType && reservation.Day == day {
			charged := reservation.Bytes
			if reservation.Purpose != "" {
				charged = smartDegradationWorstCase(charged)
				degradationReserved += charged
			}
			reserved += charged
		}
	}
	return smartBudget{
		NetworkType: networkType, Day: day,
		UsedMB:     float64(l.state.Days[networkType+"/"+day]) / 1_000_000,
		ReservedMB: float64(reserved) / 1_000_000, LimitMB: limit, ActiveTests: l.cache != nil && limit > 0,
		Degradation: smartDegradationBudget{
			UsedMB:     float64(l.state.Days[smartDegradationDayKey(networkType, day)]) / 1_000_000,
			ReservedMB: float64(degradationReserved) / 1_000_000, LimitMB: limit * 0.1,
		},
	}
}

func (l *smartLedger) Snapshot(networkType string, now time.Time, offset int) smartBudget {
	l.mu.Lock()
	defer l.mu.Unlock()
	networkType = smartBudgetType(networkType)
	day := l.dayLocked(networkType, now, offset)
	l.state.Latest[networkType] = day
	snapshot := l.snapshotLocked(networkType, day)
	snapshot.DayEndsAt = smartBudgetDayEnd(day, offset)
	return snapshot
}

func (l *smartLedger) Reserve(ctx context.Context, networkType string, now time.Time, offset int) (string, int64, error) {
	return l.reservePurpose(ctx, networkType, now, offset, "", 0)
}

func (l *smartLedger) ReserveDegradation(ctx context.Context, networkType string, now time.Time, offset int, purpose string, confirmationCap int64) (id string, capBytes int64, err error) {
	if purpose != smartDegradationConfirmation && purpose != smartDegradationChallenger {
		return "", 0, errors.New("invalid smart degradation purpose")
	}
	return l.reservePurpose(ctx, networkType, now, offset, purpose, confirmationCap)
}

func (l *smartLedger) reservePurpose(ctx context.Context, networkType string, now time.Time, offset int, purpose string, confirmationCap int64) (string, int64, error) {
	if err := ctx.Err(); err != nil {
		return "", 0, err
	}
	if platform := service.FromContext[adapter.PlatformInterface](ctx); platform != nil && platform.IsPowerSaveMode() {
		return "", 0, errSmartTestsDisabled
	}
	if manager := service.FromContext[pause.Manager](ctx); manager != nil && manager.IsPaused() {
		return "", 0, errSmartTestsDisabled
	}
	l.mu.Lock()
	l.operations++
	l.mu.Unlock()
	select {
	case l.operation <- struct{}{}:
	case <-ctx.Done():
		l.finishOperation()
		return "", 0, ctx.Err()
	}
	type admission struct {
		id    string
		bytes int64
		err   error
	}
	result := make(chan admission)
	// Acknowledgement leaves cleanup with the worker when cancellation wins the handoff.
	accepted := make(chan bool, 1)
	go func() {
		defer l.releaseOperation()
		id, bytes, err := func() (id string, bytes int64, err error) {
			defer smartContextRecover(ctx, "ledger-reserve", &err)
			return l.reserve(ctx, networkType, now, offset, purpose, confirmationCap)
		}()
		select {
		case result <- admission{id: id, bytes: bytes, err: err}:
			if <-accepted {
				return
			}
		case <-ctx.Done():
		}
		if id != "" {
			l.mu.Lock()
			l.pending = smartReconciliation{id: id}
			l.mu.Unlock()
			func() {
				var err error
				defer smartContextRecover(ctx, "ledger-discard", &err)
				_ = l.reconcilePending()
			}()
		}
	}()
	select {
	case reservation := <-result:
		err := ctx.Err()
		accepted <- err == nil
		if err != nil {
			return "", 0, err
		}
		return reservation.id, reservation.bytes, reservation.err
	case <-ctx.Done():
		return "", 0, ctx.Err()
	}
}

func (l *smartLedger) reserve(ctx context.Context, networkType string, now time.Time, offset int, purpose string, confirmationCap int64) (string, int64, error) {
	if err := ctx.Err(); err != nil {
		return "", 0, err
	}
	if err := l.reconcilePending(); err != nil {
		return "", 0, err
	}
	if err := ctx.Err(); err != nil {
		return "", 0, err
	}
	id, capBytes, state, installation, err := func() (string, int64, smartLedgerState, smartInstallation, error) {
		l.mu.Lock()
		defer l.mu.Unlock()
		id, capBytes, state, err := l.reservationLocked(networkType, now, offset, purpose, confirmationCap)
		return id, capBytes, state, l.installation, err
	}()
	if err != nil {
		return "", 0, err
	}
	if err = l.save(state, installation); err != nil {
		return "", 0, err
	}
	l.mu.Lock()
	l.setStateLocked(state)
	l.mu.Unlock()
	return id, capBytes, nil
}

func (l *smartLedger) reservationLocked(networkType string, now time.Time, offset int, purpose string, confirmationCap int64) (string, int64, smartLedgerState, error) {
	if l.cache == nil {
		return "", 0, smartLedgerState{}, errSmartTestsDisabled
	}
	if len(l.state.Reservations) != 0 {
		return "", 0, smartLedgerState{}, errSmartTestBusy
	}
	networkType = smartBudgetType(networkType)
	day := l.dayLocked(networkType, now, offset)
	snapshot := l.snapshotLocked(networkType, day)
	if !snapshot.ActiveTests {
		return "", 0, smartLedgerState{}, errSmartTestsDisabled
	}
	remaining := (snapshot.LimitMB - snapshot.UsedMB - snapshot.ReservedMB) * 1_000_000
	capBytes := int64(min(25_000_000, math.Max(0, math.Floor(remaining*0.2))))
	if capBytes < 1_100_000 {
		return "", 0, smartLedgerState{}, errSmartBudget
	}
	if purpose != "" {
		remainingAllowance := math.Floor(l.limitLocked(networkType)*100_000) - float64(l.state.Days[smartDegradationDayKey(networkType, day)])
		allowance := int64(min(float64(smartDegradationWorstCase(3*25_000_000)), math.Max(0, remainingAllowance)))
		// A harmless confirmation must leave enough allowance for another full pair.
		if purpose == smartDegradationConfirmation {
			if confirmationCap < 1_100_000 || confirmationCap > capBytes || allowance < smartDegradationWorstCase(2*confirmationCap+capBytes) {
				return "", 0, smartLedgerState{}, errSmartDegradationBudget
			}
			capBytes = confirmationCap
		} else {
			if allowance < smartDegradationWorstCase(1_100_000) {
				return "", 0, smartLedgerState{}, errSmartDegradationBudget
			}
			capBytes = min(capBytes, allowance/11*10+allowance%11*10/11)
		}
	}
	state := l.cloneStateLocked()
	state.Latest[networkType] = day
	nowUnix := max(now.Unix(), state.LatestTime)
	state.LatestTime = nowUnix
	maps.DeleteFunc(state.DegradationCooldowns, func(_ string, until time.Time) bool { return !until.After(time.Unix(nowUnix, 0)) })
	state.Starts = slices.DeleteFunc(state.Starts, func(at int64) bool { return at <= nowUnix-3600 })
	if len(state.Starts) >= 4 {
		return "", 0, smartLedgerState{}, errSmartTestRate
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", 0, smartLedgerState{}, err
	}
	id := hex.EncodeToString(random[:])
	state.Reservations[id] = smartReservation{NetworkType: networkType, Day: day, Bytes: capBytes, Purpose: purpose}
	state.Starts = append(state.Starts, nowUnix)
	return id, capBytes, state, nil
}

func (l *smartLedger) Reconcile(ctx context.Context, id string, bytes int64, uncertain bool) error {
	if bytes < 0 {
		return errors.New("negative smart measured bytes")
	}
	l.mu.Lock()
	if _, exists := l.state.Reservations[id]; !exists {
		l.mu.Unlock()
		return errors.New("unknown smart reservation")
	}
	if l.pending.id == "" {
		l.pending = smartReconciliation{id: id, bytes: bytes, uncertain: uncertain}
	}
	attempt := l.attempt
	if attempt == nil || attempt.id != id {
		attempt = &smartReconciliationAttempt{id: id, done: make(chan struct{})}
		l.attempt = attempt
		l.operations++
		go func() {
			l.operation <- struct{}{}
			defer l.releaseOperation()
			l.mu.Lock()
			pending := l.pending.id == id
			l.mu.Unlock()
			if pending {
				attempt.err = func() (err error) {
					defer smartContextRecover(ctx, "ledger-reconcile", &err)
					if s, ok := ctx.Value(smartContainmentKey{}).(*Smart); ok {
						s.injectPanic("ledger-accounting")
					}
					return l.reconcilePending()
				}()
			}
			l.mu.Lock()
			if l.attempt == attempt {
				l.attempt = nil
			}
			l.mu.Unlock()
			close(attempt.done)
		}()
	}
	l.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-attempt.done:
		if err := ctx.Err(); err != nil {
			return err
		}
		return attempt.err
	}
}

func (l *smartLedger) reconcilePending() error {
	l.mu.Lock()
	pending := l.pending
	if pending.id == "" {
		l.mu.Unlock()
		return nil
	}
	state := l.cloneStateLocked()
	installation := l.installation
	reservation := state.Reservations[pending.id]
	delete(state.Reservations, pending.id)
	if pending.uncertain {
		reservation.NetworkType = "cellular"
	}
	if pending.bytes != 0 {
		key := reservation.NetworkType + "/" + reservation.Day
		state.Days[key] += pending.bytes
		if reservation.Purpose != "" {
			state.Days[smartDegradationDayKey(reservation.NetworkType, reservation.Day)] += pending.bytes
		}
	}
	state.Latest[reservation.NetworkType] = max(state.Latest[reservation.NetworkType], reservation.Day)
	l.mu.Unlock()
	if err := l.save(state, installation); err != nil {
		return err
	}
	l.mu.Lock()
	l.setStateLocked(state)
	l.pending = smartReconciliation{}
	l.mu.Unlock()
	return nil
}

func (l *smartLedger) setStateLocked(state smartLedgerState) {
	// Snapshots may observe a newer date while metadata is being written.
	for networkType, day := range l.state.Latest {
		state.Latest[networkType] = max(state.Latest[networkType], day)
	}
	l.state = state
}

func (l *smartLedger) cloneStateLocked() smartLedgerState {
	state := l.state
	state.Days, state.Latest, state.Reservations = maps.Clone(state.Days), maps.Clone(state.Latest), maps.Clone(state.Reservations)
	state.DegradationCooldowns = maps.Clone(state.DegradationCooldowns)
	state.Starts = slices.Clone(state.Starts)
	return state
}

func (l *smartLedger) save(state smartLedgerState, installation smartInstallation) error {
	if l.cache == nil {
		return nil
	}
	installation.Ledger = state
	encoded, err := json.Marshal(installation)
	if err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(encoded, &fields); err != nil {
		return err
	}
	l.mu.Lock()
	merged := maps.Clone(l.raw)
	l.mu.Unlock()
	if merged == nil {
		merged = make(map[string]json.RawMessage)
	}
	// Empty prior maps must replace older persisted values after a reset.
	delete(merged, "priors")
	delete(merged, "prior_sources")
	maps.Copy(merged, fields)
	encoded, err = json.Marshal(merged)
	if err != nil {
		return err
	}
	if err = l.cache.SaveSmartMetadata(&adapter.SmartMetadata{Data: encoded}); err != nil {
		return err
	}
	l.mu.Lock()
	l.raw = merged
	l.mu.Unlock()
	return nil
}

func (l *smartLedger) Metadata() smartInstallation {
	l.mu.Lock()
	defer l.mu.Unlock()
	result := l.installation
	result.HMACSeed, result.DirectSeed = slices.Clone(result.HMACSeed), slices.Clone(result.DirectSeed)
	result.Ledger = l.cloneStateLocked()
	result.Priors = make(map[string][]smartPrior, len(l.installation.Priors))
	for group, priors := range l.installation.Priors {
		result.Priors[group] = slices.Clone(priors)
	}
	result.PriorSources = make(map[string][]smartPriorSource, len(l.installation.PriorSources))
	for group, sources := range l.installation.PriorSources {
		result.PriorSources[group] = slices.Clone(sources)
	}
	return result
}

func (l *smartLedger) Installation() (id string, hmacSeed, directSeed []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.installation.InstallID, slices.Clone(l.installation.HMACSeed), slices.Clone(l.installation.DirectSeed)
}

func (l *smartLedger) UpdatePriors(group string, priors []smartPrior, sources []smartPriorSource) error {
	if len(sources) > 16 {
		return errors.New("too many smart prior sources")
	}
	l.mu.Lock()
	l.operations++
	l.mu.Unlock()
	l.operation <- struct{}{}
	defer l.releaseOperation()
	l.mu.Lock()
	installation := l.installation
	state := l.cloneStateLocked()
	installation.Priors = maps.Clone(installation.Priors)
	installation.PriorSources = maps.Clone(installation.PriorSources)
	l.mu.Unlock()
	if installation.Priors == nil {
		installation.Priors = make(map[string][]smartPrior)
	}
	if installation.PriorSources == nil {
		installation.PriorSources = make(map[string][]smartPriorSource)
	}
	if len(priors) == 0 {
		delete(installation.Priors, group)
	} else {
		installation.Priors[group] = slices.Clone(priors)
	}
	if len(sources) == 0 {
		delete(installation.PriorSources, group)
	} else {
		installation.PriorSources[group] = slices.Clone(sources)
	}
	if err := l.save(state, installation); err != nil {
		return err
	}
	l.mu.Lock()
	l.installation.Priors, l.installation.PriorSources = installation.Priors, installation.PriorSources
	l.mu.Unlock()
	return nil
}

func (l *smartLedger) DegradationCooldown(key string) time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state.DegradationCooldowns[key]
}

func (l *smartLedger) SetDegradationCooldown(ctx context.Context, key string, now, until time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	l.operations++
	l.mu.Unlock()
	select {
	case l.operation <- struct{}{}:
	case <-ctx.Done():
		l.finishOperation()
		return ctx.Err()
	}
	result := make(chan error, 1)
	go func() {
		defer l.releaseOperation()
		result <- func() (err error) {
			defer smartContextRecover(ctx, "ledger-degradation-cooldown", &err)
			state, installation := func() (smartLedgerState, smartInstallation) {
				l.mu.Lock()
				defer l.mu.Unlock()
				return l.cloneStateLocked(), l.installation
			}()
			if state.DegradationCooldowns == nil {
				state.DegradationCooldowns = make(map[string]time.Time)
			}
			now = time.Unix(max(now.Unix(), state.LatestTime), 0)
			state.LatestTime = now.Unix()
			maps.DeleteFunc(state.DegradationCooldowns, func(_ string, at time.Time) bool { return !at.After(now) })
			if until.After(now) && until.After(state.DegradationCooldowns[key]) {
				state.DegradationCooldowns[key] = until
			}
			if err = l.save(state, installation); err != nil {
				return err
			}
			l.mu.Lock()
			defer l.mu.Unlock()
			l.setStateLocked(state)
			return nil
		}()
	}()
	select {
	case err := <-result:
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
