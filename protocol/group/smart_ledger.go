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
	"slices"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"
)

var (
	errSmartTestsDisabled = errors.New("smart active tests disabled")
	errSmartBudget        = errors.New("smart daily budget exhausted")
	errSmartTestBusy      = errors.New("smart throughput test already running")
	errSmartTestRate      = errors.New("smart throughput hourly limit reached")
)

type smartBudget struct {
	NetworkType string  `json:"network_type"`
	Day         string  `json:"day"`
	UsedMB      float64 `json:"used_mb"`
	ReservedMB  float64 `json:"reserved_mb"`
	LimitMB     float64 `json:"limit_mb"`
	ActiveTests bool    `json:"active_tests"`
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
	Days         map[string]int64            `json:"days"`
	Latest       map[string]string           `json:"latest"`
	Reservations map[string]smartReservation `json:"reservations"`
	Starts       []int64                     `json:"starts"`
	LatestTime   int64                       `json:"latest_time"`
}

type smartReservation struct {
	NetworkType string `json:"network_type"`
	Day         string `json:"day"`
	Bytes       int64  `json:"bytes"`
}

type smartLedger struct {
	mu           sync.Mutex
	cache        adapter.CacheFile
	installation smartInstallation
	state        smartLedgerState
	raw          map[string]json.RawMessage
	limits       map[string]map[string]float64
}

var smartLedgers = struct {
	sync.Mutex
	byCache map[adapter.CacheFile]*smartLedger
}{byCache: make(map[adapter.CacheFile]*smartLedger)}

func getSmartLedger(cache adapter.CacheFile) (*smartLedger, error) {
	smartLedgers.Lock()
	defer smartLedgers.Unlock()
	if ledger := smartLedgers.byCache[cache]; ledger != nil {
		return ledger, nil
	}
	ledger := &smartLedger{cache: cache, limits: make(map[string]map[string]float64), raw: make(map[string]json.RawMessage)}
	if cache != nil {
		metadata, err := cache.LoadSmartMetadata()
		if err != nil {
			return nil, err
		}
		if metadata != nil && len(metadata.Data) != 0 {
			if err = json.Unmarshal(metadata.Data, &ledger.raw); err != nil {
				return nil, fmt.Errorf("decode smart metadata: %w", err)
			}
			if err = json.Unmarshal(metadata.Data, &ledger.installation); err != nil {
				return nil, fmt.Errorf("decode smart installation: %w", err)
			}
		}
	}
	if ledger.installation.InstallID == "" {
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			return nil, err
		}
		ledger.installation.InstallID = hex.EncodeToString(id[:])
	}
	for _, seed := range []*[]byte{&ledger.installation.HMACSeed, &ledger.installation.DirectSeed} {
		if len(*seed) == 0 {
			*seed = make([]byte, 32)
			if _, err := rand.Read(*seed); err != nil {
				return nil, err
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
			return nil, errors.New("negative smart reservation")
		}
		ledger.state.Days[reservation.NetworkType+"/"+reservation.Day] += reservation.Bytes
	}
	ledger.state.Reservations = make(map[string]smartReservation)
	if err := ledger.saveLocked(); err != nil {
		return nil, err
	}
	smartLedgers.byCache[cache] = ledger
	return ledger, nil
}

func (l *smartLedger) Register(group string, limits map[string]float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.limits[group] = maps.Clone(limits)
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
	if latest := l.state.Latest[networkType]; day < latest {
		return latest
	}
	l.state.Latest[networkType] = day
	return day
}

func (l *smartLedger) snapshotLocked(networkType, day string) smartBudget {
	limit := l.limitLocked(networkType)
	var reserved int64
	for _, reservation := range l.state.Reservations {
		if reservation.NetworkType == networkType && reservation.Day == day {
			reserved += reservation.Bytes
		}
	}
	return smartBudget{NetworkType: networkType, Day: day, UsedMB: float64(l.state.Days[networkType+"/"+day]) / 1_000_000, ReservedMB: float64(reserved) / 1_000_000, LimitMB: limit, ActiveTests: l.cache != nil && limit > 0}
}

func (l *smartLedger) Snapshot(networkType string, now time.Time, offset int) smartBudget {
	l.mu.Lock()
	defer l.mu.Unlock()
	networkType = smartBudgetType(networkType)
	return l.snapshotLocked(networkType, l.dayLocked(networkType, now, offset))
}

func (l *smartLedger) Reserve(ctx context.Context, networkType string, now time.Time, offset int) (string, int64, error) {
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
	defer l.mu.Unlock()
	if l.cache == nil {
		return "", 0, errSmartTestsDisabled
	}
	if len(l.state.Reservations) != 0 {
		return "", 0, errSmartTestBusy
	}
	networkType = smartBudgetType(networkType)
	before := l.cloneStateLocked()
	day := l.dayLocked(networkType, now, offset)
	snapshot := l.snapshotLocked(networkType, day)
	if !snapshot.ActiveTests {
		l.state = before
		return "", 0, errSmartTestsDisabled
	}
	remaining := (snapshot.LimitMB - snapshot.UsedMB - snapshot.ReservedMB) * 1_000_000
	capBytes := int64(min(25_000_000, math.Max(0, math.Floor(remaining*0.2))))
	if capBytes < 1_100_000 {
		l.state = before
		return "", 0, errSmartBudget
	}
	nowUnix := max(now.Unix(), l.state.LatestTime)
	l.state.LatestTime = nowUnix
	l.state.Starts = slices.DeleteFunc(l.state.Starts, func(at int64) bool { return at <= nowUnix-3600 })
	if len(l.state.Starts) >= 4 {
		l.state = before
		return "", 0, errSmartTestRate
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		l.state = before
		return "", 0, err
	}
	id := hex.EncodeToString(random[:])
	l.state.Reservations[id] = smartReservation{NetworkType: networkType, Day: day, Bytes: capBytes}
	l.state.Starts = append(l.state.Starts, nowUnix)
	if err := l.saveLocked(); err != nil {
		l.state = before
		return "", 0, err
	}
	return id, capBytes, nil
}

func (l *smartLedger) Reconcile(id string, bytes int64, uncertain bool) error {
	if bytes < 0 {
		return errors.New("negative smart measured bytes")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	reservation, exists := l.state.Reservations[id]
	if !exists {
		return errors.New("unknown smart reservation")
	}
	before := l.cloneStateLocked()
	delete(l.state.Reservations, id)
	if uncertain {
		reservation.NetworkType = "cellular"
	}
	key := reservation.NetworkType + "/" + reservation.Day
	l.state.Days[key] += bytes
	if l.state.Latest[reservation.NetworkType] < reservation.Day {
		l.state.Latest[reservation.NetworkType] = reservation.Day
	}
	if err := l.saveLocked(); err != nil {
		l.state = before
		return err
	}
	return nil
}

func (l *smartLedger) cloneStateLocked() smartLedgerState {
	state := l.state
	state.Days, state.Latest, state.Reservations = maps.Clone(state.Days), maps.Clone(state.Latest), maps.Clone(state.Reservations)
	state.Starts = slices.Clone(state.Starts)
	return state
}

func (l *smartLedger) saveLocked() error {
	if l.cache == nil {
		return nil
	}
	installation := l.installation
	installation.Ledger = l.state
	encoded, err := json.Marshal(installation)
	if err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(encoded, &fields); err != nil {
		return err
	}
	merged := maps.Clone(l.raw)
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
	l.raw = merged
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
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(sources) > 16 {
		return errors.New("too many smart prior sources")
	}
	oldPriors, oldSources := l.installation.Priors, l.installation.PriorSources
	l.installation.Priors = maps.Clone(oldPriors)
	l.installation.PriorSources = maps.Clone(oldSources)
	if l.installation.Priors == nil {
		l.installation.Priors = make(map[string][]smartPrior)
	}
	if l.installation.PriorSources == nil {
		l.installation.PriorSources = make(map[string][]smartPriorSource)
	}
	if len(priors) == 0 {
		delete(l.installation.Priors, group)
	} else {
		l.installation.Priors[group] = slices.Clone(priors)
	}
	if len(sources) == 0 {
		delete(l.installation.PriorSources, group)
	} else {
		l.installation.PriorSources[group] = slices.Clone(sources)
	}
	if err := l.saveLocked(); err != nil {
		l.installation.Priors, l.installation.PriorSources = oldPriors, oldSources
		return err
	}
	return nil
}
