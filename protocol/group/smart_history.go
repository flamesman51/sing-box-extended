package group

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/service"
)

const (
	smartHistoryMaxKeys       = 48
	smartHistoryMaxIdentities = 32
	smartHistoryMaxEvents     = 500
	smartHistoryMaxBytes      = 1 << 20
	smartHistoryHalfLife      = 72 * time.Hour
	smartHistoryShrinkage     = 6.0
)

type smartHistory struct {
	Networks           map[string]*smartHistoryNetwork            `json:"networks"`
	Stats              map[string]map[string]*smartStatistic      `json:"stats"`
	Events             []smartEvent                               `json:"events"`
	Labels             map[string]string                          `json:"labels,omitempty"`
	Timeline           map[string]map[string][]smartTimelinePoint `json:"timeline,omitempty"`
	IdentityServers    map[string]string                          `json:"identity_servers,omitempty"`
	recency            map[string]uint64
	sequence           uint64
	outageObservations map[string]smartOutageObservation
}

type smartHistoryNetwork struct {
	Type     string `json:"type"`
	Label    string `json:"label,omitempty"`
	Carrier  string `json:"carrier,omitempty"`
	Radio    string `json:"radio,omitempty"`
	LastSeen int64  `json:"last_seen"`
	Parent   string `json:"parent,omitempty"`
}

type smartTimelinePoint struct {
	Time      int64   `json:"time"`
	Success   float64 `json:"success"`
	Weight    float64 `json:"weight"`
	RTT       float64 `json:"rtt"`
	RTTWeight float64 `json:"rtt_weight"`
	MBPS      float64 `json:"mbps"`
}

type smartStatistic struct {
	S                       float64       `json:"S"`
	W                       float64       `json:"W"`
	UpdatedAt               int64         `json:"updated_at"`
	LastResult              int64         `json:"last_result"`
	RTTs                    []float64     `json:"rtts,omitempty"`
	Download                float64       `json:"download_mbps,omitempty"`
	Upload                  float64       `json:"upload_mbps,omitempty"`
	ThroughputAt            int64         `json:"throughput_at,omitempty"`
	Hours                   [6][2]float64 `json:"hours"`
	HourUpdatedAt           [6]int64      `json:"hour_updated_at"`
	Flaps                   []int64       `json:"flaps,omitempty"`
	HasResult               bool          `json:"has_result,omitempty"`
	LastSuccess             bool          `json:"last_success,omitempty"`
	OutageEpisodes          float64       `json:"outage_episodes,omitempty"`
	OutageExposureHours     float64       `json:"outage_exposure_hours,omitempty"`
	OutageObservedHours     float64       `json:"outage_observed_hours,omitempty"`
	OutageOpen              bool          `json:"outage_open,omitempty"`
	OutageRecoverySuccesses int           `json:"outage_recovery_successes,omitempty"`
	OutageRecoveryAtNS      int64         `json:"outage_recovery_at_ns,omitempty"`
	OutageObservedAtNS      int64         `json:"outage_observed_at_ns,omitempty"`
	pauseAtResult           time.Duration
}

type smartEvent struct {
	Time    int64   `json:"time"`
	Kind    string  `json:"kind"`
	From    *string `json:"from"`
	To      *string `json:"to"`
	Network *string `json:"network"`
	Reason  string  `json:"reason"`
}

type smartPrior struct {
	Network             string  `json:"network"`
	ServerIdentity      string  `json:"server_identity"`
	Source              string  `json:"source"`
	S                   float64 `json:"S"`
	W                   float64 `json:"W"`
	At                  int64   `json:"at"`
	OutageEpisodes      float64 `json:"outage_episodes,omitempty"`
	OutageExposureHours float64 `json:"outage_exposure_hours,omitempty"`
	OutageObservedHours float64 `json:"outage_observed_hours,omitempty"`
}

type smartPriorSource struct {
	ID string `json:"id"`
	At int64  `json:"at"`
}

type smartExportStatistic struct {
	Network             string        `json:"network"`
	ServerIdentity      string        `json:"server_identity"`
	S                   float64       `json:"S"`
	W                   float64       `json:"W"`
	RTTP50              *float64      `json:"rtt_p50_ms"`
	RTTP90              *float64      `json:"rtt_p90_ms"`
	Download            *float64      `json:"download_mbps"`
	Upload              *float64      `json:"upload_mbps"`
	ThroughputAt        *int64        `json:"throughput_at"`
	Hours               [6][2]float64 `json:"hours"`
	OutageEpisodes      float64       `json:"outage_episodes,omitempty"`
	OutageExposureHours float64       `json:"outage_exposure_hours,omitempty"`
	OutageObservedHours float64       `json:"outage_observed_hours,omitempty"`
}

func newSmartHistory() *smartHistory {
	return &smartHistory{
		Networks:        make(map[string]*smartHistoryNetwork),
		Stats:           make(map[string]map[string]*smartStatistic),
		Events:          make([]smartEvent, 0),
		Labels:          make(map[string]string),
		Timeline:        make(map[string]map[string][]smartTimelinePoint),
		IdentityServers: make(map[string]string),
		recency:         make(map[string]uint64),
	}
}

func (h *smartHistory) Reset() {
	*h = *newSmartHistory()
}

func smartDecay(at int64, now time.Time) float64 {
	if now.Unix() <= at {
		return 1
	}
	return math.Exp2(-(float64(now.Unix()) - float64(at)) / smartHistoryHalfLife.Seconds())
}

func (h *smartHistory) touch(key string, now time.Time) {
	if h.Networks == nil {
		h.Networks = make(map[string]*smartHistoryNetwork)
	}
	if h.Stats == nil {
		h.Stats = make(map[string]map[string]*smartStatistic)
	}
	if h.recency == nil {
		h.recency = make(map[string]uint64)
	}
	if h.Networks[key] == nil {
		for len(h.Networks) >= smartHistoryMaxKeys {
			h.evictOldestNetwork()
		}
		networkType, _, _ := strings.Cut(key, "/")
		switch networkType {
		case "cell":
			networkType = "cellular"
		case "eth":
			networkType = "ethernet"
		}
		h.Networks[key] = &smartHistoryNetwork{Type: networkType, LastSeen: now.Unix()}
	}
	h.sequence++
	h.recency[key] = h.sequence
}

func (h *smartHistory) evictOldestNetwork() bool {
	var oldest string
	var oldestSeen int64
	var oldestUse uint64
	found := false
	for key, network := range h.Networks {
		seen := int64(0)
		if network != nil {
			seen = network.LastSeen
		}
		use := h.recency[key]
		if !found || use < oldestUse || use == oldestUse && (seen < oldestSeen || seen == oldestSeen && key < oldest) {
			oldest, oldestSeen, oldestUse, found = key, seen, use, true
		}
	}
	if !found {
		return false
	}
	delete(h.Networks, oldest)
	evicted := h.Stats[oldest]
	delete(h.Stats, oldest)
	for identity := range evicted {
		h.removeUnusedIdentity(identity)
	}
	delete(h.Timeline, oldest)
	delete(h.Labels, oldest)
	delete(h.recency, oldest)
	return true
}

func (h *smartHistory) statistic(key, identity string, now time.Time) *smartStatistic {
	h.touch(key, now)
	statistics := h.Stats[key]
	if statistics == nil {
		statistics = make(map[string]*smartStatistic)
		h.Stats[key] = statistics
	}
	statistic := statistics[identity]
	if statistic == nil {
		for len(statistics) >= smartHistoryMaxIdentities {
			evicted := smartEvictIdentity(statistics)
			delete(h.Timeline[key], evicted)
			h.removeUnusedIdentity(evicted)
		}
		statistic = &smartStatistic{UpdatedAt: now.Unix()}
		statistics[identity] = statistic
	}
	decay := smartDecay(statistic.UpdatedAt, now)
	statistic.S *= decay
	statistic.W *= decay
	statistic.OutageEpisodes *= decay
	statistic.OutageExposureHours *= decay
	statistic.OutageObservedHours *= decay
	statistic.UpdatedAt = max(statistic.UpdatedAt, now.Unix())
	cutoff := now.Add(-24 * time.Hour).Unix()
	statistic.Flaps = slices.DeleteFunc(statistic.Flaps, func(at int64) bool { return at < cutoff })
	return statistic
}

func smartEvictIdentity(statistics map[string]*smartStatistic) string {
	var oldest string
	var oldestAt int64
	found := false
	for identity, statistic := range statistics {
		at := int64(0)
		if statistic != nil {
			at = statistic.LastResult
		}
		if !found || at < oldestAt || at == oldestAt && identity < oldest {
			oldest, oldestAt, found = identity, at, true
		}
	}
	delete(statistics, oldest)
	return oldest
}

func (h *smartHistory) removeUnusedIdentity(identity string) {
	for _, statistics := range h.Stats {
		if statistics[identity] != nil {
			return
		}
	}
	delete(h.IdentityServers, identity)
	delete(h.outageObservations, identity)
}

// Observe receives the group's cumulative paused time; each key keeps its own baseline.
func (h *smartHistory) Observe(path []string, identity string, success bool, rtt float64, now time.Time, pausedTotal time.Duration) {
	for index, key := range path {
		if key == "" || slices.Contains(path[:index], key) {
			continue
		}
		statistic := h.statistic(key, identity, now)
		h.Networks[key].LastSeen = max(h.Networks[key].LastSeen, now.Unix())
		weight := 1.0
		if statistic.HasResult || statistic.LastResult != 0 {
			paused := max(pausedTotal-statistic.pauseAtResult, 0)
			elapsed := now.Sub(time.Unix(statistic.LastResult, 0)) - paused
			weight = min(max(elapsed.Seconds(), 0), 300) / 300
			if statistic.LastSuccess != success {
				statistic.Flaps = append(statistic.Flaps, now.Unix())
			}
		}
		statistic.W += weight
		if success {
			statistic.S += weight
			if smartFinite(rtt) && rtt > 0 {
				if len(statistic.RTTs) == 64 {
					copy(statistic.RTTs, statistic.RTTs[1:])
					statistic.RTTs = statistic.RTTs[:63]
				}
				statistic.RTTs = append(statistic.RTTs, rtt)
			}
		}
		statistic.LastResult = max(statistic.LastResult, now.Unix())
		statistic.HasResult = true
		statistic.LastSuccess = success
		statistic.pauseAtResult = pausedTotal
		h.observeTimeline(key, identity, success, rtt, weight, now)
		smartObserveHour(statistic, success, weight, now)
	}
}

func (h *smartHistory) Estimate(path []string, identity, serverIdentity string, now time.Time, priors []smartPrior) (probability, weight float64) {
	probability = 0.9
	for index, key := range slices.Backward(path) {
		if key == "" || slices.Contains(path[index+1:], key) {
			continue
		}
		statistic := h.Stats[key][identity]
		successes, total, localWeight := 0.0, 0.0, 0.0
		if statistic != nil {
			decay := smartDecay(statistic.UpdatedAt, now)
			successes, total = statistic.S*decay, statistic.W*decay
			localWeight = total
		}
		for _, prior := range priors {
			if prior.Network == key && prior.ServerIdentity == serverIdentity && smartValidSums(prior.S, prior.W) {
				decay := smartDecay(prior.At, now)
				successes += prior.S * decay
				total += prior.W * decay
			}
		}
		probability = (successes + smartHistoryShrinkage*probability) / (total + smartHistoryShrinkage)
		weight = localWeight
	}
	return
}

func smartStabilityClass(probability, weight float64) int {
	class := 3
	switch {
	case probability >= 0.98:
		class = 0
	case probability >= 0.93:
		class = 1
	case probability >= 0.80:
		class = 2
	}
	if weight < 12 && class < 2 {
		class++
	}
	return class
}

func (h *smartHistory) Class(path []string, identity, serverIdentity string, now time.Time, priors []smartPrior) int {
	probability, weight := h.Estimate(path, identity, serverIdentity, now, priors)
	class := smartStabilityClass(probability, weight)
	if len(path) > 0 && h.Stats[path[0]][identity] != nil {
		statistic := h.Stats[path[0]][identity]
		bucket := now.Hour() / 4
		decay := smartDecay(statistic.HourUpdatedAt[bucket], now)
		class = smartHourClass(class, probability, statistic.Hours[bucket][0]*decay, statistic.Hours[bucket][1]*decay)
	}
	return class
}

func (h *smartHistory) AddEvent(event smartEvent) {
	if len(h.Events) >= smartHistoryMaxEvents {
		h.Events = slices.Delete(h.Events, 0, len(h.Events)-smartHistoryMaxEvents+1)
	}
	h.Events = append(h.Events, event)
}

func (h *smartHistory) Encode() ([]byte, error) {
	h.limit()
	for {
		data, err := json.Marshal(h)
		if err != nil || len(data) <= smartHistoryMaxBytes {
			return data, err
		}
		if len(h.Events) > 0 {
			excess, removed, count := len(data)-smartHistoryMaxBytes, 0, 0
			for _, event := range h.Events {
				encoded, _ := json.Marshal(event)
				removed += len(encoded) + 1
				count++
				if removed >= excess {
					break
				}
			}
			h.Events = slices.Delete(h.Events, 0, count)
		} else if !h.evictOldestNetwork() {
			return nil, errors.New("smart history exceeds 1 MiB")
		}
	}
}

func (h *smartHistory) limit() {
	if h.Networks == nil {
		h.Networks = make(map[string]*smartHistoryNetwork)
	}
	if h.Stats == nil {
		h.Stats = make(map[string]map[string]*smartStatistic)
	}
	if h.Labels == nil {
		h.Labels = make(map[string]string)
	}
	if h.Timeline == nil {
		h.Timeline = make(map[string]map[string][]smartTimelinePoint)
	}
	if h.IdentityServers == nil {
		h.IdentityServers = make(map[string]string)
	}
	for key, statistics := range h.Stats {
		if h.Networks[key] == nil {
			h.Networks[key] = &smartHistoryNetwork{}
		}
		for len(statistics) > smartHistoryMaxIdentities {
			smartEvictIdentity(statistics)
		}
	}
	for len(h.Networks) > smartHistoryMaxKeys {
		h.evictOldestNetwork()
	}
	for key := range h.Labels {
		if h.Networks[key] == nil {
			delete(h.Labels, key)
		}
	}
	for key, members := range h.Timeline {
		if h.Networks[key] == nil {
			delete(h.Timeline, key)
			continue
		}
		for identity, points := range members {
			if h.Stats[key][identity] == nil {
				delete(members, identity)
			} else if len(points) > 96 {
				members[identity] = slices.Clone(points[len(points)-96:])
			}
		}
	}
	for identity := range h.IdentityServers {
		h.removeUnusedIdentity(identity)
	}
	if len(h.Events) > smartHistoryMaxEvents {
		h.Events = slices.Delete(h.Events, 0, len(h.Events)-smartHistoryMaxEvents)
	}
	if h.Events == nil {
		h.Events = make([]smartEvent, 0)
	}
}

func decodeSmartHistory(data []byte) (*smartHistory, error) {
	if len(data) > smartHistoryMaxBytes {
		return nil, errors.New("smart history exceeds 1 MiB")
	}
	var history *smartHistory
	if err := json.Unmarshal(data, &history); err != nil {
		return nil, err
	}
	if history == nil {
		return nil, errors.New("invalid smart history")
	}
	for _, statistics := range history.Stats {
		for _, statistic := range statistics {
			if statistic == nil || !smartValidSums(statistic.S, statistic.W) || !smartNonnegative(statistic.Download) || !smartNonnegative(statistic.Upload) || !smartNonnegative(statistic.OutageEpisodes) || !smartNonnegative(statistic.OutageExposureHours) || !smartNonnegative(statistic.OutageObservedHours) {
				return nil, errors.New("invalid smart statistic")
			}
			if statistic.OutageRecoverySuccesses < 0 || statistic.OutageRecoverySuccesses > 2 || statistic.OutageRecoveryAtNS < 0 || (statistic.OutageRecoverySuccesses == 0) != (statistic.OutageRecoveryAtNS == 0) || statistic.OutageRecoverySuccesses > 0 && !statistic.OutageOpen {
				return nil, errors.New("invalid smart outage recovery")
			}
			if statistic.OutageObservedAtNS < 0 {
				return nil, errors.New("invalid smart outage observation time")
			}
			for _, rtt := range statistic.RTTs {
				if !smartNonnegative(rtt) {
					return nil, errors.New("invalid smart RTT")
				}
			}
			for _, bucket := range statistic.Hours {
				if !smartValidSums(bucket[0], bucket[1]) {
					return nil, errors.New("invalid smart hour bucket")
				}
			}
			if len(statistic.RTTs) > 64 {
				statistic.RTTs = slices.Clone(statistic.RTTs[len(statistic.RTTs)-64:])
			}
		}
	}
	for _, members := range history.Timeline {
		for _, points := range members {
			for _, point := range points {
				if !smartValidSums(point.Success, point.Weight) || !smartNonnegative(point.RTT) || !smartNonnegative(point.RTTWeight) || !smartNonnegative(point.MBPS) {
					return nil, errors.New("invalid smart timeline point")
				}
			}
		}
	}
	history.limit()
	return history, nil
}

func smartFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func smartNonnegative(value float64) bool {
	return smartFinite(value) && value >= 0
}

func smartValidSums(successes, weight float64) bool {
	return smartNonnegative(successes) && smartNonnegative(weight) && successes <= weight
}

func smartHash(seed []byte, value string) string {
	mac := hmac.New(sha256.New, seed)
	mac.Write([]byte(value))
	return hex.EncodeToString(mac.Sum(nil)[:8])
}

// Paths run from the most specific key to the root; a Wi-Fi SSID parent must be supplied by the network snapshot.
func smartNetworkPath(key string) []string {
	parts := strings.Split(key, "/")
	switch parts[0] {
	case "wifi":
		if len(parts) > 1 {
			return []string{key, "wifi"}
		}
	case "cell":
		path := make([]string, 0, len(parts))
		for length := len(parts); length > 0; length-- {
			path = append(path, strings.Join(parts[:length], "/"))
		}
		return path
	}
	return []string{key}
}

func smartImportNetworkAllowed(key string, sameInstall bool) bool {
	parts := strings.Split(key, "/")
	switch parts[0] {
	case "wifi":
		return len(parts) == 1 || sameInstall && len(parts) == 2 && (smartHashedPart(parts[1], "bssid:") || smartHashedPart(parts[1], "ssid:"))
	case "cell":
		if len(parts) > 4 {
			return false
		}
		if len(parts) >= 2 && (len(parts[1]) < 5 || len(parts[1]) > 6 || strings.Trim(parts[1], "0123456789") != "") {
			return false
		}
		if len(parts) >= 3 && (parts[2] == "" || strings.Contains(parts[2], ":")) {
			return false
		}
		return len(parts) < 4 || sameInstall && smartHashedPart(parts[3], "tower:")
	case "eth":
		return len(parts) == 2 && parts[1] != ""
	case "other":
		return len(parts) == 1
	default:
		return false
	}
}

func smartHashedPart(value, prefix string) bool {
	if !strings.HasPrefix(value, prefix) || len(value) != len(prefix)+16 {
		return false
	}
	_, err := hex.DecodeString(value[len(prefix):])
	return err == nil
}

func smartImportedPrior(record smartExportStatistic, source string, exportedAt int64, trust float64, now time.Time) (smartPrior, bool) {
	if source == "" || record.ServerIdentity == "" || !smartImportNetworkAllowed(record.Network, true) || !smartValidSums(record.S, record.W) || !smartNonnegative(record.OutageEpisodes) || !smartNonnegative(record.OutageExposureHours) || !smartNonnegative(record.OutageObservedHours) || !smartFinite(trust) || trust <= 0 || trust > 0.5 || exportedAt < 0 || exportedAt > now.Unix() {
		return smartPrior{}, false
	}
	decay := smartDecay(exportedAt, now)
	weight := trust * min(record.W*decay, 12)
	episodes := record.OutageEpisodes * decay * trust
	exposureHours := record.OutageExposureHours * decay * trust
	observedHours := record.OutageObservedHours * decay * trust
	if weight <= 0 && episodes <= 0 && exposureHours <= 0 && observedHours <= 0 {
		return smartPrior{}, false
	}
	prior := smartPrior{Network: record.Network, ServerIdentity: record.ServerIdentity, Source: source, W: weight, At: now.Unix(), OutageEpisodes: episodes, OutageExposureHours: exposureHours, OutageObservedHours: observedHours}
	if record.W > 0 {
		prior.S = weight * (record.S / record.W)
	}
	return prior, true
}

func smartReplacePriors(existing []smartPrior, sources []smartPriorSource, source string, incoming []smartPrior, now time.Time) ([]smartPrior, []smartPriorSource, bool) {
	replaced := slices.ContainsFunc(sources, func(previous smartPriorSource) bool { return previous.ID == source })
	newSources := make([]smartPriorSource, 0, min(len(sources)+1, 16))
	sourceTime := now.Unix()
	for _, previous := range sources {
		sourceTime = max(sourceTime, previous.At)
		if previous.ID != source {
			newSources = append(newSources, previous)
		}
	}
	newSources = append(newSources, smartPriorSource{ID: source, At: sourceTime})
	slices.SortStableFunc(newSources, func(a, b smartPriorSource) int {
		if a.At < b.At {
			return -1
		}
		if a.At > b.At {
			return 1
		}
		return 0
	})
	if len(newSources) > 16 {
		newSources = slices.Clone(newSources[len(newSources)-16:])
	}
	priors := make([]smartPrior, 0, len(existing)+len(incoming))
	for _, prior := range existing {
		if prior.Source != source && slices.ContainsFunc(newSources, func(retained smartPriorSource) bool { return retained.ID == prior.Source }) {
			priors = append(priors, prior)
		}
	}
	for _, prior := range incoming {
		prior.Source = source
		priors = append(priors, prior)
	}
	return priors, newSources, replaced
}

func (h *smartHistory) observeTimeline(key, identity string, success bool, rtt, weight float64, now time.Time) {
	if h.Timeline == nil {
		h.Timeline = make(map[string]map[string][]smartTimelinePoint)
	}
	if h.Timeline[key] == nil {
		h.Timeline[key] = make(map[string][]smartTimelinePoint)
	}
	bucket := now.Unix() / 900 * 900
	points := slices.DeleteFunc(h.Timeline[key][identity], func(point smartTimelinePoint) bool { return point.Time < bucket-95*900 })
	index := len(points) - 1
	if index < 0 || points[index].Time != bucket {
		index = slices.IndexFunc(points, func(point smartTimelinePoint) bool { return point.Time == bucket })
	}
	if index < 0 {
		points = append(points, smartTimelinePoint{Time: bucket})
		slices.SortFunc(points, func(a, b smartTimelinePoint) int {
			if a.Time < b.Time {
				return -1
			}
			if a.Time > b.Time {
				return 1
			}
			return 0
		})
		index = slices.IndexFunc(points, func(point smartTimelinePoint) bool { return point.Time == bucket })
	}
	points[index].Weight += weight
	if success {
		points[index].Success += weight
		if smartFinite(rtt) && rtt > 0 {
			points[index].RTT += rtt
			points[index].RTTWeight++
		}
	}
	if len(points) > 96 {
		points = slices.Delete(points, 0, len(points)-96)
	}
	h.Timeline[key][identity] = points
}

func smartObserveHour(statistic *smartStatistic, success bool, weight float64, now time.Time) {
	bucket := now.Hour() / 4
	decay := smartDecay(statistic.HourUpdatedAt[bucket], now)
	statistic.Hours[bucket][0] *= decay
	statistic.Hours[bucket][1] *= decay
	statistic.Hours[bucket][1] += weight
	if success {
		statistic.Hours[bucket][0] += weight
	}
	statistic.HourUpdatedAt[bucket] = max(statistic.HourUpdatedAt[bucket], now.Unix())
}

func smartHourClass(base int, probability, successes, weight float64) int {
	if weight < 24 {
		return base
	}
	hourClass := smartStabilityClass((successes+smartHistoryShrinkage*probability)/(weight+smartHistoryShrinkage), weight)
	return min(max(hourClass, max(base-1, 0)), min(base+1, 3))
}

func smartMemberIdentity(ctx context.Context, outbound adapter.Outbound) (local, server string, err error) {
	if provider := service.FromContext[adapter.SmartIdentityProvider](ctx); provider != nil {
		return provider.SmartIdentity(outbound)
	}
	if provider, ok := service.FromContext[adapter.OutboundRegistry](ctx).(adapter.SmartIdentityProvider); ok {
		local, server, err = provider.SmartIdentity(outbound)
		if err == nil {
			return
		}
	}
	if provider, ok := service.FromContext[adapter.EndpointRegistry](ctx).(adapter.SmartIdentityProvider); ok {
		return provider.SmartIdentity(outbound)
	}
	return "", "", errors.New("smart outbound requires configured identity provider")
}

func smartIdentityFromOptions(kind string, options any) (local, server string, err error) {
	return adapter.ComputeSmartIdentity(kind, options)
}
