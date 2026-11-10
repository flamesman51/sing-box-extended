package group

import (
	"context"
	"encoding/json"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
)

var (
	ErrSmartNotFound           = E.New("not_found")
	ErrSmartNoGroup            = E.New("no_smart_group")
	ErrSmartInvalid            = E.New("invalid")
	ErrSmartTooLarge           = E.New("too_large")
	ErrSmartUnsupportedVersion = E.New("unsupported_version")
)

func smartGroups(outbounds adapter.OutboundManager) []*Smart {
	if outbounds == nil {
		return nil
	}
	var groups []*Smart
	for _, outbound := range outbounds.Outbounds() {
		if group, ok := outbound.(*Smart); ok {
			groups = append(groups, group)
		}
	}
	return groups
}

func smartFind(outbounds adapter.OutboundManager, tag string) (*Smart, error) {
	groups := smartGroups(outbounds)
	if tag == "" {
		if len(groups) == 0 {
			return nil, ErrSmartNoGroup
		}
		return groups[0], nil
	}
	for _, group := range groups {
		if group.Tag() == tag {
			return group, nil
		}
	}
	return nil, ErrSmartNotFound
}

func smartJSON(value any) (string, error) { data, err := json.Marshal(value); return string(data), err }

func smartNullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func smartTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.Unix()
}

func smartNumber(value float64) any {
	if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return nil
	}
	return value
}

func smartRTTs(values []float64) (any, any) {
	if len(values) == 0 {
		return nil, nil
	}
	p50, p90 := smartRTTQuantiles(values)
	return p50, p90
}

func SmartStatus(_ context.Context, outbounds adapter.OutboundManager, tag string) (response string, err error) {
	guard := smartAPIGuard{site: "status"}
	defer guard.finish(&err)
	s, err := smartFind(outbounds, tag)
	if err != nil {
		return "", err
	}
	guard.lock(s)
	now := s.clock.Now()
	s.observeAllOutagesLocked(now)
	priors := s.priorsLocked()
	state := "starting"
	switch {
	case s.paused:
		state = "paused"
	case !s.network.Link || s.offline:
		state = "offline"
	case !s.allDeadSince.IsZero() && now.Sub(s.allDeadSince) >= 45*time.Second && s.direct.value(now) == "ok":
		state = "all_dead"
	case s.generationSucceeded:
		state = "degraded"
		if s.primary != nil && s.primary.state == "healthy" && s.primary.tier == 1 {
			state = "ok"
		}
	}
	primary, standby, udp := any(nil), any(nil), any(nil)
	udp443 := map[string]any{"mode": "primary", "member": nil, "reason": ""}
	if s.primary != nil {
		primary = s.primary.tag
		udp = s.primary.tag
		udp443["member"] = s.primary.tag
	}
	if s.standby != nil {
		standby = s.standby.tag
	}
	if member, err := s.selectLocked("udp", M.ParseSocksaddr("1.1.1.1:3478"), now); err == nil {
		udp = member.tag
	}
	if !s.udpEnabled || s.quicPolicy == "off" {
		udp443["mode"] = "off"
	} else if member, err := s.selectLocked("udp", M.ParseSocksaddr("1.1.1.1:443"), now); err != nil {
		if s.primary != nil && s.primary.udp443.value(now) == "failed" && s.quicPolicy == "auto" {
			udp443 = map[string]any{"mode": "reject", "member": nil, "reason": "UDP/443 failed on eligible members"}
		}
	} else {
		udp443["member"] = member.tag
		if member != s.primary {
			udp443["mode"] = "member"
		}
	}
	members := make([]map[string]any, 0, len(s.members))
	for rank, member := range s.members {
		probability, weight := s.history.Estimate(s.network.Path, member.identity, member.serverIdentity, now, priors)
		class := s.history.Class(s.network.Path, member.identity, member.serverIdentity, s.historyTime(now), priors)
		rtt, p90 := smartRTTs(member.rtts)
		if s.profileEngine() && len(member.rtts) > 0 {
			rtt, p90 = smartProfileRTTQuantiles(member.rtts)
		}
		memberState := member.state
		if member.emergency {
			memberState = "emergency"
		}
		throughput := member.throughput
		ttl := 24 * time.Hour
		if throughput.Source == "passive" {
			ttl = 6 * time.Hour
		}
		fresh := !throughput.At.IsZero() && now.Sub(throughput.At) < ttl
		download, upload, source, at := any(nil), any(nil), any(nil), any(nil)
		if fresh {
			download = smartNumber(throughput.Download)
			upload = smartNumber(throughput.Upload)
			source = smartNullable(throughput.Source)
			at = smartTime(throughput.At)
		}
		members = append(members, map[string]any{
			"tag": member.tag, "tier": member.tier, "country": smartNullable(member.country), "state": memberState, "probation": member.probation, "quarantine_until": smartTime(member.quarantineUntil),
			"tcp": member.tcp.value(now), "udp": member.udp.value(now), "udp443": member.udp443.value(now), "rtt_ms": rtt, "rtt_p90_ms": p90, "stability": probability, "stability_class": string(rune('A' + class)), "weight": weight,
			"download_mbps": download, "upload_mbps": upload, "throughput_class": s.throughputClass(member, now), "throughput_source": source, "throughput_at": at, "last_ok": smartTime(member.lastOK), "last_fail": smartTime(member.lastFail), "rank": rank + 1,
		})
		if s.profileEngine() {
			entry := members[len(members)-1]
			if slices.Contains(s.preference.rank, "download") || slices.Contains(s.preference.rank, "speed_floor") {
				entry["download_bucket"] = nil
				if download, fresh := smartFreshDownload(member, now); fresh && download > 0 {
					entry["download_bucket"] = int(math.Floor(math.Log(download) / math.Log(1.25)))
				}
			}
			if slices.Contains(s.preference.rank, "speed_floor") {
				entry["meets_floor"] = s.meetsFloorLocked(member, now)
			}
			if slices.Contains(s.preference.rank, "outages") {
				episodes, exposure, observed, class := s.outageEvidenceLocked(member, now)
				var value any
				if class >= 0 {
					value = class
				}
				entry["outages"] = map[string]any{"episodes": episodes, "exposure_hours": exposure, "observed_hours": observed, "class": value}
			}
		}
	}
	events := make([]smartEvent, 0, min(len(s.history.Events), 100))
	for i := len(s.history.Events) - 1; i >= 0 && len(events) < 100; i-- {
		events = append(events, s.history.Events[i])
	}
	budget := smartBudget{}
	budgets := make(map[string]smartBudget, 3)
	for _, networkType := range []string{"wifi", "cellular", "other"} {
		value := smartBudget{NetworkType: networkType, Day: now.In(time.FixedZone("", s.network.Offset)).Format("2006-01-02")}
		value.DayEndsAt = smartBudgetDayEnd(value.Day, s.network.Offset)
		if s.ledger != nil {
			value = s.ledger.Snapshot(networkType, now, s.network.Offset)
		}
		budgets[networkType] = value
	}
	if s.ledger != nil {
		budget = s.ledger.Snapshot(s.network.Type, now, s.network.Offset)
	}
	timeline := s.timelineLocked(now)
	return smartJSON(map[string]any{
		"version": 1, "group": s.Tag(), "now": now.Unix(), "state": state, "generation": s.selectionGeneration,
		"panics": s.panicSnapshot(), "degraded": s.degradedLocked(), "budgets": budgets,
		"throughput_degradation": s.degradationSnapshotLocked(),
		"preference":             s.preferenceStatusLocked(),
		"selected":               map[string]any{"tcp": primary, "standby": standby, "udp": udp, "udp443": udp443},
		"network":                map[string]any{"key": s.network.Key, "type": s.network.Type, "label": smartNullable(s.network.Label), "carrier": smartNullable(s.network.Carrier), "radio": smartNullable(s.network.Radio), "country": smartNullable(s.network.Country)},
		"direct":                 map[string]any{"state": s.direct.value(now), "checked_at": smartTime(s.direct.at), "rtt_ms": smartNumber(float64(s.directRTT) / float64(time.Millisecond)), "download_mbps": nil}, "budget": budget, "members": members, "events": events, "timeline": timeline,
	})
}

func (s *Smart) timelineLocked(now time.Time) map[string]any {
	start := now.Unix()/900*900 - 95*900
	members := make(map[string][][3]any, len(s.members))
	for _, member := range s.members {
		buckets := make([][3]any, 96)
		for _, point := range s.history.Timeline[s.network.Key][member.identity] {
			index := (point.Time - start) / 900
			if index < 0 || index >= 96 {
				continue
			}
			if point.Weight > 0 {
				buckets[index][0] = point.Success / point.Weight
			}
			if point.RTTWeight > 0 {
				buckets[index][1] = point.RTT / point.RTTWeight
			}
			buckets[index][2] = smartNumber(point.MBPS)
		}
		members[member.tag] = buckets
	}
	return map[string]any{"bucket_seconds": 900, "start": start, "members": members}
}

func SmartNetworks(_ context.Context, outbounds adapter.OutboundManager, tag string) (response string, err error) {
	guard := smartAPIGuard{site: "networks"}
	defer guard.finish(&err)
	s, err := smartFind(outbounds, tag)
	if err != nil {
		return "", err
	}
	guard.lock(s)
	now := s.clock.Now()
	priors := s.priorsLocked()
	networks := make([]map[string]any, 0, len(s.history.Networks))
	keys := make([]string, 0, len(s.history.Networks))
	for key := range s.history.Networks {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b string) int {
		left, right := s.history.Networks[a].LastSeen, s.history.Networks[b].LastSeen
		if left > right {
			return -1
		}
		if left < right {
			return 1
		}
		return strings.Compare(a, b)
	})
	for _, key := range keys {
		network := s.history.Networks[key]
		path := []string{key}
		for parent := network.Parent; parent != "" && len(path) < 8 && !slices.Contains(path, parent); {
			path = append(path, parent)
			next := s.history.Networks[parent]
			if next == nil {
				break
			}
			parent = next.Parent
		}
		if len(path) == 1 {
			path = smartNetworkPath(key)
		}
		members := make([]map[string]any, 0, len(s.members))
		totalWeight := 0.0
		for _, member := range s.members {
			stat := s.history.Stats[key][member.identity]
			if stat == nil {
				stat = &smartStatistic{}
			}
			p, w := s.history.Estimate(path, member.identity, member.serverIdentity, now, priors)
			totalWeight += w
			rtt, _ := smartRTTs(stat.RTTs)
			members = append(members, map[string]any{"tag": member.tag, "stability": p, "weight": w, "download_mbps": smartNumber(stat.Download), "rtt_ms": rtt})
		}
		networks = append(networks, map[string]any{"key": key, "type": network.Type, "label": smartNullable(network.Label), "carrier": smartNullable(network.Carrier), "radio": smartNullable(network.Radio), "last_seen": network.LastSeen, "weight": totalWeight, "members": members})
	}
	return smartJSON(map[string]any{"version": 1, "networks": networks})
}

type smartExportGroup struct {
	Networks map[string]smartExportNetwork `json:"networks"`
	Stats    []smartExportStatistic        `json:"stats"`
	Labels   map[string]string             `json:"labels,omitempty"`
}
type smartExportNetwork struct {
	Type     string  `json:"type"`
	Label    *string `json:"label"`
	Carrier  *string `json:"carrier"`
	Radio    *string `json:"radio"`
	LastSeen int64   `json:"last_seen"`
}
type smartExportDocument struct {
	Version    int                         `json:"version"`
	ExportedAt int64                       `json:"exported_at"`
	InstallID  string                      `json:"install_id"`
	Redacted   bool                        `json:"redacted"`
	Device     map[string]string           `json:"device"`
	Groups     map[string]smartExportGroup `json:"groups"`
}

func SmartExport(_ context.Context, outbounds adapter.OutboundManager, raw bool) (response string, err error) {
	guard := smartAPIGuard{site: "export"}
	defer guard.finish(&err)
	groups := smartGroups(outbounds)
	document := smartExportDocument{Version: 1, ExportedAt: time.Now().Unix(), Redacted: !raw, Device: map[string]string{"model": "", "android": "", "app": ""}, Groups: make(map[string]smartExportGroup)}
	pointer := func(value string) *string {
		if value == "" {
			return nil
		}
		return &value
	}
	for _, s := range groups {
		guard.lock(s)
		now := s.clock.Now()
		s.observeAllOutagesLocked(now)
		document.ExportedAt = now.Unix()
		if document.InstallID == "" {
			document.InstallID = s.installID
		}
		group := smartExportGroup{Networks: make(map[string]smartExportNetwork), Stats: make([]smartExportStatistic, 0)}
		if raw {
			group.Labels = maps.Clone(s.history.Labels)
		}
		for key, network := range s.history.Networks {
			var label *string
			if raw {
				label = pointer(network.Label)
			}
			group.Networks[key] = smartExportNetwork{Type: network.Type, Label: label, Carrier: pointer(network.Carrier), Radio: pointer(network.Radio), LastSeen: network.LastSeen}
		}
		for key, records := range s.history.Stats {
			merged := make(map[string]*smartExportStatistic)
			for identity := range records {
				server := s.history.IdentityServers[identity]
				if server == "" {
					continue
				}
				stat := s.history.statistic(key, identity, now)
				record := merged[server]
				if record == nil {
					record = &smartExportStatistic{Network: key, ServerIdentity: server}
					merged[server] = record
				}
				record.S += stat.S
				record.W += stat.W
				record.OutageEpisodes += stat.OutageEpisodes
				record.OutageExposureHours += stat.OutageExposureHours
				record.OutageObservedHours += stat.OutageObservedHours
				if p50, p90 := smartRTTs(stat.RTTs); p50 != nil {
					a, b := p50.(float64), p90.(float64)
					record.RTTP50 = &a
					record.RTTP90 = &b
				}
				if stat.Download > 0 {
					value := stat.Download
					record.Download = &value
				}
				if stat.Upload > 0 {
					value := stat.Upload
					record.Upload = &value
				}
				if stat.ThroughputAt > 0 {
					value := stat.ThroughputAt
					record.ThroughputAt = &value
				}
				for hour := range 6 {
					decay := smartDecay(stat.HourUpdatedAt[hour], now)
					record.Hours[hour][0] += stat.Hours[hour][0] * decay
					record.Hours[hour][1] += stat.Hours[hour][1] * decay
				}
			}
			for _, record := range merged {
				group.Stats = append(group.Stats, *record)
			}
		}
		slices.SortFunc(group.Stats, func(a, b smartExportStatistic) int {
			if a.Network != b.Network {
				return strings.Compare(a.Network, b.Network)
			}
			return strings.Compare(a.ServerIdentity, b.ServerIdentity)
		})
		document.Groups[s.Tag()] = group
		guard.unlock()
	}
	return smartJSON(document)
}

type smartImportApplied struct {
	Network  string  `json:"network"`
	MappedTo string  `json:"mapped_to"`
	Members  int     `json:"members"`
	Weight   float64 `json:"weight"`
}
type smartImportIgnored struct {
	Network string `json:"network"`
	Reason  string `json:"reason"`
}
type smartImportReport struct {
	Version         int                  `json:"version"`
	SourceInstallID string               `json:"source_install_id"`
	Trust           float64              `json:"trust"`
	Applied         []smartImportApplied `json:"applied"`
	Ignored         []smartImportIgnored `json:"ignored"`
	Replaced        bool                 `json:"replaced_previous_import"`
}

var smartMutationAccess sync.Mutex

func SmartImport(_ context.Context, outbounds adapter.OutboundManager, data string, trust float64) (response string, err error) {
	guard := smartAPIGuard{site: "import"}
	defer guard.finish(&err)
	smartMutationAccess.Lock()
	defer smartMutationAccess.Unlock()
	if len(data) > 2*1024*1024 {
		return "", ErrSmartTooLarge
	}
	if trust < 0 || trust > .5 || math.IsNaN(trust) || math.IsInf(trust, 0) {
		return "", ErrSmartInvalid
	}
	var document smartExportDocument
	if err := json.Unmarshal([]byte(data), &document); err != nil {
		return "", ErrSmartInvalid
	}
	if document.Version != 1 {
		return "", ErrSmartUnsupportedVersion
	}
	if document.InstallID == "" || len(document.InstallID) > 256 || document.ExportedAt <= 0 || document.Groups == nil {
		return "", ErrSmartInvalid
	}
	report := smartImportReport{Version: 1, SourceInstallID: document.InstallID, Trust: trust, Applied: make([]smartImportApplied, 0), Ignored: make([]smartImportIgnored, 0)}
	groups := smartGroups(outbounds)
	for _, s := range groups {
		if s.ledger == nil {
			continue
		}
		guard.lock(s)
		now := s.clock.Now()
		known := make(map[string]bool)
		for _, member := range s.members {
			known[member.serverIdentity] = true
		}
		same := document.InstallID == s.installID
		var incoming []smartPrior
		applied := make(map[string]*smartImportApplied)
		importedNetworks := make(map[string]*smartHistoryNetwork)
		for _, sourceGroup := range document.Groups {
			for _, record := range sourceGroup.Stats {
				if !smartImportNetworkAllowed(record.Network, same) {
					reason := "invalid"
					if !same && (strings.Contains(record.Network, "/bssid:") || strings.Contains(record.Network, "/ssid:") || strings.Contains(record.Network, "/tower:")) {
						reason = "foreign_redacted_key"
					}
					report.Ignored = append(report.Ignored, smartImportIgnored{record.Network, reason})
					continue
				}
				if !known[record.ServerIdentity] {
					report.Ignored = append(report.Ignored, smartImportIgnored{record.Network, "unknown_member"})
					continue
				}
				if record.W < 0 || record.S < 0 || record.S > record.W || math.IsNaN(record.W) || math.IsNaN(record.S) || math.IsInf(record.W, 0) || math.IsInf(record.S, 0) || len(record.Network) > 256 {
					report.Ignored = append(report.Ignored, smartImportIgnored{record.Network, "invalid"})
					continue
				}
				prior, ok := smartImportedPrior(record, document.InstallID, document.ExportedAt, trust, now)
				if !ok {
					report.Ignored = append(report.Ignored, smartImportIgnored{record.Network, "stale"})
					continue
				}
				duplicate := false
				for _, existing := range incoming {
					if existing.Network == prior.Network && existing.ServerIdentity == prior.ServerIdentity {
						duplicate = true
						break
					}
				}
				if duplicate {
					continue
				}
				incoming = append(incoming, prior)
				sourceNetwork := sourceGroup.Networks[record.Network]
				network := &smartHistoryNetwork{Type: sourceNetwork.Type, LastSeen: min(sourceNetwork.LastSeen, now.Unix())}
				if sourceNetwork.Carrier != nil {
					network.Carrier = *sourceNetwork.Carrier
				}
				if sourceNetwork.Radio != nil {
					network.Radio = *sourceNetwork.Radio
				}
				if !document.Redacted {
					network.Label = sourceGroup.Labels[record.Network]
					if network.Label == "" && sourceNetwork.Label != nil {
						network.Label = *sourceNetwork.Label
					}
				}
				importedNetworks[record.Network] = network
				entry := applied[record.Network]
				if entry == nil {
					entry = &smartImportApplied{Network: record.Network, MappedTo: record.Network}
					applied[record.Network] = entry
				}
				entry.Members++
				entry.Weight += prior.W
			}
		}
		metadata := s.ledger.Metadata()
		priors, sources, replaced := smartReplacePriors(metadata.Priors[s.Tag()], metadata.PriorSources[s.Tag()], document.InstallID, incoming, now)
		guard.unlock()
		if err := s.ledger.UpdatePriors(s.Tag(), priors, sources); err != nil {
			return "", err
		}
		report.Replaced = report.Replaced || replaced
		for _, entry := range applied {
			report.Applied = append(report.Applied, *entry)
		}
		guard.lock(s)
		s.priors = slices.Clone(priors)
		for key, network := range importedNetworks {
			if s.history.Networks[key] == nil {
				s.history.touch(key, now)
				s.history.Networks[key] = network
			}
			if network.Label != "" && s.history.Labels[key] == "" {
				s.history.Labels[key] = network.Label
			}
		}
		s.eventLocked("import", "", "", s.network.Key, "imported historical priors", now)
		s.notifyUpdatedLocked()
		guard.unlock()
	}
	slices.SortFunc(report.Applied, func(a, b smartImportApplied) int { return strings.Compare(a.Network, b.Network) })
	return smartJSON(report)
}

func SmartReset(_ context.Context, outbounds adapter.OutboundManager, tag string) (err error) {
	guard := smartAPIGuard{site: "reset"}
	defer guard.finish(&err)
	smartMutationAccess.Lock()
	defer smartMutationAccess.Unlock()
	groups := smartGroups(outbounds)
	if tag != "" {
		group, err := smartFind(outbounds, tag)
		if err != nil {
			return err
		}
		groups = []*Smart{group}
	}
	for _, s := range groups {
		guard.group = s
		if s.ledger != nil {
			if err := s.ledger.UpdatePriors(s.Tag(), nil, nil); err != nil {
				return err
			}
		}
		guard.lockPersistence(s)
		guard.lock(s)
		generation := s.historyGeneration
		guard.unlock()
		var resetErr error
		if generations, ok := s.cache.(adapter.SmartHistoryGeneration); ok {
			generation, resetErr = generations.ResetSmartHistoryGeneration(s.Tag(), generation)
		} else if s.cache != nil {
			resetErr = s.cache.ResetSmartHistory(s.Tag())
		}
		guard.lock(s)
		s.historyGeneration = generation
		if resetErr != nil {
			guard.unlock()
			guard.unlockPersistence()
			return resetErr
		}
		s.priors = nil
		s.history.Reset()
		s.eventLocked("reset", "", "", s.network.Key, "learned history reset; live health and budget retained", s.clock.Now())
		s.notifyUpdatedLocked()
		guard.unlock()
		guard.unlockPersistence()
	}
	return nil
}
