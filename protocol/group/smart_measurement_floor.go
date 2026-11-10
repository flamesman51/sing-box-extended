package group

import "time"

const smartPassiveBinCount = 6*60*60 + 5

type smartPassiveBin struct {
	at          int64
	read, write int64
}

// Passive bytes are diagnostics and trigger inputs, never capacity evidence.
type smartPassiveBins struct {
	bins                    [smartPassiveBinCount]smartPassiveBin
	started, completed      int64
	initialized             bool
	peakDown, offeredUp     float64
	peakDownAt, offeredUpAt int64
}

func (b *smartPassiveBins) bytes(second int64) (int64, int64) {
	bin := &b.bins[(second%smartPassiveBinCount+smartPassiveBinCount)%smartPassiveBinCount]
	if bin.at != second {
		return 0, 0
	}
	return bin.read, bin.write
}

func (b *smartPassiveBins) window(end int64) (int64, int64) {
	var read, write int64
	for second := end - 5; second < end; second++ {
		r, w := b.bytes(second)
		read += r
		write += w
	}
	return read, write
}

func (b *smartPassiveBins) advance(now time.Time) {
	second := now.Unix()
	if !b.initialized {
		b.started, b.completed, b.initialized = second, second-1, true
		return
	}
	if second <= b.completed+1 {
		return
	}
	cutoff := second - 6*60*60
	expired := b.peakDown > 0 && b.peakDownAt < cutoff || b.offeredUp > 0 && b.offeredUpAt < cutoff
	if b.peakDownAt < cutoff {
		b.peakDown, b.peakDownAt = 0, 0
	}
	if b.offeredUpAt < cutoff {
		b.offeredUp, b.offeredUpAt = 0, 0
	}
	// Revisit retained windows only when their cached maximum expires.
	start := max(b.completed+2, b.started+5, cutoff+5)
	if expired {
		start = max(b.started+5, cutoff+5)
	}
	for end := start; end <= second; end++ {
		read, write := b.window(end)
		down, up := float64(read)*8/5e6, float64(write)*8/5e6
		if down > 0 && down >= b.peakDown {
			b.peakDown, b.peakDownAt = down, end-5
		}
		if up > 0 && up >= b.offeredUp {
			b.offeredUp, b.offeredUpAt = up, end-5
		}
	}
	b.completed = second - 1
}

func (s *Smart) countPassiveLocked(member *smartMember, read, write int64, now time.Time) {
	if read <= 0 && write <= 0 {
		return
	}
	if member.passiveBins == nil {
		member.passiveBins = new(smartPassiveBins)
	}
	b := member.passiveBins
	b.advance(now)
	second := now.Unix()
	bin := &b.bins[(second%smartPassiveBinCount+smartPassiveBinCount)%smartPassiveBinCount]
	if bin.at != second {
		*bin = smartPassiveBin{at: second}
	}
	bin.read += max(read, 0)
	bin.write += max(write, 0)
}

func (s *Smart) passiveDiagnosticsLocked(member *smartMember, now time.Time) (float64, float64) {
	if member.passiveBins == nil {
		return 0, 0
	}
	member.passiveBins.advance(now)
	return member.passiveBins.peakDown, member.passiveBins.offeredUp
}

type smartFloorState struct {
	Kind   string    `json:"kind"`
	Target float64   `json:"target"`
	At     time.Time `json:"at"`
	Value  float64   `json:"value"`
}

type smartLegacyEpisodeObservation struct {
	Value float64   `json:"value"`
	At    time.Time `json:"at"`
}

func (s *Smart) ordinaryImprovementSuppressedLocked() bool { return s.degradation.episode != nil }

func (s *Smart) downloadStaleAfterRevocationLocked(member *smartMember) bool {
	at := member.throughput.At
	if result := member.throughput.DownloadResult; result.Valid {
		at = result.At
	}
	for _, other := range s.members {
		if other.floorState.Kind == "revoked" && (at.Before(other.floorState.At) || other == member && at.Equal(other.floorState.At)) {
			return true
		}
	}
	return false
}

func (s *Smart) effectiveDownloadLocked(member *smartMember, now time.Time) (float64, bool) {
	value, fresh := smartFreshDownload(member, now)
	at := member.throughput.At
	if result := member.throughput.DownloadResult; result.Valid {
		at = result.At
	}
	if member.floorState.Kind == "revoked" && !at.After(member.floorState.At) {
		fresh = false
	}
	if s.primary != nil && s.primary.floorState.Kind == "revoked" && at.Before(s.primary.floorState.At) {
		fresh = false
	}
	return value, fresh
}

func (s *Smart) legacyEpisodeDownloadLocked(member *smartMember, now time.Time) (float64, bool) {
	value, fresh := smartFreshDownload(member, now)
	at := member.throughput.At
	if member.throughput.Source == "passive" {
		at = time.Time{}
	}
	if result := member.throughput.DownloadResult; result.Valid {
		at = result.At
	}
	obs := member.legacyEpisodeObs
	if obs.At.After(at) || at.IsZero() {
		return obs.Value, !obs.At.IsZero() && now.Sub(obs.At) < 24*time.Hour && smartFinite(obs.Value) && obs.Value >= 0
	}
	return value, fresh
}

func (s *Smart) clearRecoveredFloorLocked(member *smartMember, now time.Time) {
	state := member.floorState
	if state.Kind != "revoked" {
		return
	}
	value, fresh := smartFreshDownload(member, now)
	at := member.throughput.At
	if result := member.throughput.DownloadResult; result.Valid {
		at = result.At
	}
	if !fresh || !at.After(state.At) || value < s.targetMbps {
		return
	}
	member.floorState = smartFloorState{}
	if stat := s.history.Stats[s.network.Key][member.identity]; stat != nil {
		stat.FloorState = smartFloorState{}
	}
	s.dirty = true
}

func (s *Smart) commitDegradationEvidenceLocked(e *smartDegradationEpisode, now time.Time) {
	for _, item := range []struct {
		member  *smartMember
		result  smartDirectionResult
		primary bool
	}{
		{e.primary, e.primaryResult, true}, {e.challenger, e.challengerResult, false},
	} {
		if item.member == nil || !item.result.Valid {
			continue
		}
		member, result := item.member, item.result
		if !member.downloadUnderLoad.Valid || !result.At.Before(member.downloadUnderLoad.At) {
			member.downloadUnderLoad = result
		}
		var floor smartFloorState
		if item.primary && result.Value < s.degradationThresholdLocked() {
			floor = smartFloorState{Kind: "revoked", Target: s.targetMbps, At: result.At, Value: result.Value}
		}
		if !item.primary && result.Value >= s.targetMbps {
			floor = smartFloorState{Kind: "proven", Target: s.targetMbps, At: result.At, Value: result.Value}
		}
		if floor.Kind != "" && !floor.At.Before(member.floorState.At) {
			member.floorState = floor
		}
		if !s.profileEngine() && !result.At.Before(member.legacyEpisodeObs.At) {
			member.legacyEpisodeObs = smartLegacyEpisodeObservation{Value: result.Value, At: result.At}
		}
		stat := s.history.statistic(e.key, member.identity, now)
		stat.FloorState, stat.LegacyEpisodeObs, stat.DownloadUnderLoad = member.floorState, member.legacyEpisodeObs, member.downloadUnderLoad
		s.history.IdentityServers[member.identity] = member.serverIdentity
	}
	s.dirty = true
}
