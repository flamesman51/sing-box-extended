package group

import (
	"cmp"
	"math"
	"slices"
	"time"
)

type smartUDPRefresh struct {
	at, retryAt, deadline time.Time
	retries               int
	pending               bool
}

func (s *Smart) profileEngine() bool { return s.preference.profile != "" && !s.preference.legacy }

func smartFreshDownload(member *smartMember, now time.Time) (float64, bool) {
	value := member.throughput
	return value.Download, value.Source == "active" && !value.At.IsZero() && now.Sub(value.At) < 24*time.Hour && smartFinite(value.Download) && value.Download >= 0
}

func smartProfileRTTQuantiles(values []float64) (p50, p90 float64) {
	if len(values) == 0 {
		return 0, 0
	}
	var buffer [64]float64
	n := copy(buffer[:], values[max(len(values)-64, 0):])
	sorted := buffer[:n]
	slices.Sort(sorted)
	return sorted[(n-1)/2], sorted[(9*n+9)/10-1]
}

func (s *Smart) meetsFloorLocked(member *smartMember, now time.Time) bool {
	s.clearRecoveredFloorLocked(member, now)
	state := member.floorState
	at := member.throughput.At
	if result := member.throughput.DownloadResult; result.Valid {
		at = result.At
	}
	if state.Kind == "proven" && now.Sub(state.At) < 24*time.Hour && state.Target >= s.targetMbps && !at.After(state.At) {
		return true
	}
	if state.Kind == "revoked" {
		return false
	}
	download, fresh := s.effectiveDownloadLocked(member, now)
	threshold := s.targetMbps
	if member == s.primary {
		threshold *= 1 - s.preference.hysteresis
	}
	return fresh && download >= threshold
}

func (s *Smart) rankProfileMemberLocked(member *smartMember, now time.Time) {
	member.rankState = 3
	switch {
	case member.state == "healthy":
		member.rankState = 0
	case member.emergency:
		member.rankState = 1
	case member.state == "suspect":
		member.rankState = 2
	}
	member.rankStability = s.history.Class(s.network.Path, member.identity, member.serverIdentity, s.historyTime(now), s.priors)
	member.rankThroughput = s.throughputClass(member, now)
	member.rankFloor = s.meetsFloorLocked(member, now)
	member.rankDownload = math.MaxInt
	if download, fresh := s.effectiveDownloadLocked(member, now); fresh {
		member.rankDownload = 1 << 30
		if download > 0 {
			member.rankDownload = -int(math.Floor(math.Log(download) / math.Log(1.25)))
		}
	}
	member.rankLatency = math.Inf(1)
	member.rankSpread = math.MaxInt
	p50, p90 := smartProfileRTTQuantiles(member.rtts)
	if len(member.rtts) >= 3 {
		member.rankLatency = math.Floor(p50 / 25)
	}
	if len(member.rtts) >= 8 {
		member.rankSpread = int(math.Floor((p90 - p50) / 25))
	}
	member.rankOutages = -1
	if slices.Contains(s.preference.rank, "outages") {
		_, _, member.rankOutages = s.outageEstimateLocked(member, now)
	}
}

func smartUDPKey(evidence smartEvidence, now time.Time) int {
	switch evidence.value(now) {
	case "ok":
		return 0
	case "failed":
		return 2
	default:
		return 1
	}
}

func smartBoolCompare(a, b bool) int {
	if a == b {
		return 0
	}
	if !a {
		return -1
	}
	return 1
}

func (s *Smart) profileCriterionCompareLocked(a, b *smartMember, criterion string, now time.Time) int {
	switch criterion {
	case "stability":
		return cmp.Compare(a.rankStability, b.rankStability)
	case "speed_floor":
		return smartBoolCompare(!a.rankFloor, !b.rankFloor)
	case "download":
		return cmp.Compare(a.rankDownload, b.rankDownload)
	case "throughput":
		return cmp.Compare(a.rankThroughput, b.rankThroughput)
	case "upload":
		aValue, aFresh := smartFreshUpload(a, now)
		bValue, bFresh := smartFreshUpload(b, now)
		return cmp.Compare(smartSpeedBucket(aValue, aFresh), smartSpeedBucket(bValue, bFresh))
	case "rtt", "jitter":
		var aBuffer, bBuffer [64]float64
		return cmp.Compare(smartRTTBucket(smartFreshRTTSamples(a, now, aBuffer[:]), criterion),
			smartRTTBucket(smartFreshRTTSamples(b, now, bBuffer[:]), criterion))
	case "latency":
		return cmp.Compare(a.rankLatency, b.rankLatency)
	case "rtt_spread":
		return cmp.Compare(a.rankSpread, b.rankSpread)
	case "udp":
		return cmp.Compare(smartUDPKey(a.udp, now), smartUDPKey(b.udp, now))
	case "quic":
		return cmp.Compare(smartUDPKey(a.udp443, now), smartUDPKey(b.udp443, now))
	case "outages":
		key := func(class int) int {
			if class < 0 {
				return 1
			}
			if class == 0 {
				return 0
			}
			return class + 1
		}
		return cmp.Compare(key(a.rankOutages), key(b.rankOutages))
	}
	return 0
}

func (s *Smart) profileCompareLocked(a, b *smartMember, now time.Time) (int, string) {
	if d := cmp.Compare(a.tier, b.tier); d != 0 {
		return d, "tier"
	}
	if d := cmp.Compare(a.rankState, b.rankState); d != 0 {
		return d, "state"
	}
	if floor := s.preference.minStability; floor >= 0 {
		if d := smartBoolCompare(a.rankStability > floor, b.rankStability > floor); d != 0 {
			return d, "min_stability"
		}
	}
	for _, criterion := range s.preference.rank {
		if d := s.profileCriterionCompareLocked(a, b, criterion, now); d != 0 {
			return d, criterion
		}
		if criterion == "speed_floor" && !a.rankFloor && !b.rankFloor {
			if d := s.profileCriterionCompareLocked(a, b, "download", now); d != 0 {
				return d, "speed_floor"
			}
		}
	}
	return 0, ""
}

func (s *Smart) rankProfileLocked(now time.Time) {
	for _, member := range s.members {
		s.rankProfileMemberLocked(member, now)
	}
	country := ""
	if s.primary != nil {
		country = s.primary.country
	}
	slices.SortStableFunc(s.members, func(a, b *smartMember) int {
		if d, _ := s.profileCompareLocked(a, b, now); d != 0 {
			return d
		}
		if country != "" && a.country != b.country {
			if a.country == country {
				return -1
			}
			if b.country == country {
				return 1
			}
		}
		return cmp.Compare(a.configOrder, b.configOrder)
	})
}

func (s *Smart) betterLocked(candidate, primary *smartMember, now time.Time) (bool, string) {
	s.rankProfileMemberLocked(candidate, now)
	s.rankProfileMemberLocked(primary, now)
	difference, criterion := s.profileCompareLocked(candidate, primary, now)
	if difference >= 0 {
		return false, criterion
	}
	if criterion == "download" || criterion == "speed_floor" && !candidate.rankFloor && !primary.rankFloor {
		candidateDownload, candidateFresh := s.effectiveDownloadLocked(candidate, now)
		primaryDownload, primaryFresh := s.effectiveDownloadLocked(primary, now)
		if !candidateFresh || primaryFresh && candidateDownload < s.preference.speedMargin*primaryDownload {
			return false, criterion
		}
	}
	if criterion == "upload" {
		candidateUpload, candidateFresh := smartFreshUpload(candidate, now)
		primaryUpload, primaryFresh := smartFreshUpload(primary, now)
		if !candidateFresh || primaryFresh && candidateUpload < s.preference.speedMargin*primaryUpload {
			return false, criterion
		}
	}
	return true, criterion
}

func (s *Smart) safeToProbeLocked(candidate, primary *smartMember, now time.Time) bool {
	if primary == nil || candidate == nil || candidate == primary || candidate.state != "healthy" || candidate.tier != primary.tier {
		return false
	}
	return s.preference.minStability < 0 || s.history.Class(s.network.Path, candidate.identity, candidate.serverIdentity, s.historyTime(now), s.priors) <= s.preference.minStability
}

func smartCriterionObservation(member *smartMember, criterion string) time.Time {
	switch criterion {
	case "speed_floor", "download", "throughput":
		if result := member.throughput.DownloadResult; result.Valid {
			return result.At
		}
		return member.throughput.At
	case "upload":
		return member.throughput.UploadResult.At
	case "rtt":
		return member.udpObservation.At
	case "jitter":
		return member.jitterObservation.At
	case "udp":
		return member.udp.at
	case "quic":
		return member.udp443.at
	case "stability", "outages", "latency", "rtt_spread":
		if member.lastFail.After(member.lastOK) {
			return member.lastFail
		}
		return member.lastOK
	default:
		return time.Time{}
	}
}

func (s *Smart) resetProfileImprovementLocked() {
	s.improvementEpoch++
	if candidate := s.improvementCandidate; candidate != nil {
		if candidate.confirmationCancel != nil {
			candidate.confirmationCancel()
			candidate.confirmationCancel = nil
		}
		candidate.improvementObservations = 0
		candidate.improvementAt = time.Time{}
		candidate.improvementCriterion = ""
	}
	s.improvementCandidate = nil
}

func (s *Smart) chooseProfileImprovementLocked(now time.Time) {
	if s.primary == nil || s.ordinaryImprovementSuppressedLocked() {
		return
	}
	if s.preference.tierRestore > 0 {
		for _, candidate := range s.members {
			if candidate != s.primary && candidate.state == "healthy" && candidate.tier < s.primary.tier && now.Sub(candidate.healthySince) >= s.preference.tierRestore {
				s.switchLocked(candidate, "tier_restore", "preferred tier confirmed healthy", now)
				return
			}
		}
	}
	if !s.preference.improvement {
		s.resetProfileImprovementLocked()
		return
	}
	var winner *smartMember
	criterion := ""
	for _, candidate := range s.members {
		if candidate == s.primary || candidate.state != "healthy" || candidate.tier != s.primary.tier {
			continue
		}
		if better, decisive := s.betterLocked(candidate, s.primary, now); better {
			winner, criterion = candidate, decisive
			break
		}
	}
	if winner == nil {
		s.resetProfileImprovementLocked()
		return
	}
	if !s.profileCriterionFreshLocked(winner, criterion, now) {
		s.resetProfileImprovementLocked()
		return
	}
	observation := smartCriterionObservation(winner, criterion)
	if winner != s.improvementCandidate || criterion != winner.improvementCriterion {
		s.resetProfileImprovementLocked()
		s.improvementCandidate = winner
		winner.improvementCriterion = criterion
	}
	if !observation.IsZero() && s.profileObservationBetterLocked(winner, s.primary, criterion, now) &&
		(winner.improvementObservations == 0 || observation.After(winner.improvementAt) && observation.Sub(winner.improvementAt) >= s.preference.confirmationInterval) {
		winner.improvementAt = observation
		winner.improvementObservations++
	}
	if winner.improvementObservations >= s.preference.confirmations && now.Sub(s.selectedAt) >= s.preference.dwell {
		s.switchLocked(winner, "switch", "confirmed "+criterion+" improvement", now)
		return
	}
	s.scheduleImprovementConfirmationLocked(now)
}

func (s *Smart) queueProfileUDPLocked(now time.Time, discovery bool) {
	if s.primary == nil {
		return
	}
	for index, criterion := range []string{"udp", "quic"} {
		active := slices.Contains(s.preference.rank, criterion)
		if criterion == "udp" {
			active = active || slices.Contains(s.preference.rank, "rtt") || slices.Contains(s.preference.rank, "jitter")
		}
		if discovery && criterion != "quic" || !discovery && !active {
			continue
		}
		interval, kind := 20*time.Minute, "udp"
		if criterion == "quic" {
			interval, kind = 8*time.Minute, "udp443"
		}
		var fixed [3]*smartMember
		if s.standby != nil && s.standby.state == "healthy" {
			fixed[0] = s.standby
		}
		for _, member := range s.members {
			if member != fixed[0] && s.safeToProbeLocked(member, s.primary, now) {
				fixed[1] = member
				break
			}
		}
		if candidate := s.improvementCandidate; candidate != nil && candidate.improvementObservations < s.preference.confirmations && s.safeToProbeLocked(candidate, s.primary, now) {
			fixed[2] = candidate
		}
		for position, member := range fixed {
			if member == nil || slices.Contains(fixed[:position], member) {
				continue
			}
			s.queueProfileUDPFixedLocked(member, index, kind, interval, now)
		}
		if !discovery && !s.udpPoolAt[index].IsZero() && now.Sub(s.udpPoolAt[index]) < interval {
			continue
		}
		s.udpPoolAt[index] = now
		fallback := false
		for _, member := range s.members {
			if s.udpFallbackEligibleLocked(member, s.primary, criterion == "quic", now) {
				fallback = true
				break
			}
		}
		var first, next *smartMember
		firstOrder, nextOrder := math.MaxInt, math.MaxInt
		for _, member := range s.members {
			if member == s.primary || slices.Contains(fixed[:], member) || member.state != "healthy" || member.tier != s.primary.tier {
				continue
			}
			order := member.configOrder
			if !s.safeToProbeLocked(member, s.primary, now) {
				if fallback {
					continue
				}
				order += len(s.members)
			}
			if order < firstOrder {
				first, firstOrder = member, order
			}
			if order >= s.udpPoolNext[index] && order < nextOrder {
				next, nextOrder = member, order
			}
		}
		if next == nil {
			next = first
			nextOrder = firstOrder
		}
		if next != nil {
			s.udpPoolNext[index] = nextOrder + 1
			evidence := next.udp
			if kind == "udp443" {
				evidence = next.udp443
			}
			if evidence.at.IsZero() || now.Sub(evidence.at) >= interval {
				s.queueLocked(next, kind, 0, false, now)
			}
		}
	}
}

func (s *Smart) queueProfileUDPFixedLocked(member *smartMember, index int, kind string, interval time.Duration, now time.Time) {
	evidence := member.udp
	ttl := 30 * time.Minute
	if kind == "udp443" {
		evidence, ttl = member.udp443, 10*time.Minute
	}
	refresh := &member.udpRefresh[index]
	if !refresh.at.Equal(evidence.at) {
		*refresh = smartUDPRefresh{at: evidence.at}
	}
	if !evidence.at.IsZero() && now.Sub(evidence.at) < interval {
		return
	}
	if refresh.pending {
		if member.queued[kind] {
			return
		}
		refresh.pending = false
		smartUDPRetry(refresh, now)
	}
	if !refresh.deadline.IsZero() && !now.Before(refresh.deadline) {
		*refresh = smartUDPRefresh{at: evidence.at}
	}
	if now.Before(refresh.retryAt) {
		return
	}
	if refresh.deadline.IsZero() {
		refresh.deadline = evidence.expires
		if !now.Before(refresh.deadline) {
			refresh.deadline = now.Add(ttl)
		}
	}
	s.queueLocked(member, kind, 0, false, now)
	refresh.pending = member.queued[kind]
	if !refresh.pending {
		smartUDPRetry(refresh, now)
	}
}

func smartUDPRetry(refresh *smartUDPRefresh, now time.Time) {
	delays := [...]time.Duration{30 * time.Second, time.Minute, 2 * time.Minute}
	refresh.retryAt = now.Add(delays[min(refresh.retries, len(delays)-1)])
	refresh.retries++
}

func (s *Smart) observeAllOutagesLocked(now time.Time) {
	if s.profileEngine() {
		for _, member := range s.members {
			s.observeOutagesLocked(member, now)
		}
	}
}

func (s *Smart) setOfflineLocked(offline bool, now time.Time) {
	if s.offline != offline {
		s.observeAllOutagesLocked(now)
		s.offline = offline
		s.observeAllOutagesLocked(now)
	}
}

func (s *Smart) profileMemberStateChangedLocked(member *smartMember, now time.Time) {
	if !s.profileEngine() {
		return
	}
	s.observeOutagesLocked(member, now)
	if member == s.improvementCandidate && member.state != "healthy" {
		s.resetProfileImprovementLocked()
	}
}

func (s *Smart) udpFallbackEligibleLocked(member, primary *smartMember, quic bool, now time.Time) bool {
	if member.state != "healthy" {
		return false
	}
	if quic {
		return member.tier == primary.tier && member.udp443.value(now) == "ok" && s.throughputClass(member, now) <= s.throughputClass(primary, now)+1
	}
	return member.udp.value(now) == "ok"
}
