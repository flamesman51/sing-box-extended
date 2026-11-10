package group

import (
	"slices"
	"time"
)

type smartOutageObservation struct {
	path                                           []string
	generation, configGeneration, memberGeneration uint64
	at                                             time.Time
	pausedTotal                                    time.Duration
	active                                         bool
	atRisk                                         bool
}

// Observe at lifecycle boundaries as well as ticks; a new path never inherits an unobserved interval.
func (s *Smart) observeOutagesLocked(member *smartMember, now time.Time) {
	if s.history == nil || member == nil || member.identity == "" {
		return
	}
	if s.history.outageObservations == nil {
		s.history.outageObservations = make(map[string]smartOutageObservation)
	}
	pausedTotal := s.pausedTotal
	if s.paused && !s.pauseStarted.IsZero() {
		pausedTotal += max(now.Sub(s.pauseStarted), 0)
	}
	path := s.network.Path
	previous, observed := s.history.outageObservations[member.identity]
	samePath := observed && slices.Equal(previous.path, path)
	if samePath {
		path = previous.path
	} else {
		path = slices.Clone(path)
	}
	current := smartOutageObservation{
		path: path, generation: s.generation, configGeneration: s.configGeneration, memberGeneration: member.generation,
		at: now, pausedTotal: pausedTotal, active: !s.paused && !s.closed && !s.loopStopped && !s.offline && s.network.Link,
		atRisk: member.state == "healthy" || member.state == "suspect",
	}
	continuous := observed && previous.active && current.active && samePath && previous.generation == current.generation && previous.configGeneration == current.configGeneration && previous.memberGeneration == current.memberGeneration
	elapsed := time.Duration(0)
	if continuous {
		elapsed = max(now.Sub(previous.at)-max(pausedTotal-previous.pausedTotal, 0), 0)
	}
	if current.active || elapsed > 0 {
		for index, key := range s.network.Path {
			if key == "" || slices.Contains(s.network.Path[:index], key) {
				continue
			}
			statistic := s.history.statistic(key, member.identity, now)
			if current.active && !continuous && statistic.OutageOpen && (statistic.OutageObservedAtNS == 0 || now.Sub(time.Unix(0, statistic.OutageObservedAtNS)) > 30*time.Minute) {
				statistic.OutageOpen = false
				statistic.OutageRecoverySuccesses = 0
				statistic.OutageRecoveryAtNS = 0
			}
			statistic.OutageObservedAtNS = now.UnixNano()
			statistic.OutageObservedHours += elapsed.Hours()
			if previous.atRisk {
				statistic.OutageExposureHours += elapsed.Hours()
			}
			s.history.Networks[key].LastSeen = max(s.history.Networks[key].LastSeen, now.Unix())
		}
		s.history.IdentityServers[member.identity] = member.serverIdentity
		s.dirty = true
	}
	s.history.outageObservations[member.identity] = current
}

// The caller opens only on a usable-to-quarantined transition, not on failed probation.
func (s *Smart) openOutageLocked(member *smartMember, now time.Time) {
	s.observeOutagesLocked(member, now)
	if s.history == nil || member == nil {
		return
	}
	for index, key := range s.network.Path {
		if key == "" || slices.Contains(s.network.Path[:index], key) {
			continue
		}
		statistic := s.history.statistic(key, member.identity, now)
		if !statistic.OutageOpen {
			statistic.OutageEpisodes++
			statistic.OutageOpen = true
			statistic.OutageRecoverySuccesses = 0
			statistic.OutageRecoveryAtNS = 0
			s.dirty = true
		}
	}
}

// The caller must already have confirmed recovery before closing an episode.
func (s *Smart) closeOutageLocked(member *smartMember, now time.Time) {
	s.observeOutagesLocked(member, now)
	if s.history == nil || member == nil {
		return
	}
	for _, key := range s.network.Path {
		if statistic := s.history.Stats[key][member.identity]; statistic != nil && statistic.OutageOpen {
			statistic.OutageOpen = false
			statistic.OutageRecoverySuccesses = 0
			statistic.OutageRecoveryAtNS = 0
			s.dirty = true
		}
	}
}

// Episode recovery is independent of live health and survives restart or generation changes.
func (s *Smart) observeOutageRecoveryLocked(member *smartMember, success bool, now time.Time) {
	s.observeOutagesLocked(member, now)
	if s.history == nil || member == nil {
		return
	}
	for index, key := range s.network.Path {
		if key == "" || slices.Contains(s.network.Path[:index], key) {
			continue
		}
		statistic := s.history.Stats[key][member.identity]
		if statistic == nil || !statistic.OutageOpen {
			continue
		}
		if !success {
			statistic.OutageRecoverySuccesses = 0
			statistic.OutageRecoveryAtNS = 0
			s.dirty = true
			continue
		}
		if statistic.OutageRecoverySuccesses > 0 && now.Sub(time.Unix(0, statistic.OutageRecoveryAtNS)) < 20*time.Second {
			continue
		}
		statistic.OutageRecoverySuccesses++
		statistic.OutageRecoveryAtNS = now.UnixNano()
		if statistic.OutageRecoverySuccesses == 3 {
			statistic.OutageOpen = false
			statistic.OutageRecoverySuccesses = 0
			statistic.OutageRecoveryAtNS = 0
		}
		s.dirty = true
	}
}

func (s *Smart) outageEstimateLocked(member *smartMember, now time.Time) (episodes, exposureHours float64, class int) {
	episodes, exposureHours, _, class = s.outageEvidenceLocked(member, now)
	return
}

func (s *Smart) outageEvidenceLocked(member *smartMember, now time.Time) (episodes, exposureHours, observedHours float64, class int) {
	class = -1
	if s.history == nil || member == nil || len(s.network.Path) == 0 {
		return
	}
	// Use one hierarchy level, never a sum of overlapping observation periods.
	haveLevel := false
	for index, key := range s.network.Path {
		if key == "" || slices.Contains(s.network.Path[:index], key) {
			continue
		}
		levelEpisodes, levelExposure, levelObserved := 0.0, 0.0, 0.0
		if statistic := s.history.Stats[key][member.identity]; statistic != nil {
			decay := smartDecay(statistic.UpdatedAt, now)
			levelEpisodes = statistic.OutageEpisodes * decay
			levelExposure = statistic.OutageExposureHours * decay
			levelObserved = statistic.OutageObservedHours * decay
		}
		for _, prior := range s.priors {
			if prior.Network == key && prior.ServerIdentity == member.serverIdentity && smartNonnegative(prior.OutageEpisodes) && smartNonnegative(prior.OutageExposureHours) && smartNonnegative(prior.OutageObservedHours) {
				decay := smartDecay(prior.At, now)
				levelEpisodes += prior.OutageEpisodes * decay
				levelExposure += prior.OutageExposureHours * decay
				levelObserved += prior.OutageObservedHours * decay
			}
		}
		if !haveLevel {
			haveLevel = true
			episodes, exposureHours, observedHours = levelEpisodes, levelExposure, levelObserved
		}
		if levelExposure >= 2 {
			episodes, exposureHours, observedHours = levelEpisodes, levelExposure, levelObserved
			break
		}
	}
	if exposureHours < 2 {
		return
	}
	rate := episodes / exposureHours
	switch {
	case rate < 0.05:
		class = 0
	case rate < 0.25:
		class = 1
	case rate < 1:
		class = 2
	default:
		class = 3
	}
	return
}
