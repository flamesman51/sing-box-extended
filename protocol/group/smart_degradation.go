package group

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"
)

const (
	smartDegradationConfirmation = "degradation-confirmation"
	smartDegradationChallenger   = "degradation-challenger"
	// Cover the 1, 2, and 4 second TCP RTO backoff steps without treating long idle periods as demand.
	smartDegradationMaxStall = 8 * time.Second
)

type smartDegradationEpisode struct {
	primary, challenger                                                         *smartMember
	key                                                                         string
	generation, configGeneration, memberGeneration, challengerGeneration, epoch uint64
	ctx                                                                         context.Context
	cancel                                                                      context.CancelFunc
	until                                                                       time.Time
	confirmed                                                                   float64
	checked                                                                     time.Time
}

type smartDegradationState struct {
	episode               *smartDegradationEpisode
	sampled, window       time.Time
	previous              int64
	coverage              smartDegradationCoverage
	flows                 int
	observed, provisional float64
	state, blocked        string
	checked, retry        time.Time
	attempts              map[string]time.Time
	cooldowns             map[string]time.Time
	scopeKey              string
	scopePrimary          *smartMember
}

func (s *Smart) degradationKey(member *smartMember) string {
	digest := sha256.Sum256([]byte(s.Tag() + "\x00" + s.network.Key + "\x00" + member.identity))
	return hex.EncodeToString(digest[:])
}

func (s *Smart) clearDegradationLocked() {
	d := &s.degradation
	if e := d.episode; e != nil {
		e.cancel()
		for _, member := range []*smartMember{e.primary, e.challenger} {
			if member != nil && member.degradationQueued == e {
				delete(member.queued, "throughput")
				member.degradationQueued = nil
			}
		}
		if s.admission != nil && s.admission.job.episode == e {
			s.admission = nil
		}
	}
	d.episode = nil
	d.window = time.Time{}
	d.sampled = time.Time{}
	d.coverage = smartDegradationCoverage{}
	d.flows = 0
	d.provisional = 0
}

func (s *Smart) degradationValidLocked(e *smartDegradationEpisode, now time.Time) bool {
	return e != nil && s.degradation.episode == e && e.ctx.Err() == nil && !s.closed && !s.paused && s.network.Link && s.primary == e.primary && s.primaryEpoch == e.epoch && s.network.Key == e.key && s.generation == e.generation && s.configGeneration == e.configGeneration && e.primary.generation == e.memberGeneration && e.primary.state == "healthy" && now.Before(e.until)
}

func (s *Smart) degradationProbeEligibleLocked(member *smartMember, now time.Time) bool {
	if s.profileEngine() {
		return s.safeToProbeLocked(member, s.primary, now)
	}
	return s.demandChallengerEligibleLocked(member, now)
}

func (s *Smart) degradationThresholdLocked() float64 {
	if !s.profileEngine() {
		return s.targetMbps * .5
	}
	for _, criterion := range s.preference.rank {
		if criterion == "speed_floor" || criterion == "download" || criterion == "throughput" {
			return (1 - s.preference.hysteresis) * s.targetMbps
		}
	}
	return 0
}

func (s *Smart) degradationCandidateLocked(now time.Time) *smartMember {
	var candidate *smartMember
	for _, member := range s.members {
		if !s.degradationProbeEligibleLocked(member, now) {
			continue
		}
		if candidate == nil || s.degradation.attempts[member.identity].Before(s.degradation.attempts[candidate.identity]) {
			candidate = member
		}
	}
	return candidate
}

func (s *Smart) scheduleDegradationLocked(now time.Time) bool {
	d := &s.degradation
	if e := d.episode; e != nil {
		if !s.degradationValidLocked(e, now) {
			s.clearDegradationLocked()
			d.state = "idle"
			return false
		}
		member, purpose := e.primary, smartDegradationConfirmation
		if e.confirmed > 0 {
			if e.challenger == nil || e.challenger.generation != e.challengerGeneration || !s.degradationProbeEligibleLocked(e.challenger, now) {
				s.finishDegradationLocked(now, 30*time.Minute, "ineligible")
				return false
			}
			member, purpose = e.challenger, smartDegradationChallenger
		}
		if !member.queued["throughput"] && (s.admission == nil || s.admission.job.episode != e) && !now.Before(d.retry) {
			s.startDegradationAdmissionLocked(member, purpose, e, now)
		}
		return true
	}
	if s.primary == nil || s.primary.state != "healthy" || s.degradationThresholdLocked() == 0 || !s.throughputAllowedLocked(now) {
		return false
	}
	if d.scopeKey != s.network.Key || d.scopePrimary != s.primary {
		d.scopeKey = s.network.Key
		d.scopePrimary = s.primary
		d.retry = time.Time{}
		d.window = time.Time{}
	}
	if !d.sampled.IsZero() && now.Sub(d.sampled) < 500*time.Millisecond {
		return false
	}
	total := s.primary.passiveRead
	flows := 0
	var lastRead time.Time
	for lease := range s.leases[s.primary] {
		if lease.generation == s.generation && now.Sub(lease.started) >= 10*time.Second {
			if lease.lastRead.After(lastRead) {
				lastRead = lease.lastRead
			}
			flows++
		}
	}
	if d.window.IsZero() || flows == 0 || now.Sub(lastRead) > smartDegradationMaxStall || now.Sub(d.sampled) > 3*time.Second {
		d.window, d.sampled = now, now
		d.previous = total
		d.coverage = smartDegradationCoverage{}
		d.flows = flows
		if flows == 0 || now.Sub(lastRead) > smartDegradationMaxStall {
			d.window = time.Time{}
		}
		return false
	}
	sampleInterval := now.Sub(d.sampled)
	delta := total - d.previous
	d.observed = float64(delta) * 8 / sampleInterval.Seconds() / 1e6
	d.previous = total
	if d.observed >= s.degradationThresholdLocked() {
		d.window = now
		d.coverage = smartDegradationCoverage{}
		d.sampled = now
		d.state = "idle"
		return false
	}
	d.sampled = now
	d.coverage.add(sampleInterval, delta)
	d.flows = flows
	if d.coverage.duration < 20*time.Second {
		d.state = "observing"
		return false
	}
	qualified := d.coverage.bytes >= 128*1024
	if !qualified {
		d.state = "observing"
		return false
	}
	d.window = time.Time{}
	key := s.degradationKey(s.primary)
	retry := d.cooldowns[key]
	if persisted := s.ledger.DegradationCooldown(key); persisted.After(retry) {
		retry = persisted
	}
	if retry.After(now) {
		d.retry = retry
		d.state = "cooldown"
		return false
	}
	if d.retry.After(now) {
		return false
	}
	ctx := s.workCtx
	if ctx == nil {
		ctx = s.ctx
	}
	ctx, cancel := context.WithCancel(ctx)
	e := &smartDegradationEpisode{primary: s.primary, key: s.network.Key, generation: s.generation, configGeneration: s.configGeneration, memberGeneration: s.primary.generation, epoch: s.primaryEpoch, ctx: ctx, cancel: cancel, until: now.Add(5 * time.Minute)}
	d.episode = e
	d.state = "queued"
	d.blocked = ""
	d.retry = time.Time{}
	s.startDegradationAdmissionLocked(s.primary, smartDegradationConfirmation, e, now)
	return true
}

func (s *Smart) finishDegradationLocked(now time.Time, cooldown time.Duration, reason string) {
	d := &s.degradation
	e := d.episode
	if e == nil {
		return
	}
	key := s.degradationKey(e.primary)
	until := now.Add(cooldown)
	if d.cooldowns == nil {
		d.cooldowns = make(map[string]time.Time)
	}
	for old, expiry := range d.cooldowns {
		if !expiry.After(now) {
			delete(d.cooldowns, old)
		}
	}
	d.cooldowns[key] = until
	d.retry = until
	d.state = reason
	d.blocked = reason
	s.clearDegradationLocked()
	s.persistDegradationCooldownLocked(key, now, until)
}

func (s *Smart) persistDegradationCooldownLocked(key string, now, until time.Time) {
	if s.ledger != nil {
		s.accountingWorkers.Go(func() {
			var err error
			defer s.recoverError("degradation-cooldown", &err)
			ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), smartContainmentKey{}, s), smartShutdownTimeout)
			defer cancel()
			err = s.ledger.SetDegradationCooldown(ctx, key, now, until)
			if err != nil && s.logger != nil {
				s.logger.Error("save smart degradation cooldown: ", err)
			}
		})
	}
}

func (s *Smart) applyDegradationResultLocked(result smartResult, now time.Time) {
	e := result.episode
	owned := result.member != nil && result.member.degradationQueued == e
	if owned {
		delete(result.member.queued, "throughput")
		result.member.degradationQueued = nil
	}
	if !s.degradationValidLocked(e, now) {
		if s.degradation.episode == e {
			s.clearDegradationLocked()
		}
		return
	}
	if result.contained || result.err != nil || !owned {
		s.finishDegradationLocked(now, 30*time.Minute, "no_data")
		return
	}
	d := &s.degradation
	d.checked = now
	if result.purpose == smartDegradationConfirmation {
		if result.member != e.primary {
			s.finishDegradationLocked(now, 30*time.Minute, "invalidated")
			return
		}
		if result.download >= s.targetMbps {
			e.primary.throughput = smartThroughput{Download: result.download, At: now, Source: "active"}
			e.primary.floorDegraded = false
			s.finishDegradationLocked(now, time.Hour, "primary_fast")
			return
		}
		if result.download <= 0 || result.download >= s.degradationThresholdLocked() {
			s.finishDegradationLocked(now, 30*time.Minute, "inconclusive")
			return
		}
		if s.profileEngine() {
			e.primary.floorDegraded = true
			e.primary.throughput = smartThroughput{Download: result.download, At: now, Source: "active"}
			e.primary.lastThroughput = now
			for _, key := range s.network.Path {
				stat := s.history.statistic(key, e.primary.identity, now)
				stat.Download = result.download
				stat.ThroughputAt = now.Unix()
				points := s.history.Timeline[key][e.primary.identity]
				if len(points) > 0 {
					points[len(points)-1].MBPS = result.download
				}
			}
			s.history.IdentityServers[e.primary.identity] = e.primary.serverIdentity
			s.eventLocked("speedtest", "", e.primary.tag, s.network.Key, "confirmed primary degradation", now)
		}
		e.confirmed = result.download
		e.checked = now
		d.provisional = result.download
		d.state = "confirmed"
		if s.degradationChallengerRetryLocked().After(now) {
			s.finishDegradationLocked(now, 30*time.Minute, "challenger_cooldown")
			return
		}
		e.challenger = s.degradationCandidateLocked(now)
		if e.challenger == nil {
			s.finishDegradationLocked(now, 30*time.Minute, "no_challenger")
			return
		}
		e.challengerGeneration = e.challenger.generation
		return
	}
	if result.purpose != smartDegradationChallenger || e.challenger != result.member || result.member.generation != e.challengerGeneration || !s.degradationProbeEligibleLocked(result.member, now) {
		s.finishDegradationLocked(now, 30*time.Minute, "ineligible")
		return
	}
	if s.profileEngine() && result.download <= 0 {
		s.finishDegradationLocked(now, 30*time.Minute, "inconclusive")
		return
	}
	if !s.profileEngine() && result.download < s.targetMbps {
		s.finishDegradationLocked(now, 30*time.Minute, "shared_bottleneck_suspected")
		return
	}
	e.primary.throughput = smartThroughput{Download: e.confirmed, At: e.checked, Source: "active"}
	result.member.throughput = smartThroughput{Download: result.download, At: now, Source: "active"}
	if result.download >= s.targetMbps {
		result.member.floorDegraded = false
	}
	for _, member := range []*smartMember{e.primary, result.member} {
		measuredAt := now
		if s.profileEngine() {
			measuredAt = member.throughput.At
		}
		member.lastThroughput = measuredAt
		for _, key := range s.network.Path {
			stat := s.history.statistic(key, member.identity, now)
			stat.Download = member.throughput.Download
			stat.ThroughputAt = measuredAt.Unix()
		}
	}
	s.eventLocked("speedtest", "", result.member.tag, s.network.Key, "fresh degradation pair", now)
	reason := "demand challenger meets target"
	if s.profileEngine() {
		better, criterion := s.betterLocked(result.member, e.primary, now)
		if !s.preference.improvement || !better {
			s.finishDegradationLocked(now, 30*time.Minute, "no_improvement")
			return
		}
		reason = "confirmed degradation improvement: " + criterion
	}
	s.switchLocked(result.member, "switch", reason, now)
	d.state = "switched"
	d.blocked = ""
	d.provisional = 0
}

func (s *Smart) degradationSnapshotLocked() map[string]any {
	d := &s.degradation
	return map[string]any{"state": d.state, "observed_mbps": smartNumber(d.observed), "provisional_mbps": smartNumber(d.provisional), "continuous_flows": d.flows, "checked_at": smartTime(d.checked), "retry_at": smartTime(d.retry), "blocked_reason": smartNullable(d.blocked)}
}

func (s *Smart) degradationPassive(e *smartDegradationEpisode) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.degradationValidLocked(e, s.clock.Now()) {
		return 0
	}
	return e.primary.passiveRead
}

func (s *Smart) degradationJobValid(job smartJob) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.throughputAllowedLocked(s.clock.Now()) && s.degradationValidLocked(job.episode, s.clock.Now()) && (job.purpose != smartDegradationChallenger || job.member == job.episode.challenger && job.member.generation == job.episode.challengerGeneration && s.degradationProbeEligibleLocked(job.member, s.clock.Now()))
}

func (s *Smart) degradationChallengerCooldownKey() string {
	digest := sha256.Sum256([]byte("challenger\x00" + s.Tag()))
	return hex.EncodeToString(digest[:])
}

func (s *Smart) degradationChallengerRetryLocked() time.Time {
	var retry time.Time
	if !s.lastChallenger.IsZero() {
		retry = s.lastChallenger.Add(time.Hour)
	}
	if s.ledger != nil {
		if persisted := s.ledger.DegradationCooldown(s.degradationChallengerCooldownKey()); persisted.After(retry) {
			retry = persisted
		}
	}
	return retry
}

type smartDegradationSample struct {
	duration time.Duration
	bytes    float64
}

type smartDegradationCoverage struct {
	// Samples are at least 500 ms, so 41 entries cover a partially trimmed 20 s window.
	samples     [41]smartDegradationSample
	head, count int
	duration    time.Duration
	bytes       float64
}

func (c *smartDegradationCoverage) add(duration time.Duration, bytes int64) {
	c.samples[(c.head+c.count)%len(c.samples)] = smartDegradationSample{duration: duration, bytes: float64(bytes)}
	c.count++
	c.duration += duration
	c.bytes += float64(bytes)
	for c.duration > 20*time.Second {
		oldest := &c.samples[c.head]
		trim := min(oldest.duration, c.duration-20*time.Second)
		removedBytes := oldest.bytes * float64(trim) / float64(oldest.duration)
		c.duration -= trim
		c.bytes -= removedBytes
		oldest.bytes -= removedBytes
		oldest.duration -= trim
		if oldest.duration == 0 {
			c.head = (c.head + 1) % len(c.samples)
			c.count--
		}
	}
}
