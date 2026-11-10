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
	episode                   *smartDegradationEpisode
	sampled, window, progress time.Time
	baseline, previous        int64
	ticks, flows              int
	observed, provisional     float64
	state, blocked            string
	checked, retry            time.Time
	attempts                  map[string]time.Time
	cooldowns                 map[string]time.Time
	scopeKey                  string
	scopePrimary              *smartMember
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
	d.ticks = 0
	d.flows = 0
	d.provisional = 0
}

func (s *Smart) degradationValidLocked(e *smartDegradationEpisode, now time.Time) bool {
	return e != nil && s.degradation.episode == e && e.ctx.Err() == nil && !s.closed && !s.paused && s.network.Link && s.primary == e.primary && s.primaryEpoch == e.epoch && s.network.Key == e.key && s.generation == e.generation && s.configGeneration == e.configGeneration && e.primary.generation == e.memberGeneration && e.primary.state == "healthy" && now.Before(e.until)
}

func (s *Smart) degradationCandidateLocked(now time.Time) *smartMember {
	var candidate *smartMember
	for _, member := range s.members {
		if !s.demandChallengerEligibleLocked(member, now) {
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
			if e.challenger == nil || e.challenger.generation != e.challengerGeneration || !s.demandChallengerEligibleLocked(e.challenger, now) {
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
	if s.primary == nil || s.primary.state != "healthy" || !s.throughputAllowedLocked(now) {
		return false
	}
	if d.scopeKey != s.network.Key || d.scopePrimary != s.primary {
		d.scopeKey = s.network.Key
		d.scopePrimary = s.primary
		d.retry = time.Time{}
		d.window = time.Time{}
	}
	if !d.sampled.IsZero() && now.Sub(d.sampled) < time.Second {
		return false
	}
	total := s.primary.passiveRead
	flows := 0
	for lease := range s.leases[s.primary] {
		if lease.generation == s.generation && now.Sub(lease.started) >= 10*time.Second && now.Sub(lease.lastRead) <= 2*time.Second {
			flows++
		}
	}
	if d.window.IsZero() || now.Sub(d.sampled) > 2*time.Second || now.Sub(d.progress) > 2*time.Second || flows == 0 {
		d.window, d.sampled, d.progress = now, now, now
		d.baseline = total
		d.previous = total
		d.ticks = 0
		d.flows = flows
		return false
	}
	elapsed := now.Sub(d.window)
	d.observed = float64(total-d.previous) * 8 / now.Sub(d.sampled).Seconds() / 1e6
	if total > d.previous {
		d.ticks++
		d.progress = now
	}
	d.previous = total
	if d.observed >= s.targetMbps*.5 {
		d.window = now
		d.baseline = total
		d.ticks = 0
		d.sampled = now
		d.state = "idle"
		return false
	}
	d.sampled = now
	d.flows = flows
	if elapsed < 20*time.Second {
		d.state = "observing"
		return false
	}
	qualified := d.ticks >= 18 && now.Sub(d.progress) <= 2*time.Second && total-d.baseline >= 256*1024 && d.observed < s.targetMbps*.5
	d.window = time.Time{}
	if !qualified {
		d.state = "idle"
		return false
	}
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
			s.finishDegradationLocked(now, time.Hour, "primary_fast")
			return
		}
		if result.download <= 0 || result.download >= s.targetMbps*.5 {
			s.finishDegradationLocked(now, 30*time.Minute, "inconclusive")
			return
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
	if result.purpose != smartDegradationChallenger || e.challenger != result.member || result.member.generation != e.challengerGeneration || !s.demandChallengerEligibleLocked(result.member, now) {
		s.finishDegradationLocked(now, 30*time.Minute, "ineligible")
		return
	}
	if result.download < s.targetMbps {
		s.finishDegradationLocked(now, 30*time.Minute, "shared_bottleneck_suspected")
		return
	}
	e.primary.throughput = smartThroughput{Download: e.confirmed, At: e.checked, Source: "active"}
	result.member.throughput = smartThroughput{Download: result.download, At: now, Source: "active"}
	for _, member := range []*smartMember{e.primary, result.member} {
		member.lastThroughput = now
		for _, key := range s.network.Path {
			stat := s.history.statistic(key, member.identity, now)
			stat.Download = member.throughput.Download
			stat.ThroughputAt = now.Unix()
		}
	}
	s.eventLocked("speedtest", "", result.member.tag, s.network.Key, "fresh degradation pair", now)
	s.switchLocked(result.member, "switch", "demand challenger meets target", now)
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
	return s.throughputAllowedLocked(s.clock.Now()) && s.degradationValidLocked(job.episode, s.clock.Now()) && (job.purpose != smartDegradationChallenger || job.member == job.episode.challenger && job.member.generation == job.episode.challengerGeneration && s.demandChallengerEligibleLocked(job.member, s.clock.Now()))
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
