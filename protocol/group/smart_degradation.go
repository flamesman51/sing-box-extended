package group

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const (
	smartDegradationConfirmation = "degradation-confirmation"
	smartDegradationChallenger   = "degradation-challenger"
	// Cover TCP RTO backoff without treating long idle periods as demand.
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
	plan                                                                        smartMeasurementPlan
	meter                                                                       *smartMeasureMeter
	reservation                                                                 string
	capBytes                                                                    int64
	primaryResult, challengerResult                                             smartDirectionResult
	accountingWait                                                              sync.WaitGroup
	accountingOnce                                                              sync.Once
	accountingPanic                                                             atomic.Bool
}

type smartDegradationState struct {
	episode               *smartDegradationEpisode
	sampled, window       time.Time
	coverage              smartDegradationCoverage
	lastDemandRead        int64
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
		s.finishEpisodeAccountingLocked(e)
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
	d.window, d.sampled = time.Time{}, time.Time{}
	d.coverage = smartDegradationCoverage{}
	d.lastDemandRead, d.flows, d.provisional = 0, 0, 0
}

func (s *Smart) degradationValidLocked(e *smartDegradationEpisode, now time.Time) bool {
	return e != nil && s.degradation.episode == e && e.ctx.Err() == nil && !s.closed && !s.paused && !s.network.PowerSave && s.network.Link && s.primary == e.primary && s.primaryEpoch == e.epoch && s.network.Key == e.key && s.generation == e.generation && s.configGeneration == e.configGeneration && e.primary.generation == e.memberGeneration && e.primary.state == "healthy" && now.Before(e.until)
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
		if e.primaryResult.Valid {
			if e.challenger == nil || e.challenger.generation != e.challengerGeneration || !s.degradationProbeEligibleLocked(e.challenger, now) {
				s.finishDegradationLocked(now, 30*time.Minute, "ineligible")
				return false
			}
			if !e.challenger.queued["throughput"] {
				s.queueDegradationChallengerLocked(e, now)
			}
		} else if !e.primary.queued["throughput"] && (s.admission == nil || s.admission.job.episode != e) && !now.Before(d.retry) {
			s.startDegradationAdmissionLocked(e.primary, smartDegradationConfirmation, e, now)
		}
		return true
	}
	if s.primary == nil || s.primary.state != "healthy" || s.degradationThresholdLocked() <= 0 || !s.throughputAllowedLocked(now) {
		return false
	}
	if d.scopeKey != s.network.Key || d.scopePrimary != s.primary {
		d.scopeKey, d.scopePrimary = s.network.Key, s.primary
		d.retry, d.window, d.sampled = time.Time{}, time.Time{}, time.Time{}
		d.coverage = smartDegradationCoverage{}
	}
	flows := 0
	var lastRead time.Time
	for lease := range s.leases[s.primary] {
		if lease.generation == s.generation && now.Sub(lease.started) >= 10*time.Second {
			flows++
			if lease.lastRead.After(lastRead) {
				lastRead = lease.lastRead
			}
		}
	}
	d.flows = flows
	if flows == 0 || now.Sub(lastRead) > smartDegradationMaxStall {
		d.window, d.sampled = time.Time{}, time.Time{}
		d.coverage = smartDegradationCoverage{}
		return false
	}
	if s.primary.passiveBins == nil {
		s.primary.passiveBins = new(smartPassiveBins)
	}
	bins := s.primary.passiveBins
	bins.advance(now)
	if d.sampled.IsZero() {
		d.sampled, d.window = now.Truncate(time.Second), now.Truncate(time.Second)
		d.lastDemandRead = now.Unix()
		return false
	}
	qualified := false
	for second := max(d.sampled.Unix(), now.Unix()-smartPassiveBinCount+1); second < now.Unix(); second++ {
		read, _ := bins.bytes(second)
		if read > 0 {
			d.lastDemandRead = second
		}
		d.observed = float64(read) * 8 / 1e6
		if d.observed >= s.degradationThresholdLocked() || second-d.lastDemandRead > int64(smartDegradationMaxStall/time.Second) {
			d.coverage = smartDegradationCoverage{}
			d.window = time.Unix(second+1, 0)
			qualified = false
			continue
		}
		d.coverage.add(time.Second, read)
		qualified = d.coverage.duration >= 20*time.Second && d.coverage.bytes >= 128*1024
	}
	d.sampled = now.Truncate(time.Second)
	if !qualified {
		d.state = "observing"
		return false
	}
	key := s.degradationKey(s.primary)
	retry := d.cooldowns[key]
	if persisted := s.ledger.DegradationCooldown(key); persisted.After(retry) {
		retry = persisted
	}
	if retry.After(now) {
		d.retry, d.state = retry, "cooldown"
		return false
	}
	if d.retry.After(now) {
		return false
	}
	challenger := s.degradationCandidateLocked(now)
	if challenger == nil {
		d.state, d.blocked = "no_challenger", "no_challenger"
		return false
	}
	plan := s.downloadMeasurementPlanLocked()
	plan.DownloadPhases = 2
	ctx := s.workCtx
	if ctx == nil {
		ctx = s.ctx
	}
	ctx, cancel := context.WithTimeout(ctx, plan.Deadline())
	e := &smartDegradationEpisode{primary: s.primary, challenger: challenger, key: s.network.Key, generation: s.generation, configGeneration: s.configGeneration, memberGeneration: s.primary.generation, challengerGeneration: challenger.generation, epoch: s.primaryEpoch, ctx: ctx, cancel: cancel, until: now.Add(plan.Deadline()), plan: plan}
	d.episode, d.state, d.blocked, d.retry = e, "queued", "", time.Time{}
	s.startDegradationAdmissionLocked(s.primary, smartDegradationConfirmation, e, now)
	return true
}

func (s *Smart) finishDegradationLocked(now time.Time, cooldown time.Duration, reason string) {
	d := &s.degradation
	e := d.episode
	if e == nil {
		return
	}
	key, until := s.degradationKey(e.primary), now.Add(cooldown)
	if d.cooldowns == nil {
		d.cooldowns = make(map[string]time.Time)
	}
	for old, expiry := range d.cooldowns {
		if !expiry.After(now) {
			delete(d.cooldowns, old)
		}
	}
	d.cooldowns[key], d.retry, d.state, d.blocked = until, until, reason, reason
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
		s.finishDegradationLocked(now, 30*time.Minute, "inconclusive")
		return
	}
	measurement := result.measurement.Download
	if !measurement.Valid || !smartFinite(measurement.Value) || measurement.Value < 0 {
		if result.purpose == smartDegradationChallenger && e.primaryResult.Valid && result.member == e.challenger && result.member.generation == e.challengerGeneration && s.degradationProbeEligibleLocked(result.member, now) {
			s.commitDegradationEvidenceLocked(e, now)
		}
		s.finishDegradationLocked(now, 30*time.Minute, "inconclusive")
		return
	}
	if measurement.At.IsZero() {
		measurement.At = now
	}
	d := &s.degradation
	d.checked = now
	if result.purpose == smartDegradationConfirmation {
		if result.member != e.primary || e.primaryResult.Valid {
			s.finishDegradationLocked(now, 30*time.Minute, "invalidated")
			return
		}
		e.primaryResult, e.confirmed, e.checked = measurement, measurement.Value, measurement.At
		fastThreshold := s.targetMbps
		if s.profileEngine() {
			fastThreshold = s.degradationThresholdLocked()
		}
		if measurement.Value >= fastThreshold {
			s.commitDegradationEvidenceLocked(e, now)
			s.finishDegradationLocked(now, time.Hour, "primary_fast")
			return
		}
		// Legacy's hysteresis-band result was inconclusive, not a slow confirmation.
		if !s.profileEngine() && (measurement.Value <= 0 || measurement.Value >= s.degradationThresholdLocked()) {
			s.commitDegradationEvidenceLocked(e, now)
			s.finishDegradationLocked(now, 30*time.Minute, "inconclusive")
			return
		}
		d.provisional, d.state = measurement.Value, "confirmed"
		if e.challenger == nil || e.challenger.generation != e.challengerGeneration || !s.degradationProbeEligibleLocked(e.challenger, now) {
			s.finishDegradationLocked(now, 30*time.Minute, "ineligible")
			return
		}
		s.queueDegradationChallengerLocked(e, now)
		return
	}
	if result.purpose != smartDegradationChallenger || !e.primaryResult.Valid || e.challenger != result.member || result.member.generation != e.challengerGeneration || !s.degradationProbeEligibleLocked(result.member, now) {
		s.finishDegradationLocked(now, 30*time.Minute, "ineligible")
		return
	}
	e.challengerResult = measurement
	s.commitDegradationEvidenceLocked(e, now)
	if measurement.Value < s.targetMbps {
		reason := "challenger_slow"
		if !s.profileEngine() {
			reason = "shared_bottleneck_suspected"
		}
		s.finishDegradationLocked(now, 30*time.Minute, reason)
		return
	}
	reason := "demand challenger meets target"
	if s.profileEngine() {
		if !slices.Contains(s.preference.rank, "speed_floor") {
			s.finishDegradationLocked(now, 30*time.Minute, "no_floor_criterion")
			return
		}
		better, criterion := s.betterLocked(result.member, e.primary, now)
		if !s.preference.improvement || !better || criterion != "speed_floor" {
			s.finishDegradationLocked(now, 30*time.Minute, "challenger_slow")
			return
		}
		reason = "confirmed degradation improvement: speed_floor"
	}
	s.finishDegradationLocked(now, 30*time.Minute, "switched")
	s.switchLocked(result.member, "switch", reason, now)
	d.state, d.blocked, d.provisional = "switched", "", 0
}

func (s *Smart) degradationSnapshotLocked() map[string]any {
	d := &s.degradation
	return map[string]any{"state": d.state, "observed_mbps": smartNumber(d.observed), "provisional_mbps": smartNumber(d.provisional), "continuous_flows": d.flows, "checked_at": smartTime(d.checked), "retry_at": smartTime(d.retry), "blocked_reason": smartNullable(d.blocked)}
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
	samples     [21]smartDegradationSample
	head, count int
	duration    time.Duration
	bytes       float64
}

func (c *smartDegradationCoverage) add(duration time.Duration, bytes int64) {
	if c.count == 20 {
		oldest := c.samples[c.head]
		c.duration -= oldest.duration
		c.bytes -= oldest.bytes
		c.head = (c.head + 1) % len(c.samples)
		c.count--
	}
	c.samples[(c.head+c.count)%len(c.samples)] = smartDegradationSample{duration: duration, bytes: float64(bytes)}
	c.count++
	c.duration += duration
	c.bytes += float64(bytes)
}
