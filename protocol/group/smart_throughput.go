package group

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/service"
)

const smartShutdownTimeout = 5 * time.Second

const smartImprovementConfirmation = "improvement_confirmation"

func (s *Smart) improvementConfirmationEligibleLocked(member *smartMember, now time.Time) bool {
	if !s.profileEngine() || !s.preference.improvement || member == nil || member != s.improvementCandidate ||
		member.improvementObservations < 1 || member.improvementObservations >= s.preference.confirmations ||
		(member.improvementCriterion != "speed_floor" && member.improvementCriterion != "download" && member.improvementCriterion != "throughput" &&
			member.improvementCriterion != "upload" && member.improvementCriterion != "rtt" && member.improvementCriterion != "jitter") ||
		now.Sub(member.improvementAt) < s.preference.confirmationInterval ||
		s.primary == nil || member == s.primary || member.state != "healthy" || member.tier != s.primary.tier {
		return false
	}
	if !s.profileCriterionFreshLocked(member, member.improvementCriterion, now) {
		return false
	}
	better, criterion := s.betterLocked(member, s.primary, now)
	return better && criterion == member.improvementCriterion
}

func (s *Smart) improvementConfirmationJobValidLocked(member *smartMember, primaryEpoch, improvementEpoch uint64, now time.Time) bool {
	if !s.profileEngine() || !s.preference.improvement || member == nil || member != s.improvementCandidate ||
		primaryEpoch != s.primaryEpoch || improvementEpoch != s.improvementEpoch ||
		s.primary == nil || member == s.primary || member.state != "healthy" || member.tier != s.primary.tier {
		return false
	}
	if !s.profileCriterionFreshLocked(member, member.improvementCriterion, now) {
		return false
	}
	better, criterion := s.betterLocked(member, s.primary, now)
	return better && criterion == member.improvementCriterion
}

func (s *Smart) scheduleImprovementConfirmationLocked(now time.Time) bool {
	member := s.improvementCandidate
	plan := s.smartConfirmationPlanLocked(member)
	if !s.improvementConfirmationEligibleLocked(member, now) || !s.measurementPlanAllowedLocked(plan, now) ||
		s.degradation.episode != nil || s.admission != nil && s.admission.job.ctx.Err() == nil {
		return false
	}
	for _, candidate := range s.members {
		if candidate.queued["throughput"] || plan.STUNBursts > 0 && candidate == member && candidate.queued["udp"] {
			return false
		}
	}
	if plan.STUNBursts == 0 {
		for _, entries := range s.leases {
			for lease := range entries {
				if now.Sub(lease.lastRead) < 2*time.Second || now.Sub(lease.lastWrite) < 2*time.Second {
					return false
				}
			}
		}
	}
	s.startThroughputPurposeAdmissionLocked(member, smartImprovementConfirmation, nil, now)
	return s.admission != nil
}

type smartAdmission struct {
	job smartJob
	ack chan bool
}

func (s *Smart) startThroughputAdmissionLocked(member *smartMember, now time.Time) {
	s.startThroughputPurposeAdmissionLocked(member, "", nil, now)
}

func (s *Smart) startDegradationAdmissionLocked(member *smartMember, purpose string, episode *smartDegradationEpisode, now time.Time) {
	if !s.degradationValidLocked(episode, now) {
		return
	}
	s.startThroughputPurposeAdmissionLocked(member, purpose, episode, now)
}

func (s *Smart) startThroughputPurposeAdmissionLocked(member *smartMember, purpose string, episode *smartDegradationEpisode, now time.Time) {
	if s.admission != nil && s.admission.job.ctx.Err() == nil {
		return
	}
	plan := s.regularMeasurementPlanLocked(member, now)
	if purpose == smartImprovementConfirmation {
		plan = s.smartConfirmationPlanLocked(member)
	}
	if episode != nil {
		plan = episode.plan
	}
	if plan.Download == nil && plan.Upload == nil && plan.STUNBursts == 0 ||
		!s.measurementPlanAllowedLocked(plan, now) ||
		s.admissionRetryNetwork == s.network.Type && now.Before(s.admissionRetryAt) ||
		plan.STUNBursts == 0 && s.measurementPlanBackedOffLocked(member, plan, now) {
		return
	}
	ctx := s.measurementContextLocked(member, plan)
	if purpose == smartImprovementConfirmation {
		member.confirmationCancel = member.measurementCancel
	}
	if episode != nil {
		ctx = episode.ctx
	}
	kind := "throughput"
	if plan.STUNBursts > 0 {
		kind = "udp"
	}
	admission := &smartAdmission{job: smartJob{
		member: member, kind: kind, generation: s.generation,
		memberGeneration: member.generation, configGeneration: s.configGeneration,
		ctx: ctx, networkType: s.network.Type, timeout: plan.Deadline(),
		episode: episode, purpose: purpose, plan: plan,
		primaryEpoch: s.primaryEpoch, improvementEpoch: s.improvementEpoch,
	}, ack: make(chan bool, 1)}
	s.admission = admission
	estimate := s.measurementEstimateLocked(member, plan, episode)
	ledger, networkType, offset := s.ledger, s.network.Type, s.network.Offset
	s.admissionWorkers.Go(func() {
		id, capBytes, err := func() (id string, capBytes int64, err error) {
			defer s.recoverError("ledger-admission", &err)
			s.injectPanic("ledger-admission")
			if plan.STUNBursts > 0 {
				return "", 0, ctx.Err()
			}
			return ledger.ReservePlan(context.WithValue(ctx, smartContainmentKey{}, s), networkType, now, offset, estimate)
		}()
		admission.job.reservation, admission.job.capBytes = id, capBytes
		if id != "" {
			admission.job.meter = newSmartMeasureMeter(capBytes)
		}
		result := smartResult{kind: "admission", admission: admission, err: err, contained: errors.Is(err, errSmartContained)}
		accepted := false
		select {
		case s.results <- result:
			select {
			case accepted = <-admission.ack:
			case <-ctx.Done():
				func() {
					s.mu.Lock()
					defer s.mu.Unlock()
					select {
					case accepted = <-admission.ack:
					default:
					}
				}()
			}
		case <-ctx.Done():
		}
		if !accepted {
			s.discardReservation(id)
		}
	})
}

func (s *Smart) applyAdmissionLocked(result smartResult, now time.Time) {
	admission := result.admission
	if s.admission == admission {
		s.admission = nil
	}
	job := admission.job
	current := job.generation == s.generation && job.configGeneration == s.configGeneration &&
		(job.episode == nil || s.degradationValidLocked(job.episode, now)) &&
		(job.purpose != smartImprovementConfirmation || s.improvementConfirmationJobValidLocked(job.member, job.primaryEpoch, job.improvementEpoch, now))
	if result.err != nil && current {
		if errors.Is(result.err, errSmartBudget) {
			s.backoffMeasurementPlanLocked(job.member, job.plan, now)
		} else {
			delay := 30 * time.Second
			switch {
			case errors.Is(result.err, errSmartTestBusy):
				delay = 5 * time.Second
			case errors.Is(result.err, errSmartTestRate), errors.Is(result.err, errSmartTestsDisabled):
				delay = time.Minute
			case errors.Is(result.err, context.Canceled), errors.Is(result.err, context.DeadlineExceeded):
				delay = time.Second
			}
			s.admissionRetryAt, s.admissionRetryNetwork = now.Add(delay), job.networkType
		}
	}
	eligible := current && result.err == nil && s.measurementPlanAllowedLocked(job.plan, now) && job.member.state == "healthy"
	if eligible && job.episode == nil && job.plan.STUNBursts == 0 {
		eligible = s.measurementIdleLocked(now)
	}
	if job.purpose == smartImprovementConfirmation {
		eligible = eligible && s.degradation.episode == nil
	}
	accepted := false
	if eligible && job.ctx.Err() == nil && job.memberGeneration == job.member.generation && slices.Contains(s.members, job.member) {
		s.queueLocked(job.member, job.kind, 0, false, now)
		for index := len(s.pending) - 1; index >= 0; index-- {
			pending := &s.pending[index]
			if pending.member == job.member && pending.kind == job.kind && pending.reservation == "" {
				job.urgent = pending.urgent
				*pending = job
				if job.episode != nil {
					episode := job.episode
					episode.reservation, episode.capBytes, episode.meter = job.reservation, job.capBytes, job.meter
					episode.accountingWait.Add(1)
					job.member.degradationQueued = episode
					s.degradation.state = "checking"
				}
				if job.kind == "throughput" {
					job.member.lastThroughput = now
				}
				accepted = true
				break
			}
		}
	}
	if !accepted {
		s.cancelMemberMeasurementLocked(job.member)
		if job.episode != nil && s.degradation.episode == job.episode {
			s.finishDegradationLocked(now, 30*time.Minute, "admission_blocked")
		}
	}
	admission.ack <- accepted
}

func (s *Smart) throughputAllowedLocked(now time.Time) bool {
	if !s.throughputEnabled || s.ledger == nil || s.ledger.cache == nil || s.closed || s.paused || !s.network.Link || s.network.PowerSave || s.primary == nil || !s.generationSucceeded {
		return false
	}
	if s.pauseManager != nil && s.pauseManager.IsPaused() {
		return false
	}
	if platform := service.FromContext[adapter.PlatformInterface](s.ctx); platform != nil && platform.IsPowerSaveMode() {
		return false
	}
	return s.ledger.Snapshot(s.network.Type, now, s.network.Offset).ActiveTests
}

func (s *Smart) reconcileReservation(id string, bytes int64, uncertain bool) (err error) {
	defer s.recoverError("ledger-accounting", &err)
	ctx, cancel := context.WithTimeout(context.Background(), smartShutdownTimeout)
	defer cancel()
	return s.ledger.Reconcile(context.WithValue(ctx, smartContainmentKey{}, s), id, bytes, uncertain)
}

func (s *Smart) discardReservation(id string) {
	if id == "" || s.ledger == nil {
		return
	}
	s.accountingWorkers.Go(func() {
		if err := s.reconcileReservation(id, 0, false); err != nil && s.logger != nil {
			s.logger.Error("reconcile unstarted smart throughput test: ", err)
		}
	})
}

func (s *Smart) discardBufferedJobsLocked() {
	for {
		select {
		case job := <-s.otherJobs:
			if job.episode != nil {
				job.episode.accountingWait.Done()
			} else if job.kind == "throughput" {
				s.discardReservation(job.reservation)
			}
		default:
			return
		}
	}
}
