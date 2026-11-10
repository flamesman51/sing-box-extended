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
	if !s.throughputAllowedLocked(now) || s.admissionRetryNetwork == s.network.Type && now.Before(s.admissionRetryAt) {
		return
	}
	ctx := s.workCtx
	if ctx == nil {
		ctx = s.ctx
	}
	if episode != nil {
		ctx = episode.ctx
	}
	admission := &smartAdmission{
		job: smartJob{
			member: member, kind: "throughput", generation: s.generation,
			memberGeneration: member.generation, configGeneration: s.configGeneration,
			ctx: ctx, networkType: s.network.Type, timeout: 30 * time.Second,
			episode: episode, purpose: purpose,
		},
		ack: make(chan bool, 1),
	}
	s.admission = admission
	ledger, networkType, offset := s.ledger, s.network.Type, s.network.Offset
	s.admissionWorkers.Go(func() {
		id, capBytes, err := func() (id string, capBytes int64, err error) {
			defer s.recoverError("ledger-admission", &err)
			s.injectPanic("ledger-admission")
			if episode != nil {
				return ledger.ReserveDegradation(context.WithValue(ctx, smartContainmentKey{}, s), networkType, now, offset, purpose, smartDegradationConfirmationCap(s.targetMbps))
			}
			return ledger.Reserve(context.WithValue(ctx, smartContainmentKey{}, s), networkType, now, offset)
		}()
		admission.job.reservation, admission.job.capBytes = id, capBytes
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
		if !accepted && id != "" {
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
	if result.err != nil && job.generation == s.generation && job.configGeneration == s.configGeneration && (job.episode == nil || s.degradationValidLocked(job.episode, now)) {
		delay := 30 * time.Second
		switch {
		case errors.Is(result.err, errSmartTestBusy):
			delay = 5 * time.Second
		case errors.Is(result.err, errSmartTestRate), errors.Is(result.err, errSmartTestsDisabled):
			delay = time.Minute
		case errors.Is(result.err, errSmartBudget):
			local := now.In(time.FixedZone("smart-local", s.network.Offset))
			year, month, day := local.Date()
			delay = time.Date(year, month, day+1, 0, 0, 0, 0, local.Location()).Sub(now)
		case errors.Is(result.err, context.Canceled):
			delay = time.Second
		}
		if job.episode != nil && errors.Is(result.err, errSmartDegradationBudget) {
			s.degradation.retry, s.degradation.blocked = now.Add(time.Hour), "degradation_allowance"
		} else {
			s.admissionRetryAt, s.admissionRetryNetwork = now.Add(delay), job.networkType
		}
	}
	accepted := false
	eligible := result.err == nil && s.throughputAllowedLocked(now) && job.member.state == "healthy"
	if eligible && job.episode == nil {
		for _, entries := range s.leases {
			for lease := range entries {
				if now.Sub(lease.lastRead) < 2*time.Second || now.Sub(lease.lastWrite) < 2*time.Second {
					eligible = false
				}
			}
		}
	}
	if job.episode != nil {
		eligible = eligible && s.degradationValidLocked(job.episode, now)
		if job.purpose == smartDegradationChallenger {
			eligible = eligible && job.member == job.episode.challenger && job.member.generation == job.episode.challengerGeneration && s.demandChallengerEligibleLocked(job.member, now) && !s.degradationChallengerRetryLocked().After(now)
		}
	}
	if eligible && job.ctx.Err() == nil && job.generation == s.generation && job.configGeneration == s.configGeneration &&
		job.memberGeneration == job.member.generation && slices.Contains(s.members, job.member) {
		s.queueLocked(job.member, "throughput", 0, false, now)
		for index := len(s.pending) - 1; index >= 0; index-- {
			pending := &s.pending[index]
			if pending.member == job.member && pending.kind == "throughput" && pending.reservation == "" {
				pending.reservation, pending.capBytes, pending.timeout = job.reservation, job.capBytes, job.timeout
				if job.episode != nil {
					pending.ctx, pending.episode, pending.purpose = job.ctx, job.episode, job.purpose
					job.member.degradationQueued = job.episode
					s.degradation.state = "checking"
					if job.purpose == smartDegradationChallenger {
						if s.degradation.attempts == nil {
							s.degradation.attempts = make(map[string]time.Time)
						}
						s.degradation.attempts[job.member.identity] = now
						s.lastChallenger = now
						s.persistDegradationCooldownLocked(s.degradationChallengerCooldownKey(), now, now.Add(time.Hour))
					}
				}
				if job.member != s.primary && job.member != s.standby {
					s.lastChallenger = now
				}
				accepted = true
				break
			}
		}
	}
	if !accepted && job.episode != nil && s.degradation.episode == job.episode {
		if result.contained || errors.Is(result.err, context.Canceled) || !s.degradationValidLocked(job.episode, now) {
			s.finishDegradationLocked(now, 30*time.Minute, "no_data")
		} else if result.err != nil && !errors.Is(result.err, errSmartBudget) && !errors.Is(result.err, errSmartDegradationBudget) {
			s.degradation.retry = s.admissionRetryAt
		} else {
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
			if job.kind == "throughput" {
				s.discardReservation(job.reservation)
			}
		default:
			return
		}
	}
}
