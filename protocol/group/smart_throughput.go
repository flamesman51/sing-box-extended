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
	admission := &smartAdmission{
		job: smartJob{
			member: member, kind: "throughput", generation: s.generation,
			memberGeneration: member.generation, configGeneration: s.configGeneration,
			ctx: ctx, networkType: s.network.Type, timeout: 30 * time.Second,
		},
		ack: make(chan bool, 1),
	}
	s.admission = admission
	ledger, networkType, offset := s.ledger, s.network.Type, s.network.Offset
	s.admissionWorkers.Go(func() {
		id, capBytes, err := func() (id string, capBytes int64, err error) {
			defer s.recoverError("ledger-admission", &err)
			s.injectPanic("ledger-admission")
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
	if result.err != nil && job.generation == s.generation && job.configGeneration == s.configGeneration {
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
		s.admissionRetryAt, s.admissionRetryNetwork = now.Add(delay), job.networkType
	}
	accepted := false
	eligible := result.err == nil && s.throughputAllowedLocked(now) && job.member.state == "healthy"
	if eligible {
		for _, entries := range s.leases {
			for lease := range entries {
				if now.Sub(lease.lastRead) < 2*time.Second || now.Sub(lease.lastWrite) < 2*time.Second {
					eligible = false
				}
			}
		}
	}
	if eligible && job.ctx.Err() == nil && job.generation == s.generation && job.configGeneration == s.configGeneration &&
		job.memberGeneration == job.member.generation && slices.Contains(s.members, job.member) {
		s.queueLocked(job.member, "throughput", 0, false, now)
		for index := len(s.pending) - 1; index >= 0; index-- {
			pending := &s.pending[index]
			if pending.member == job.member && pending.kind == "throughput" && pending.reservation == "" {
				pending.reservation, pending.capBytes, pending.timeout = job.reservation, job.capBytes, job.timeout
				if job.member != s.primary && job.member != s.standby {
					s.lastChallenger = now
				}
				accepted = true
				break
			}
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
	s.injectPanic("ledger-accounting")
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
