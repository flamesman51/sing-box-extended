package group

import (
	"context"
	"slices"
	"time"
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
	s.admissionWorkers.Add(1)
	go func() {
		defer s.admissionWorkers.Done()
		id, capBytes, err := ledger.Reserve(ctx, networkType, now, offset)
		admission.job.reservation, admission.job.capBytes = id, capBytes
		result := smartResult{kind: "admission", admission: admission, err: err}
		accepted := false
		select {
		case s.results <- result:
			select {
			case accepted = <-admission.ack:
			case <-ctx.Done():
				s.mu.Lock()
				select {
				case accepted = <-admission.ack:
				default:
				}
				s.mu.Unlock()
			}
		case <-ctx.Done():
		}
		if !accepted && id != "" {
			s.discardReservation(id)
		}
	}()
}

func (s *Smart) applyAdmissionLocked(result smartResult, now time.Time) {
	admission := result.admission
	if s.admission == admission {
		s.admission = nil
	}
	job := admission.job
	accepted := false
	if result.err == nil && !s.closed && !s.paused && job.ctx.Err() == nil &&
		job.generation == s.generation && job.configGeneration == s.configGeneration &&
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

func (s *Smart) reconcileReservation(id string, bytes int64, uncertain bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), smartShutdownTimeout)
	defer cancel()
	return s.ledger.Reconcile(ctx, id, bytes, uncertain)
}

func (s *Smart) discardReservation(id string) {
	if id == "" || s.ledger == nil {
		return
	}
	s.accountingWorkers.Add(1)
	go func() {
		defer s.accountingWorkers.Done()
		if err := s.reconcileReservation(id, 0, false); err != nil && s.logger != nil {
			s.logger.Error("reconcile unstarted smart throughput test: ", err)
		}
	}()
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
