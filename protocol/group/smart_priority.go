package group

import "context"

func (s *Smart) preemptBackgroundLocked(job smartJob) {
	if job.kind != "tcp" || !job.urgent || job.member == nil || job.member != s.primary {
		return
	}
	if s.backgroundCancel != nil {
		for _, member := range s.members {
			if member.queued["udp443"] {
				member.discoveryPreempted = true
			}
		}
		s.backgroundCancel()
	}
	ctx := s.workCtx
	if ctx == nil {
		ctx = s.ctx
	}
	s.backgroundCtx, s.backgroundCancel = context.WithCancel(ctx)
}
