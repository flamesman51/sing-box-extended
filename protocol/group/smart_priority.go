package group

import (
	"context"
	"time"
)

func (s *Smart) preemptBackgroundLocked(job smartJob) {
	if job.kind != "tcp" || !job.urgent || job.member == nil || job.member != s.primary {
		return
	}
	if s.backgroundCancel != nil {
		s.backgroundCancel()
	}
	ctx := s.workCtx
	if ctx == nil {
		ctx = s.ctx
	}
	s.backgroundCtx, s.backgroundCancel = context.WithCancel(ctx)
	s.lastDiscovery = time.Time{}
}
