package group

import (
	"context"
	"net"
	"time"
)

func smartProbeTimeout(err error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		found := false
		for _, cause := range joined.Unwrap() {
			if cause == errSmartResolve {
				continue
			}
			if !smartProbeTimeout(cause) {
				return false
			}
			found = true
		}
		return found
	}
	if err == context.DeadlineExceeded {
		return true
	}
	// quic-go timeout errors report Timeout() but unwrap to net.ErrClosed.
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return smartProbeTimeout(wrapped.Unwrap())
	}
	return false
}

func (s *Smart) measurementGateClosedLocked() bool {
	return s.primary != nil && (s.measurementPrimaryGate == s.primary || s.primary.state == "suspect")
}

func (s *Smart) openMeasurementPrimaryGateLocked(now time.Time) {
	if s.primary == nil {
		return
	}
	s.measurementPrimaryGate = s.primary
	if s.measurementPrimaryTimeout.IsZero() {
		s.measurementPrimaryTimeout = now
	}
	s.wakeMeasurementLoadLocked()
	s.checkMeasurementPrimaryLocked(now)
}

func (s *Smart) checkMeasurementPrimaryLocked(now time.Time) {
	if s.measurementPrimaryGate != s.primary {
		s.measurementPrimaryGate, s.measurementPrimaryTimeout = nil, time.Time{}
	}
	if !s.measurementGateClosedLocked() {
		return
	}
	if len(s.measurementLoad.executing) > 0 {
		if !s.measurementPrimaryTimeout.IsZero() && now.Sub(s.measurementPrimaryTimeout) >= 10*time.Second {
			for _, cancel := range s.measurementLoad.executing {
				cancel()
			}
		}
		return
	}
	if !s.primary.inflight {
		operator := int(s.generation % 3)
		if s.primary.state == "suspect" {
			operator = s.primary.suspectOperator
		}
		s.queueLocked(s.primary, "tcp", operator, s.primary.state == "suspect", now)
	}
}

func smartMeasurementDataContext(ctx context.Context) (context.Context, func(), error) {
	s, _ := ctx.Value(smartContainmentKey{}).(*Smart)
	if s == nil {
		return ctx, func() {}, nil
	}
	lease, err := s.acquireMeasurementLease(ctx, false)
	if err != nil {
		return ctx, func() {}, err
	}
	return lease.context(), lease.release, nil
}
