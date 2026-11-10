package group

import (
	"io"
	"testing"
	"time"
)

func TestSmartDisableDirectAttributesMemberFailures(t *testing.T) {
	s, clock := smartTestGroup()
	defer s.cancel()
	s.options.Probe.DisableDirect = true
	s.generationSucceeded = false
	s.direct = smartEvidence{state: "failed", at: clock.now, expires: clock.now.Add(time.Minute)}
	s.tickLocked(clock.now)
	member := s.primary
	failure := smartResult{member: member, generation: s.generation, memberGeneration: member.generation, configGeneration: s.configGeneration, kind: "tcp", err: io.ErrUnexpectedEOF}
	s.applyResultLocked(failure, clock.now)
	failure.confirm = true
	s.applyResultLocked(failure, clock.now.Add(time.Second))
	if member.state != "quarantined" || member.lastFail.IsZero() || s.offline {
		t.Fatalf("disabled direct comparison deferred member failure: state=%s last_fail=%v offline=%v", member.state, member.lastFail, s.offline)
	}
	for _, job := range s.pending {
		if job.kind == "direct" {
			t.Fatal("disabled direct probe was scheduled")
		}
	}
	if s.directInflight {
		t.Fatal("disabled direct probe is in flight")
	}
	for _, member := range s.members {
		member.lastFail = clock.now.Add(time.Second)
	}
	s.direct = smartEvidence{state: "ok", at: clock.now, expires: clock.now.Add(time.Minute)}
	s.updateNotificationLocked(clock.now.Add(time.Second))
	if !s.allDeadSince.IsZero() {
		t.Fatal("disabled direct comparison enabled an all-dead notification")
	}
}
