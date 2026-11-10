package group

import (
	"context"
	"testing"
	"time"
)

func TestSmartProfileQUICFixedRefreshRetriesPreemption(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "responsive")
	s.preference.rank = []string{"quic"}
	s.preference.confirmations, s.preference.dwell = 3, time.Hour
	s.selectedAt = clock.Now()
	primary, candidate := s.primary, s.standby
	primary.udp443 = smartEvidence{state: "failed", at: clock.Now(), expires: clock.Now().Add(time.Hour)}
	candidate.udp443 = smartEvidence{state: "ok", at: clock.Now(), expires: clock.Now().Add(10 * time.Minute)}
	s.chooseLocked(clock.Now())
	s.backgroundCtx, s.backgroundCancel = context.WithCancel(s.ctx)
	clock.Advance(8 * time.Minute)
	s.queueProfileUDPLocked(clock.Now(), false)
	if len(s.pending) != 1 || s.pending[0].member != candidate {
		t.Fatal("fixed refresh was not queued")
	}
	job := s.pending[0]
	s.pending = nil
	s.queueLocked(primary, "tcp", 0, false, clock.Now())
	if job.ctx.Err() == nil {
		t.Fatal("test heartbeat did not preempt the background refresh")
	}
	s.applyResultLocked(smartResult{member: candidate, kind: "udp443", err: context.Canceled, generation: job.generation, configGeneration: job.configGeneration, memberGeneration: job.memberGeneration}, clock.Now())
	s.pending = nil
	s.queueProfileUDPLocked(clock.Now(), false)
	if candidate.queued["udp443"] || !candidate.udpRefresh[1].retryAt.Equal(clock.Now().Add(30*time.Second)) {
		t.Fatal("cancelled fixed refresh did not enter the 30-second retry")
	}
	clock.Advance(29 * time.Second)
	s.queueProfileUDPLocked(clock.Now(), false)
	if candidate.queued["udp443"] {
		t.Fatal("fixed refresh ignored retry backoff")
	}
	clock.Advance(time.Second)
	s.queueProfileUDPLocked(clock.Now(), false)
	if len(s.pending) != 1 || s.pending[0].member != candidate || s.pending[0].ctx.Err() != nil {
		t.Fatal("fixed refresh was not retried on a live context")
	}
	if candidate.udp443.value(clock.Now()) != "ok" || candidate.improvementObservations != 1 {
		t.Fatal("preemption expired fixed evidence or discarded confirmations")
	}
	job = s.pending[0]
	s.pending = nil
	clock.Advance(time.Second)
	s.applyResultLocked(smartResult{member: candidate, kind: "udp443", generation: job.generation, configGeneration: job.configGeneration, memberGeneration: job.memberGeneration}, clock.Now())
	if candidate.udp443.value(clock.Now()) != "ok" || candidate.improvementObservations != 2 {
		t.Fatal("successful retry did not preserve and advance confirmations")
	}
}

func TestSmartProfileUDPFixedRetryBackoffAndDrop(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "realtime-gaming")
	member := s.standby
	member.udp = smartEvidence{state: "ok", at: clock.Now().Add(-20 * time.Minute), expires: clock.Now().Add(10 * time.Minute)}
	for _, delay := range []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute} {
		s.queueProfileUDPFixedLocked(member, 0, "udp", 20*time.Minute, clock.Now())
		if !member.queued["udp"] {
			t.Fatal("fixed refresh was not queued")
		}
		delete(member.queued, "udp")
		s.pending = nil
		s.queueProfileUDPFixedLocked(member, 0, "udp", 20*time.Minute, clock.Now())
		if !member.udpRefresh[0].retryAt.Equal(clock.Now().Add(delay)) {
			t.Fatalf("retry=%s want=%s", member.udpRefresh[0].retryAt.Sub(clock.Now()), delay)
		}
		clock.Advance(delay)
	}
	member.udpRefresh = [2]smartUDPRefresh{}
	s.pending = make([]smartJob, 256)
	s.queueProfileUDPFixedLocked(member, 0, "udp", 20*time.Minute, clock.Now())
	if member.queued["udp"] || !member.udpRefresh[0].retryAt.Equal(clock.Now().Add(30*time.Second)) {
		t.Fatal("dropped fixed refresh was counted as refreshed instead of retried")
	}
}

func TestSmartProfileSoftFloorDiscoveryUsesForwardingEligibility(t *testing.T) {
	s, clock := smartProfileTestGroup(t, "phone")
	primary, standby := s.primary, s.standby
	primary.throughput = smartThroughput{Download: 10, At: clock.Now(), Source: "active"}
	standby.throughput = smartThroughput{Download: .1, At: clock.Now(), Source: "active"}
	primary.udp443 = smartEvidence{state: "failed", at: clock.Now(), expires: clock.Now().Add(10 * time.Minute)}
	standby.udp443 = smartEvidence{state: "ok", at: clock.Now(), expires: clock.Now().Add(10 * time.Minute)}
	candidate := &smartMember{tag: "c", identity: "c", serverIdentity: "c", state: "healthy", tier: 1, generation: 1, configOrder: 2, throughput: primary.throughput}
	s.members = append(s.members, candidate)
	smartProfileSetStability(s, candidate, .5, clock.Now())
	if s.throughputClass(primary, clock.Now()) != 0 || s.throughputClass(standby, clock.Now()) != 5 {
		t.Fatal("fixture does not reproduce the forwarding class mismatch")
	}
	s.handleRequest(smartRequest{kind: "udp443"})
	if !candidate.queued["udp443"] {
		t.Fatal("positive but forwarding-ineligible standby suppressed below-floor exploration")
	}
	delete(candidate.queued, "udp443")
	s.pending = nil
	primary.udp = primary.udp443
	standby.udp = standby.udp443
	s.preference.rank = []string{"udp"}
	s.queueProfileUDPLocked(clock.Now(), false)
	if candidate.queued["udp"] {
		t.Fatal("below-floor exploration ignored a valid generic UDP fallback")
	}
}
