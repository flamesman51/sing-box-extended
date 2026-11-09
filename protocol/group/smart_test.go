package group

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

type (
	smartFakeClock struct {
		now    time.Time
		timers []*smartFakeTimer
	}
	smartFakeTimer struct {
		at      time.Time
		channel chan time.Time
		stopped bool
	}
)

func (t *smartFakeTimer) C() <-chan time.Time { return t.channel }
func (t *smartFakeTimer) Stop() bool          { active := !t.stopped; t.stopped = true; return active }
func (c *smartFakeClock) Now() time.Time      { return c.now }
func (c *smartFakeClock) NewTimer(d time.Duration) smartTimer {
	timer := &smartFakeTimer{at: c.now.Add(d), channel: make(chan time.Time, 1)}
	c.timers = append(c.timers, timer)
	return timer
}

func (c *smartFakeClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
	for _, timer := range c.timers {
		if !timer.stopped && !c.now.Before(timer.at) {
			timer.channel <- c.now
			timer.stopped = true
		}
	}
}

type smartFakeOutbound struct{ tag string }

func (o *smartFakeOutbound) Type() string           { return "fake" }
func (o *smartFakeOutbound) Tag() string            { return o.tag }
func (o *smartFakeOutbound) Network() []string      { return []string{"tcp", "udp"} }
func (o *smartFakeOutbound) Dependencies() []string { return nil }
func (o *smartFakeOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	a, b := net.Pipe()
	_ = b.Close()
	return a, nil
}

func (o *smartFakeOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, net.ErrClosed
}

type smartTestCloser struct{ closed int }

func (c *smartTestCloser) Close() error { c.closed++; return nil }

func smartTestGroup() (*Smart, *smartFakeClock) {
	clock := &smartFakeClock{now: time.Unix(1800000000, 0)}
	s := &Smart{
		clock: clock, ctx: context.Background(), history: newSmartHistory(), generation: 1, configGeneration: 1,
		network: smartNetwork{Key: "wifi", Type: "wifi", Path: []string{"wifi"}, Link: true},
		kick:    make(chan struct{}, 1), requests: make(chan smartRequest, 64), results: make(chan smartResult, 64), leases: make(map[*smartMember]map[*smartLease]struct{}),
		startedAt: clock.now, lastDemand: clock.now, interrupt: true, quicPolicy: "auto", udpEnabled: true, targetMbps: 25,
		heartbeat: 15 * time.Second, standbyInterval: time.Minute, backgroundInterval: 5 * time.Minute,
	}
	for order, tag := range []string{"a", "b"} {
		s.members = append(s.members, &smartMember{outbound: &smartFakeOutbound{tag}, tag: tag, identity: tag, serverIdentity: tag, tier: 1, configOrder: order, generation: 1, state: "healthy", healthySince: clock.now, lastOK: clock.now, tcp: smartEvidence{state: "ok", at: clock.now, expires: clock.now.Add(time.Minute)}})
	}
	s.primary, s.standby = s.members[0], s.members[1]
	return s, clock
}

func TestSmartLateAttachAndMemberIsolation(t *testing.T) {
	s, _ := smartTestGroup()
	decision, err := s.SelectMember(context.Background(), "tcp", M.Socksaddr{})
	if err != nil {
		t.Fatal(err)
	}
	other := &smartTestCloser{}
	otherDecision := adapter.NewSmartDecision(s.members[1].outbound, []adapter.SmartSelection{{Group: s, Member: "b", Generation: 1}})
	otherLease := s.Attach(otherDecision, other)
	defer otherLease.Close()
	closers := s.revokeLocked(s.members[0])
	for _, closer := range closers {
		_ = closer.Close()
	}
	late := &smartTestCloser{}
	lease := s.Attach(decision, late)
	defer lease.Close()
	if late.closed != 1 || other.closed != 0 {
		t.Fatalf("late=%d unrelated=%d", late.closed, other.closed)
	}
}

func TestSmartBlackholeDetectionBound(t *testing.T) {
	s, clock := smartTestGroup()
	primary, standby := s.primary, s.standby
	primary.lastProbe = clock.now
	clock.now = clock.now.Add(15 * time.Second)
	s.tickLocked(clock.now)
	if !primary.inflight {
		t.Fatal("heartbeat was not scheduled")
	}
	clock.now = clock.now.Add(8 * time.Second)
	s.applyResultLocked(smartResult{member: primary, generation: 1, memberGeneration: 1, configGeneration: 1, kind: "tcp", operator: 0, err: io.ErrUnexpectedEOF}, clock.now)
	if s.primary != primary || primary.state != "suspect" {
		t.Fatal("first operator failure must only suspect")
	}
	clock.now = clock.now.Add(8 * time.Second)
	s.applyResultLocked(smartResult{member: standby, generation: 1, memberGeneration: 1, configGeneration: 1, kind: "tcp", rtt: 100 * time.Millisecond}, clock.now)
	closers := s.applyResultLocked(smartResult{member: primary, generation: 1, memberGeneration: 1, configGeneration: 1, kind: "tcp", operator: 1, confirm: true, err: io.ErrUnexpectedEOF}, clock.now)
	for _, closer := range closers {
		_ = closer.Close()
	}
	if s.Now() != "b" || primary.state != "quarantined" {
		t.Fatalf("selected=%s state=%s", s.Now(), primary.state)
	}
	if clock.now.Sub(s.startedAt) != 31*time.Second {
		t.Fatal("worst-case bound changed")
	}
}

func TestSmartUDP443UnknownNeverRejects(t *testing.T) {
	s, _ := smartTestGroup()
	decision, err := s.SelectMember(context.Background(), "udp", M.ParseSocksaddr("1.1.1.1:443"))
	if err != nil || decision.Outbound().Tag() != "a" {
		t.Fatalf("decision=%v error=%v", decision, err)
	}
}

func TestSmartSlowAliveAndOperatorReset(t *testing.T) {
	s, clock := smartTestGroup()
	primary := s.primary
	failure := smartResult{member: primary, generation: 1, memberGeneration: 1, configGeneration: 1, kind: "tcp", err: io.ErrUnexpectedEOF}
	s.applyResultLocked(failure, clock.now)
	if primary.state != "suspect" {
		t.Fatal(primary.state)
	}
	success := failure
	success.err = nil
	success.rtt = 2200 * time.Millisecond
	success.confirm = true
	s.applyResultLocked(success, clock.now.Add(3*time.Second))
	if primary.state != "healthy" || s.primary != primary || !primary.lastFail.IsZero() {
		t.Fatalf("state=%s lastFail=%v", primary.state, primary.lastFail)
	}
	if got := s.memberTimeoutLocked(primary); got < 7600*time.Millisecond {
		t.Fatalf("slow-member timeout=%v", got)
	}
}

func TestSmartUDP443AlternateRejectAndGeneration(t *testing.T) {
	s, clock := smartTestGroup()
	primary, alternate := s.primary, s.standby
	primary.udp443 = smartEvidence{state: "failed", at: clock.now, expires: clock.now.Add(10 * time.Minute)}
	alternate.udp443 = smartEvidence{state: "ok", at: clock.now, expires: clock.now.Add(10 * time.Minute)}
	decision, err := s.SelectMember(context.Background(), "udp", M.ParseSocksaddr("1.1.1.1:443"))
	if err != nil || decision.Outbound() != alternate.outbound {
		t.Fatalf("alternate not chosen: %v", err)
	}
	alternate.udp443 = smartEvidence{}
	if _, err = s.SelectMember(context.Background(), "udp", M.ParseSocksaddr("1.1.1.1:443")); err == nil {
		t.Fatal("known negative must reject without alternate")
	}
	generation := s.selectionGeneration
	s.newGenerationLocked(s.network, clock.now)
	if s.selectionGeneration == generation {
		t.Fatal("reject generation did not change")
	}
	if _, err = s.SelectMember(context.Background(), "udp", M.ParseSocksaddr("1.1.1.1:443")); err != nil {
		t.Fatalf("new generation unknown must not reject: %v", err)
	}
}

func TestSmartCaptiveAndLinkLossRemainNeutral(t *testing.T) {
	s, clock := smartTestGroup()
	s.direct = smartEvidence{state: "failed", at: clock.now, expires: clock.now.Add(2 * time.Minute)}
	for _, member := range s.members {
		s.applyResultLocked(smartResult{member: member, generation: 1, memberGeneration: 1, configGeneration: 1, kind: "tcp", confirm: true, err: io.ErrUnexpectedEOF}, clock.now)
	}
	clock.Advance(61 * time.Second)
	s.updateNotificationLocked(clock.now)
	if !s.allDeadSince.IsZero() || len(s.history.Stats) != 0 {
		t.Fatal("captive network penalized members or started all_dead")
	}
	s.network.Link = false
	s.direct = smartEvidence{state: "ok", expires: clock.now.Add(time.Minute)}
	s.applyResultLocked(smartResult{member: s.primary, generation: 1, memberGeneration: 1, configGeneration: 1, kind: "tcp", confirm: true, err: io.ErrUnexpectedEOF}, clock.now)
	if s.primary.state != "healthy" || len(s.history.Stats) != 0 {
		t.Fatal("link loss penalized member")
	}
}

func TestSmartColdWGAndStaleGeneration(t *testing.T) {
	s, clock := smartTestGroup()
	primary := s.primary
	primary.lastOK = time.Time{}
	primary.rtts = nil
	if got := s.memberTimeoutLocked(primary); got < 6*time.Second || got > 8*time.Second {
		t.Fatalf("cold WG timeout=%v", got)
	}
	s.newGenerationLocked(s.network, clock.now)
	s.applyResultLocked(smartResult{member: primary, generation: 1, memberGeneration: 1, configGeneration: 1, kind: "tcp", confirm: true, err: io.ErrUnexpectedEOF}, clock.now)
	if primary.state != "unknown" || len(s.history.Stats) != 0 {
		t.Fatal("stale result changed new generation")
	}
}

func TestSmartPausedSchedulerAndWakeRound(t *testing.T) {
	s, clock := smartTestGroup()
	s.paused = true
	s.tickLocked(clock.now)
	if len(s.pending) != 0 || len(clock.timers) != 0 {
		t.Fatal("paused scheduler scheduled work")
	}
	s.paused = false
	s.tickLocked(clock.now)
	health := 0
	for _, job := range s.pending {
		if job.kind == "tcp" {
			health++
		}
	}
	if health != 2 {
		t.Fatalf("wake health jobs=%d", health)
	}
}

func TestSmartTierRestorationHoldDown(t *testing.T) {
	s, clock := smartTestGroup()
	preferred, backup := s.primary, s.standby
	backup.tier = 2
	s.primary = backup
	clock.Advance(59 * time.Second)
	s.chooseLocked(clock.now)
	if s.primary != backup {
		t.Fatal("restored preferred tier before hold-down")
	}
	clock.Advance(time.Second)
	s.chooseLocked(clock.now)
	if s.primary != preferred {
		t.Fatal("did not restore preferred tier at 60s")
	}
}

func TestSmartEmergencyBackoff(t *testing.T) {
	s, clock := smartTestGroup()
	s.primary = nil
	s.standby = nil
	for _, member := range s.members {
		member.state = "quarantined"
		member.quarantineUntil = clock.now.Add(time.Hour)
	}
	clock.Advance(10 * time.Second)
	for _, expected := range []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 30 * time.Second, 30 * time.Second} {
		s.lastDemand = clock.now
		s.tickLocked(clock.now)
		if got := s.nextEmergency.Sub(clock.now); got != expected {
			t.Fatalf("backoff=%v want %v", got, expected)
		}
		for _, member := range s.members {
			member.inflight = false
			member.queued = make(map[string]bool)
		}
		s.pending = nil
		clock.Advance(expected)
	}
}

func TestSmartNestedDecisionAttachesEveryGroup(t *testing.T) {
	outer, _ := smartTestGroup()
	inner, _ := smartTestGroup()
	outer.primary.outbound = inner
	decision, err := outer.SelectMember(context.Background(), "tcp", M.Socksaddr{})
	if err != nil || len(decision.Selections()) != 2 {
		t.Fatalf("nested decision: %v %v", decision.Selections(), err)
	}
	closer := &smartTestCloser{}
	lease := adapter.AttachSmartDecision(decision, closer)
	if len(outer.leases[outer.primary]) != 1 || len(inner.leases[inner.primary]) != 1 {
		t.Fatal("nested attachments missing")
	}
	if err = lease.Close(); err != nil {
		t.Fatal(err)
	}
	if closer.closed != 1 || len(outer.leases[outer.primary]) != 0 || len(inner.leases[inner.primary]) != 0 {
		t.Fatal("nested close did not unregister exactly once")
	}
}

func TestSmartHourBucketsUsePlatformTimezone(t *testing.T) {
	s, clock := smartTestGroup()
	clock.now = time.Date(2026, 11, 9, 2, 0, 0, 0, time.UTC)
	s.timezone = time.FixedZone("platform", 3*3600)
	s.recordObservationLocked(s.primary, true, 100*time.Millisecond, clock.now)
	stat := s.history.Stats["wifi"][s.primary.identity]
	if stat.Hours[1] != [2]float64{1, 1} || stat.Hours[0] != [2]float64{} {
		t.Fatalf("platform hour attribution: %v", stat.Hours)
	}
}

func TestSmartQuarantineDecayKeepsFailureTimestamp(t *testing.T) {
	s, clock := smartTestGroup()
	primary := s.primary
	primary.quarantineLevel = 3
	primary.quarantineDecayAt, primary.lastFail = clock.now, clock.now
	failedAt := primary.lastFail
	clock.Advance(6 * time.Hour)
	s.applyResultLocked(smartResult{member: primary, generation: 1, memberGeneration: 1, configGeneration: 1, kind: "tcp", rtt: time.Millisecond}, clock.now)
	if primary.quarantineLevel != 2 || primary.lastFail != failedAt {
		t.Fatalf("level=%d last_fail=%v", primary.quarantineLevel, primary.lastFail)
	}
}

func TestSmartStandbyEvidenceUsesStandbyCadence(t *testing.T) {
	s, clock := smartTestGroup()
	standby := s.standby
	s.applyResultLocked(smartResult{member: standby, generation: 1, memberGeneration: 1, configGeneration: 1, kind: "tcp", rtt: time.Millisecond}, clock.now)
	if got := standby.tcp.expires.Sub(clock.now); got != 2*time.Minute {
		t.Fatalf("standby TCP TTL=%v", got)
	}
}

func TestSmartCloseJoinsOwnerAndStopsTimer(t *testing.T) {
	s, clock := smartTestGroup()
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.workCtx, s.workCancel = context.WithCancel(ctx)
	s.loopDone = make(chan struct{})
	go s.loop(ctx)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for _, timer := range clock.timers {
		if !timer.stopped {
			t.Fatal("owner timer survived close")
		}
	}
}
