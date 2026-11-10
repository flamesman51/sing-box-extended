package group

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/x/list"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"
)

type (
	smartFakeClock struct {
		mu      sync.Mutex
		now     time.Time
		timers  []*smartFakeTimer
		changed chan struct{}
	}
	smartFakeTimer struct {
		clock   *smartFakeClock
		at      time.Time
		channel chan time.Time
		stopped bool
	}
)

func (t *smartFakeTimer) C() <-chan time.Time { return t.channel }
func (t *smartFakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	active := !t.stopped
	t.stopped = true
	return active
}

func (c *smartFakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *smartFakeClock) NewTimer(d time.Duration) smartTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &smartFakeTimer{clock: c, at: c.now.Add(d), channel: make(chan time.Time, 1)}
	c.timers = append(c.timers, timer)
	select {
	case c.changed <- struct{}{}:
	default:
	}
	return timer
}

func (c *smartFakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
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
	clock := &smartFakeClock{now: time.Unix(1800000000, 0), changed: make(chan struct{}, 64)}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Smart{
		clock: clock, ctx: ctx, cancel: cancel, history: newSmartHistory(), generation: 1, configGeneration: 1, memberGeneration: 1,
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

func TestSmartUDP443RejectionStillDiscoversAlternates(t *testing.T) {
	s, clock := smartTestGroup()
	s.primary.udp443 = smartEvidence{state: "failed", at: clock.now, expires: clock.now.Add(10 * time.Minute)}
	destination := M.ParseSocksaddr("1.1.1.1:443")
	for range 3 {
		if _, err := s.SelectMember(context.Background(), "udp", destination); err == nil {
			t.Fatal("known UDP/443 failure did not reject")
		}
	}
	bits := s.requestBits.Swap(0)
	for index, kind := range smartRequestNames {
		if bits&(1<<index) != 0 {
			s.handleRequest(smartRequest{kind})
		}
	}
	if s.lastUDP443Demand != clock.now || s.lastDiscovery != clock.now {
		t.Fatal("rejected flow did not trigger discovery")
	}
	if len(s.pending) != 2 {
		t.Fatalf("discovery did not coalesce: jobs=%d", len(s.pending))
	}
	alternate := s.standby
	s.applyResultLocked(smartResult{member: alternate, generation: s.generation, memberGeneration: alternate.generation, configGeneration: s.configGeneration, kind: "udp443"}, clock.now)
	decision, err := s.SelectMember(context.Background(), "udp", destination)
	if err != nil || decision.Outbound() != alternate.outbound {
		t.Fatalf("discovered alternate not selected: %v", err)
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

func TestSmartTCPVerdictSurvivesIdleCadence(t *testing.T) {
	s, clock := smartTestGroup()
	primary := s.primary
	s.lastDemand = clock.now
	s.applyResultLocked(smartResult{member: primary, generation: 1, memberGeneration: 1, configGeneration: 1, kind: "tcp", rtt: time.Millisecond}, clock.now)
	// Demand stops: the next probe comes at the idle cadence, far beyond 2x heartbeat.
	clock.Advance(5 * time.Minute)
	if got := primary.tcp.value(clock.now); got != "ok" {
		t.Fatalf("verdict between idle probes=%s", got)
	}
	clock.Advance(smartTCPVerdictTTL)
	if got := primary.tcp.value(clock.now); got != "unknown" {
		t.Fatalf("verdict after an hour without checks=%s", got)
	}
}

func TestSmartCloseJoinsOwnerAndStopsTimer(t *testing.T) {
	s, clock := smartTestGroup()
	s.workCtx, s.workCancel = context.WithCancel(s.ctx)
	s.loopDone = make(chan struct{})
	go s.loop(s.ctx)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for _, timer := range clock.timers {
		if !timer.stopped {
			t.Fatal("owner timer survived close")
		}
	}
}

type smartReplacementProvider struct {
	adapter.Provider
	values   []adapter.Outbound
	callback adapter.ProviderUpdateCallback
}

func (p *smartReplacementProvider) Outbounds() []adapter.Outbound { return p.values }
func (p *smartReplacementProvider) RegisterCallback(callback adapter.ProviderUpdateCallback) *list.Element[adapter.ProviderUpdateCallback] {
	p.callback = callback
	return nil
}

func (*smartReplacementProvider) UnregisterCallback(*list.Element[adapter.ProviderUpdateCallback]) {}

type smartReplacementProviderManager struct {
	adapter.ProviderManager
	provider *smartReplacementProvider
}

func (m *smartReplacementProviderManager) Get(string) (adapter.Provider, bool) {
	return m.provider, true
}

type smartTestIdentityProvider struct{}

func (smartTestIdentityProvider) SmartIdentity(outbound adapter.Outbound) (string, string, error) {
	return outbound.Tag(), outbound.Tag(), nil
}

func TestSmartProviderRefreshDoesNotRetainUnobservedIdentities(t *testing.T) {
	s, _ := smartTestGroup()
	s.members, s.primary, s.standby = nil, nil, nil
	provider := &smartReplacementProvider{}
	s.providerManager = &smartReplacementProviderManager{provider: provider}
	s.providerCallbacks = make(map[adapter.Provider]*list.Element[adapter.ProviderUpdateCallback])
	s.options.Providers = []string{"provider"}
	s.ctx = service.ContextWith[adapter.SmartIdentityProvider](s.ctx, smartTestIdentityProvider{})
	for index := range 100 {
		provider.values = []adapter.Outbound{&smartFakeOutbound{tag: fmt.Sprintf("configuration-%d", index)}}
		if err := s.loadMembers(); err != nil {
			t.Fatal(err)
		}
		if len(s.history.IdentityServers) != 0 {
			t.Fatal("unobserved provider configurations retained history identity metadata")
		}
	}
}

func TestSmartProviderReplacementRejectsOldIncarnation(t *testing.T) {
	s, clock := smartTestGroup()
	s.members, s.primary, s.standby = nil, nil, nil
	s.memberGeneration = 0
	provider := &smartReplacementProvider{values: []adapter.Outbound{&smartFakeOutbound{tag: "a"}}}
	s.providerManager = &smartReplacementProviderManager{provider: provider}
	s.providerCallbacks = make(map[adapter.Provider]*list.Element[adapter.ProviderUpdateCallback])
	s.options.Providers = []string{"provider"}
	s.ctx = service.ContextWith[adapter.SmartIdentityProvider](s.ctx, smartTestIdentityProvider{})
	if err := s.loadMembers(); err != nil {
		t.Fatal(err)
	}
	s.newGenerationLocked(s.network, clock.Now())
	decision, err := s.SelectMember(context.Background(), "tcp", M.Socksaddr{})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Selections()[0].Generation != 2 {
		t.Fatalf("regression requires the old generation-2 decision: %v", decision.Selections())
	}
	attached := &smartTestCloser{}
	lease := s.Attach(decision, attached)
	defer lease.Close()
	old := s.primary
	provider.values = []adapter.Outbound{&smartFakeOutbound{tag: "a"}}
	if err = provider.callback("provider"); err != nil {
		t.Fatal(err)
	}
	bits := s.requestBits.Swap(0)
	for index, kind := range smartRequestNames {
		if bits&(1<<index) != 0 {
			s.handleRequest(smartRequest{kind})
		}
	}
	if s.primary == old || s.primary == nil || s.primary.outbound != provider.values[0] {
		t.Fatal("provider request did not replace the member")
	}
	late := &smartTestCloser{}
	lateLease := s.Attach(decision, late)
	defer lateLease.Close()
	if late.closed != 1 || attached.closed != 1 || len(s.leases[s.primary]) != 0 {
		t.Fatalf("replacement accepted old attachment: late=%d attached=%d", late.closed, attached.closed)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSmartShutdownRejectsSelectionAndLateAttach(t *testing.T) {
	s, _ := smartTestGroup()
	decision, err := s.SelectMember(context.Background(), "tcp", M.Socksaddr{})
	if err != nil {
		t.Fatal(err)
	}
	attached := &smartTestCloser{}
	lease := s.Attach(decision, attached)
	defer lease.Close()
	token := decision.Selections()[0].Generation
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if s.primary.generation == token {
		t.Fatal("shutdown did not invalidate the selection token")
	}
	if _, err = s.SelectMember(context.Background(), "tcp", M.Socksaddr{}); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("selection after shutdown: %v", err)
	}
	late := &smartTestCloser{}
	lateLease := s.Attach(decision, late)
	defer lateLease.Close()
	if attached.closed != 1 || late.closed != 1 {
		t.Fatalf("shutdown attachments: attached=%d late=%d", attached.closed, late.closed)
	}
}

func TestSmartGenerationWorkCannotOutliveLifecycle(t *testing.T) {
	created, err := NewSmart(context.Background(), nil, nil, "smart", option.SmartOutboundOptions{})
	if err != nil {
		t.Fatal(err)
	}
	s := created.(*Smart)
	clock := &smartFakeClock{now: time.Unix(1800000000, 0)}
	s.clock, s.paused = clock, true
	s.network = smartNetwork{Key: "wifi", Type: "wifi", Path: []string{"wifi"}, Link: true}
	scope := adapter.NewScope(context.Background(), nil)
	defer scope.Close()
	if err = s.Start(adapter.StartStateStarted, scope); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	first := s.workCtx
	s.cancel()
	s.newGenerationLocked(s.network, clock.Now())
	if first.Err() != context.Canceled || s.workCtx.Err() != context.Canceled || s.backgroundCtx.Err() != context.Canceled {
		t.Fatal("new work generation escaped lifecycle cancellation")
	}
	s.paused = false
	s.queueLocked(nil, "direct", 0, false, clock.Now())
	if len(s.pending) != 0 {
		t.Fatal("canceled lifecycle admitted work")
	}
}

type smartNotificationPlatform struct {
	smartNetworkTestPlatform
	sent, canceled int
}

func (p *smartNotificationPlatform) SendNotification(*adapter.Notification) error {
	p.sent++
	return nil
}

func (p *smartNotificationPlatform) CancelNotification(string, int32) error {
	p.canceled++
	return nil
}

func TestSmartPauseAndCloseCancelAllDead(t *testing.T) {
	for _, action := range []string{"pause", "close"} {
		t.Run(action, func(t *testing.T) {
			s, clock := smartTestGroup()
			defer s.Close()
			platform := &smartNotificationPlatform{}
			s.ctx = service.ContextWith[adapter.PlatformInterface](s.ctx, platform)
			s.network = smartReadNetwork(s.ctx, nil, clock.Now())
			s.ctx = pause.WithDefaultManager(s.ctx)
			s.pauseManager = service.FromContext[pause.Manager](s.ctx)
			s.notify = true
			for _, member := range s.members {
				member.lastFail = clock.Now().Add(time.Second)
			}
			s.direct = smartEvidence{state: "ok", expires: clock.Now().Add(2 * time.Minute)}
			s.updateNotificationLocked(clock.Now())
			clock.Advance(45 * time.Second)
			s.updateNotificationLocked(clock.Now())
			if platform.sent != 1 || !s.notified {
				t.Fatal("all_dead notification was not posted")
			}
			if action == "pause" {
				s.pauseManager.DevicePause()
				s.handleRequest(smartRequest{"pause"})
				s.tickLocked(clock.Now())
			} else if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if platform.canceled != 1 || s.notified || !s.allDeadSince.IsZero() {
				t.Fatalf("notification survived %s: canceled=%d active=%t", action, platform.canceled, s.notified)
			}
		})
	}
}

func TestSmartWakeWithoutPrimaryRunsEmergencyWithMinimumGap(t *testing.T) {
	s, clock := smartTestGroup()
	defer s.Close()
	s.ctx = pause.WithDefaultManager(s.ctx)
	s.pauseManager = service.FromContext[pause.Manager](s.ctx)
	s.network = smartReadNetwork(s.ctx, nil, clock.Now())
	s.primary, s.standby = nil, nil
	for _, member := range s.members {
		member.state = "quarantined"
		member.quarantineUntil = clock.Now().Add(time.Hour)
	}
	clock.Advance(2 * time.Minute)
	s.nextEmergency = clock.Now().Add(time.Minute)
	s.pauseManager.DevicePause()
	s.handleRequest(smartRequest{"pause"})
	s.pauseManager.DeviceWake()
	s.handleRequest(smartRequest{"pause"})
	s.handleRequest(smartRequest{"pause"})
	s.tickLocked(clock.Now())
	if len(s.pending) != 2 {
		t.Fatalf("wake did not bypass quarantine: jobs=%d", len(s.pending))
	}
	s.pauseManager.DevicePause()
	s.handleRequest(smartRequest{"pause"})
	clock.Advance(time.Second)
	s.pauseManager.DeviceWake()
	s.handleRequest(smartRequest{"pause"})
	s.tickLocked(clock.Now())
	if len(s.pending) != 0 {
		t.Fatal("repeated wake bypassed the two-second emergency gap")
	}
	clock.Advance(time.Second)
	s.tickLocked(clock.Now())
	if len(s.pending) != 2 {
		t.Fatal("coalesced wake emergency was lost during the minimum gap")
	}
}

func TestSmartStallEpisodesCoalesceAndCoolDown(t *testing.T) {
	s, clock := smartTestGroup()
	defer s.Close()
	decision, err := s.SelectMember(context.Background(), "tcp", M.Socksaddr{})
	if err != nil {
		t.Fatal(err)
	}
	lease := s.Attach(decision, &smartTestCloser{})
	defer lease.Close()
	s.requestBits.Store(0)
	clock.Advance(20 * time.Second)
	for range 10 {
		lease.CountWrite(1024)
	}
	if s.requestBits.Swap(0) == 0 {
		t.Fatal("stall episode did not request a health check")
	}
	s.handleRequest(smartRequest{"stall"})
	if len(s.pending) != 1 {
		t.Fatalf("stall did not coalesce: jobs=%d", len(s.pending))
	}
	s.pending = nil
	s.applyResultLocked(smartResult{member: s.primary, generation: s.generation, memberGeneration: s.primary.generation, configGeneration: s.configGeneration, kind: "tcp"}, clock.Now())
	for range 5 {
		clock.Advance(time.Second)
		lease.CountWrite(1024)
	}
	if s.requestBits.Swap(0) != 0 {
		t.Fatal("one stalled upload repeatedly requested probes after successful health checks")
	}
	s.handleRequest(smartRequest{"stall"})
	if len(s.pending) != 0 {
		t.Fatal("another stall bypassed the primary cooldown")
	}
	lease.CountRead(1)
	clock.Advance(20 * time.Second)
	lease.CountWrite(1024)
	if s.requestBits.Swap(0) == 0 {
		t.Fatal("a new stall episode was not admitted after read progress")
	}
	s.handleRequest(smartRequest{"stall"})
	if len(s.pending) != 1 {
		t.Fatal("a new stall episode did not recover after cooldown")
	}
}

func TestSmartDemandChallengerPreservesRankingPriority(t *testing.T) {
	for _, priority := range []string{"tier", "state", "stability", "equal"} {
		t.Run(priority, func(t *testing.T) {
			s, clock := smartTestGroup()
			defer s.Close()
			ledger, err := getSmartLedger(&smartLedgerTestCache{})
			if err != nil {
				t.Fatal(err)
			}
			s.ledger, s.throughputEnabled, s.generationSucceeded = ledger, true, true
			primary, challenger := s.primary, s.standby
			primary.throughput = smartThroughput{Download: 10, At: clock.Now(), Source: "active"}
			s.selectedAt = clock.Now()
			episode := smartTestDegradationEpisode(s, clock.Now())
			smartTestDegradationResult(s, episode, primary, smartDegradationConfirmation, 3, clock.Now())
			switch priority {
			case "tier":
				challenger.tier = 2
			case "state":
				challenger.state = "suspect"
			case "stability":
				stat := s.history.statistic("wifi", primary.identity, clock.Now())
				stat.S, stat.W = 100, 100
				stat = s.history.statistic("wifi", challenger.identity, clock.Now())
				stat.S, stat.W = 50, 100
			}
			s.rankLocked(clock.Now())
			if priority == "equal" {
				if s.degradationCandidateLocked(clock.Now()) != challenger {
					t.Fatal("eligible challenger missing")
				}
			} else if s.degradationCandidateLocked(clock.Now()) != nil {
				t.Fatalf("%s priority was ignored when choosing a demand challenger", priority)
			}
			// A queued measurement must be revalidated if priorities changed while it ran.
			episode.challenger, episode.challengerGeneration = challenger, challenger.generation
			smartTestDegradationResult(s, episode, challenger, smartDegradationChallenger, 30, clock.Now())
			if priority == "equal" {
				if s.primary != challenger {
					t.Fatal("demand did not waive dwell for an equal-priority challenger")
				}
			} else if s.primary != primary {
				t.Fatalf("throughput overrode %s priority at commit", priority)
			}
			for _, event := range s.history.Events {
				if priority != "equal" && event.Reason == "demand challenger meets target" {
					t.Fatal("ineligible challenger emitted a contradictory switch")
				}
			}
		})
	}
}

type smartProbeCall struct {
	tag      string
	operator int
	at       time.Time
}

type smartTimedOutbound struct {
	smartFakeOutbound
	clock     *smartFakeClock
	calls     chan smartProbeCall
	delay     time.Duration
	blackhole bool
}

func (o *smartTimedOutbound) DialContext(ctx context.Context, _ string, destination M.Socksaddr) (net.Conn, error) {
	timer := o.clock.NewTimer(o.delay)
	defer timer.Stop()
	select {
	case o.calls <- smartProbeCall{tag: o.tag, operator: int(destination.Port), at: o.clock.Now()}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C():
		if o.blackhole {
			return nil, io.ErrUnexpectedEOF
		}
		a, b := net.Pipe()
		_ = b.Close()
		return a, nil
	}
}

func smartStartTestOwner(t *testing.T, s *Smart, clock *smartFakeClock, beforeGeneralWorkers func()) {
	t.Helper()
	s.healthJobs, s.otherJobs = make(chan smartJob, 3), make(chan smartJob, 2)
	if s.workCtx == nil {
		s.workCtx, s.workCancel = context.WithCancel(s.ctx)
	}
	if s.backgroundCtx == nil {
		s.backgroundCtx, s.backgroundCancel = context.WithCancel(s.workCtx)
	}
	s.probe = func(ctx context.Context, dialer N.Dialer, operator int) (time.Duration, error) {
		started := clock.Now()
		conn, err := dialer.DialContext(ctx, "tcp", M.Socksaddr{Port: uint16(operator)})
		if err == nil {
			_ = conn.Close()
		}
		return clock.Now().Sub(started), err
	}
	s.directProbe = func(context.Context, string, []byte) (time.Duration, error) {
		return time.Millisecond, nil
	}
	s.loopDone = make(chan struct{})
	s.workers.Add(1)
	go s.worker(s.ctx, true)
	go s.loop(s.ctx)
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if beforeGeneralWorkers != nil {
		beforeGeneralWorkers()
	}
	for range 2 {
		s.workers.Add(1)
		go s.worker(s.ctx, false)
	}
}

func smartAwaitGroup(t *testing.T, s *Smart, clock *smartFakeClock, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		s.mu.Lock()
		ok := condition()
		s.mu.Unlock()
		if ok {
			return
		}
		select {
		case <-clock.changed:
		case <-deadline.C:
			t.Fatal("owner did not reach the expected state")
		}
	}
}

func smartAwaitProbe(t *testing.T, calls <-chan smartProbeCall, tag string, operator int) smartProbeCall {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case call := <-calls:
			if call.tag == tag && call.operator == operator {
				return call
			}
		case <-deadline.C:
			t.Fatalf("probe for %s operator %d was starved", tag, operator)
		}
	}
}

func TestSmartCompletedDiscoveryKeepsTenMinuteCadence(t *testing.T) {
	s, clock := smartTestGroup()
	t.Cleanup(s.cancel)
	started, primary := clock.Now(), s.primary
	s.generationSucceeded = true
	s.backgroundCtx, s.backgroundCancel = context.WithCancel(s.ctx)
	s.lastDirect = started
	primary.lastUDP, primary.lastUDP443 = started, started
	for _, member := range s.members {
		member.lastProbe = started
	}
	s.handleRequest(smartRequest{kind: "udp443"})
	jobs := s.pending
	s.pending = nil
	if len(jobs) != len(s.members) {
		t.Fatalf("initial discovery jobs=%d", len(jobs))
	}
	for _, job := range jobs {
		s.applyResultLocked(smartResult{member: job.member, kind: job.kind, generation: job.generation, memberGeneration: job.memberGeneration, configGeneration: job.configGeneration}, started)
	}
	for heartbeat := 1; heartbeat <= 40; heartbeat++ {
		clock.Advance(s.heartbeat)
		now := clock.Now()
		s.lastDemand = now
		s.tickLocked(now)
		s.handleRequest(smartRequest{kind: "udp443"})
		if heartbeat < 40 && !s.lastDiscovery.Equal(started) {
			t.Fatalf("heartbeat %d changed the completed discovery cadence", heartbeat)
		}
		jobs, s.pending = s.pending, nil
		discoveryJobs := 0
		for _, job := range jobs {
			if job.kind == "udp443" {
				discoveryJobs++
				if heartbeat < 40 && (job.member != primary || heartbeat != 20) {
					t.Fatalf("heartbeat %d repeated completed discovery for %s", heartbeat, job.member.tag)
				}
			}
			s.applyResultLocked(smartResult{member: job.member, kind: job.kind, generation: job.generation, memberGeneration: job.memberGeneration, configGeneration: job.configGeneration, rtt: time.Millisecond}, now)
		}
		if heartbeat == 40 && (discoveryJobs != len(s.members) || !s.lastDiscovery.Equal(now)) {
			t.Fatalf("ten-minute discovery jobs=%d last=%v", discoveryJobs, s.lastDiscovery)
		}
	}
}

func TestSmartInterruptedDiscoveryRequeuesOnlyCanceledMembers(t *testing.T) {
	for _, placement := range []string{"pending", "buffered", "running"} {
		t.Run(placement, func(t *testing.T) {
			s, clock := smartTestGroup()
			s.workCtx, s.workCancel = context.WithCancel(s.ctx)
			s.backgroundCtx, s.backgroundCancel = context.WithCancel(s.workCtx)
			s.healthJobs, s.otherJobs = make(chan smartJob, 3), make(chan smartJob, 2)
			s.members = append(s.members, &smartMember{outbound: &smartFakeOutbound{tag: "c"}, tag: "c", identity: "c", serverIdentity: "c", tier: 1, configOrder: 2, generation: 1, state: "healthy"})
			s.handleRequest(smartRequest{kind: "udp443"})
			completed, buffered, interrupted := s.pending[0], s.pending[1], s.pending[2]
			s.pending = nil
			s.applyResultLocked(smartResult{member: completed.member, kind: completed.kind, generation: completed.generation, memberGeneration: completed.memberGeneration, configGeneration: completed.configGeneration}, clock.Now())
			s.results <- smartResult{member: buffered.member, kind: buffered.kind, generation: buffered.generation, memberGeneration: buffered.memberGeneration, configGeneration: buffered.configGeneration}
			interrupted.member.udp443 = smartEvidence{state: "ok", at: clock.Now().Add(-time.Minute), expires: clock.Now().Add(9 * time.Minute)}
			evidence := interrupted.member.udp443
			started := make(chan struct{}, 1)
			s.udpProbe = func(ctx context.Context, _ N.Dialer, _ bool) (time.Duration, error) {
				started <- struct{}{}
				<-ctx.Done()
				return 0, ctx.Err()
			}
			s.probe = func(context.Context, N.Dialer, int) (time.Duration, error) {
				return time.Millisecond, nil
			}
			startWorker := func() {
				s.workers.Add(1)
				go s.worker(s.ctx, false)
			}
			t.Cleanup(func() {
				s.cancel()
				s.workers.Wait()
			})
			if placement == "pending" {
				s.pending = append(s.pending, interrupted)
			} else {
				s.otherJobs <- interrupted
			}
			if placement == "running" {
				startWorker()
				select {
				case <-started:
				case <-time.After(2 * time.Second):
					t.Fatal("discovery did not start")
				}
			}
			s.queueLocked(s.primary, "tcp", 1, false, clock.Now())
			if interrupted.ctx.Err() != context.Canceled {
				t.Fatal("primary heartbeat did not cancel unfinished discovery")
			}
			if placement == "pending" {
				s.dispatchLocked()
			}
			if placement != "running" {
				startWorker()
			}
			timeout := time.NewTimer(2 * time.Second)
			defer timeout.Stop()
			completions := 0
			for completions < 2 {
				select {
				case result := <-s.results:
					if result.kind != "udp443" {
						continue
					}
					if result.member == interrupted.member && !errors.Is(result.err, context.Canceled) {
						t.Fatalf("unfinished discovery result=%v", result.err)
					}
					selectionGeneration := s.selectionGeneration
					s.applyResultLocked(result, clock.Now())
					if result.member == interrupted.member && s.selectionGeneration != selectionGeneration {
						t.Fatal("cancellation changed UDP/443 selection evidence")
					}
					completions++
				case <-timeout.C:
					t.Fatal("discovery results did not arrive")
				}
			}
			s.handleRequest(smartRequest{kind: "udp443"})
			retries := 0
			for _, job := range s.pending {
				if job.kind != "udp443" {
					continue
				}
				retries++
				if job.member != interrupted.member || job.ctx != s.backgroundCtx || job.ctx.Err() != nil {
					t.Fatalf("unexpected discovery retry for %s", job.member.tag)
				}
			}
			if retries != 1 || !s.lastDiscovery.Equal(clock.Now()) {
				t.Fatalf("discovery retries=%d last=%v", retries, s.lastDiscovery)
			}
			healthRecorded := false
			for _, statistics := range s.history.Stats {
				for _, statistic := range statistics {
					healthRecorded = healthRecorded || statistic.W != 0 || len(statistic.RTTs) != 0
				}
			}
			if interrupted.member.state != "healthy" || interrupted.member.udp443 != evidence || healthRecorded {
				t.Fatal("discovery cancellation penalized member health")
			}
			if placement != "running" {
				select {
				case <-started:
					t.Fatal("canceled queued discovery reached the probe")
				default:
				}
			}
		})
	}
}

func TestSmartUnpreemptedDiscoveryCancellationDoesNotRetry(t *testing.T) {
	s, clock := smartTestGroup()
	t.Cleanup(s.cancel)
	s.handleRequest(smartRequest{kind: "udp443"})
	jobs := s.pending
	s.pending = nil
	for _, job := range jobs {
		s.applyResultLocked(smartResult{member: job.member, kind: job.kind, generation: job.generation, memberGeneration: job.memberGeneration, configGeneration: job.configGeneration, err: context.Canceled}, clock.Now())
	}
	s.handleRequest(smartRequest{kind: "udp443"})
	if len(s.pending) != 0 || len(s.history.Stats) != 0 {
		t.Fatal("unpreempted cancellation retried discovery or changed health")
	}
}

func TestSmartPrimaryDeadlinePreemptsOnlyBackgroundContext(t *testing.T) {
	for _, workGeneration := range []bool{false, true} {
		t.Run(fmt.Sprintf("work_generation=%t", workGeneration), func(t *testing.T) {
			s, clock := smartTestGroup()
			t.Cleanup(s.cancel)
			parent := s.ctx
			if workGeneration {
				s.workCtx, s.workCancel = context.WithCancel(s.ctx)
				t.Cleanup(s.workCancel)
				parent = s.workCtx
			}
			s.backgroundCtx, s.backgroundCancel = context.WithCancel(parent)
			oldBackground := s.backgroundCtx
			s.lastDiscovery = clock.Now()
			s.primary.lastUDP, s.primary.lastUDP443 = clock.Now(), clock.Now()
			s.queueLocked(s.primary, "udp443", 0, false, clock.Now())
			s.queueLocked(s.standby, "tcp", 0, false, clock.Now())
			if oldBackground.Err() != nil || !s.lastDiscovery.Equal(clock.Now()) {
				t.Fatal("non-primary health work preempted background discovery")
			}
			s.queueLocked(s.primary, "tcp", 1, false, clock.Now())
			if oldBackground.Err() != context.Canceled {
				t.Fatal("primary deadline left buffered background work active")
			}
			if s.pending[0].ctx != oldBackground || s.pending[0].ctx.Err() != context.Canceled {
				t.Fatal("buffered background job escaped its canceled context")
			}
			for _, job := range s.pending[1:] {
				if job.ctx != parent || job.ctx.Err() != nil {
					t.Fatal("primary or standby health was canceled with background work")
				}
			}
			background := s.backgroundCtx
			if background == oldBackground || background.Err() != nil || !s.lastDiscovery.Equal(clock.Now()) {
				t.Fatal("primary deadline did not renew background context and preserve discovery cadence")
			}
			if !s.primary.lastUDP.Equal(clock.Now()) || !s.primary.lastUDP443.Equal(clock.Now()) {
				t.Fatal("preemption changed UDP probe cadence")
			}
			s.queueLocked(s.standby, "udp", 0, false, clock.Now())
			if s.pending[len(s.pending)-1].ctx != background {
				t.Fatal("future background job reused the canceled context")
			}
			if workGeneration {
				s.workCancel()
				if background.Err() != context.Canceled || s.ctx.Err() != nil {
					t.Fatal("replacement background context escaped work-generation cancellation")
				}
			}
			s.cancel()
			for _, job := range s.pending {
				if job.ctx.Err() != context.Canceled {
					t.Fatal("queued work escaped lifecycle cancellation")
				}
			}
		})
	}
}

func TestSmartRejectedPrimaryDoesNotPreemptBackground(t *testing.T) {
	s, clock := smartTestGroup()
	t.Cleanup(s.cancel)
	s.backgroundCtx, s.backgroundCancel = context.WithCancel(s.ctx)
	s.lastDiscovery = clock.Now()
	s.pending = make([]smartJob, 256)
	s.queueLocked(s.primary, "tcp", 1, false, clock.Now())
	if s.backgroundCtx.Err() != nil || !s.lastDiscovery.Equal(clock.Now()) {
		t.Fatal("rejected primary job canceled background work")
	}
	if s.primary.inflight || s.primary.queued["tcp"] {
		t.Fatal("rejected primary job remained inflight")
	}
}

func TestSmartOwnerBlackholeBoundWithQueuedBackgroundConfirmations(t *testing.T) {
	for _, standbyBusy := range []bool{false, true} {
		t.Run(fmt.Sprintf("standby_busy=%t", standbyBusy), func(t *testing.T) {
			s, clock := smartTestGroup()
			started := clock.Now()
			calls := make(chan smartProbeCall, 64)
			primary, standby := s.primary, s.standby
			s.timeout, s.udpEnabled, s.generationSucceeded = 8*time.Second, false, true
			s.lastDirect, s.lastFullRound = started, started
			s.lastNetworkRead = started.Add(time.Hour)
			for _, member := range s.members {
				member.lastProbe = started
				member.outbound = &smartTimedOutbound{smartFakeOutbound: smartFakeOutbound{tag: member.tag}, clock: clock, calls: calls, delay: 8 * time.Second, blackhole: member == primary}
			}
			for order, tag := range []string{"c", "d", "e"} {
				member := &smartMember{tag: tag, identity: tag, serverIdentity: tag, tier: 1, configOrder: order + 2, generation: 1, state: "suspect", lastProbe: started}
				member.outbound = &smartTimedOutbound{smartFakeOutbound: smartFakeOutbound{tag: tag}, clock: clock, calls: calls, delay: 8 * time.Second, blackhole: true}
				s.members = append(s.members, member)
			}
			clock.Advance(14 * time.Second)
			s.workCtx, s.workCancel = context.WithCancel(s.ctx)
			s.backgroundCtx, s.backgroundCancel = context.WithCancel(s.workCtx)
			var beforeGeneralWorkers func()
			if standbyBusy {
				s.queueLocked(standby, "tcp", 0, false, clock.Now())
				beforeGeneralWorkers = func() {
					call := smartAwaitProbe(t, calls, "b", 0)
					if call.at.Sub(started) != 14*time.Second {
						t.Fatalf("standby did not occupy the reserved worker before the heartbeat: %+v", call)
					}
				}
			}
			for _, member := range s.members[2:] {
				s.queueLocked(member, "tcp", 0, true, clock.Now())
			}
			smartStartTestOwner(t, s, clock, beforeGeneralWorkers)
			background := make(map[string]bool)
			occupiedDeadline := time.NewTimer(2 * time.Second)
			defer occupiedDeadline.Stop()
			for len(background) < 2 {
				select {
				case call := <-calls:
					if call.tag == "a" || call.tag == "b" {
						t.Fatalf("unexpected early health probe: %+v", call)
					}
					if call.at.Sub(started) != 14*time.Second {
						t.Fatalf("background confirmation did not occupy a general worker: %+v", call)
					}
					background[call.tag] = true
				case <-occupiedDeadline.C:
					t.Fatal("background workers did not become occupied")
				}
			}
			clock.Advance(time.Second)
			s.request("demand")
			first := smartAwaitProbe(t, calls, "a", 1)
			if first.at.Sub(started) != 15*time.Second {
				t.Fatalf("primary heartbeat was delayed by worker contention: %v", first.at.Sub(started))
			}
			if standbyBusy {
				clock.Advance(7 * time.Second)
				s.request("demand")
				smartAwaitGroup(t, s, clock, func() bool {
					return standby.lastOK.Equal(started.Add(22*time.Second)) && !standby.inflight
				})
				clock.Advance(time.Second)
			} else {
				clock.Advance(8 * time.Second)
			}
			s.request("demand")
			health := make(map[string]bool)
			deadline := time.NewTimer(2 * time.Second)
			defer deadline.Stop()
			for len(health) < 2 {
				select {
				case call := <-calls:
					if (call.tag == "a" || call.tag == "b") && call.operator == 2 {
						if call.at.Sub(started) != 23*time.Second {
							t.Fatalf("health validation missed its reservation: %+v", call)
						}
						health[call.tag] = true
					}
				case <-deadline.C:
					t.Fatal("primary confirmation and standby validation did not start in parallel")
				}
			}
			clock.Advance(8 * time.Second)
			s.request("demand")
			smartAwaitGroup(t, s, clock, func() bool {
				return primary.state == "quarantined" && s.primary == standby && standby.lastOK.Equal(clock.Now())
			})
			decision, err := s.SelectMember(context.Background(), "tcp", M.Socksaddr{})
			if err != nil || decision.Outbound() != standby.outbound {
				t.Fatalf("dead primary remained selectable at 31 seconds: %v", err)
			}
			if clock.Now().Sub(started) != 31*time.Second {
				t.Fatal("regression did not exercise the documented worst-case bound")
			}
		})
	}
}

func TestSmartCloseCancelsOwnerWorkersAfterGenerationChange(t *testing.T) {
	s, clock := smartTestGroup()
	calls := make(chan smartProbeCall, 8)
	s.udpEnabled, s.generationSucceeded = false, true
	s.lastDirect, s.lastNetworkRead = clock.Now(), clock.Now().Add(time.Hour)
	for _, member := range s.members {
		member.outbound = &smartTimedOutbound{smartFakeOutbound: smartFakeOutbound{tag: member.tag}, clock: clock, calls: calls, delay: time.Hour}
	}
	s.newGenerationLocked(s.network, clock.Now())
	smartStartTestOwner(t, s, clock, nil)
	smartAwaitProbe(t, calls, "a", int(s.generation%3))
	closed := make(chan error, 1)
	s.request("network")
	s.request("pause")
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("work survived shutdown until its probe timeout")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.workCtx.Err() != context.Canceled || s.backgroundCtx.Err() != context.Canceled {
		t.Fatal("owner installed work outside the canceled lifecycle")
	}
}
