package group

import (
	"context"
	"errors"
	"io"
	"maps"
	"math"
	"net"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/x/list"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"
)

func RegisterSmart(registry *outbound.Registry) {
	outbound.Register[option.SmartOutboundOptions](registry, C.TypeSmart, NewSmart)
}

var (
	_ adapter.SmartGroup              = (*Smart)(nil)
	_ adapter.URLTestGroup            = (*Smart)(nil)
	_ adapter.InterfaceUpdateListener = (*Smart)(nil)
)

type smartClock interface {
	Now() time.Time
	NewTimer(time.Duration) smartTimer
}

type smartTimer interface {
	C() <-chan time.Time
	Stop() bool
}

type smartRealClock struct{}

func (smartRealClock) Now() time.Time                      { return time.Now() }
func (smartRealClock) NewTimer(d time.Duration) smartTimer { return &smartRealTimer{time.NewTimer(d)} }

type smartRealTimer struct{ *time.Timer }

func (t *smartRealTimer) C() <-chan time.Time { return t.Timer.C }

type smartEvidence struct {
	state       string
	at, expires time.Time
}

func (e smartEvidence) value(now time.Time) string {
	if e.state == "" || !now.Before(e.expires) {
		return "unknown"
	}
	return e.state
}

type smartThroughput struct {
	Download, Upload float64
	At               time.Time
	Source           string
}

type smartMember struct {
	outbound                                                                  adapter.Outbound
	tag, identity, serverIdentity                                             string
	tier                                                                      int
	country                                                                   string
	configOrder                                                               int
	generation                                                                uint64
	state                                                                     string
	probation                                                                 int
	emergency                                                                 bool
	quarantineLevel                                                           int
	quarantineDecayAt                                                         time.Time
	startupRank                                                               int
	quarantineUntil, healthySince, lastOK, lastFail, lastProbe, lastProbation time.Time
	lastUDP, lastUDP443, lastThroughput                                       time.Time
	tcp, udp, udp443                                                          smartEvidence
	rtts                                                                      []float64
	throughput                                                                smartThroughput
	inflight                                                                  bool
	queued                                                                    map[string]bool
	improvementAt                                                             time.Time
	improvementObservations                                                   int
	improvementSignature                                                      int
	rankState, rankStability, rankThroughput                                  int
	rankLatency                                                               float64
}

type (
	smartRequest struct{ kind string }
	smartJob     struct {
		member                                         *smartMember
		kind                                           string
		operator                                       int
		confirm                                        bool
		generation, memberGeneration, configGeneration uint64
		ctx                                            context.Context
		timeout                                        time.Duration
		capBytes                                       int64
		reservation                                    string
		urgent                                         bool
		country, networkType                           string
	}
)

type smartResult struct {
	member                                         *smartMember
	kind                                           string
	operator                                       int
	confirm                                        bool
	generation, memberGeneration, configGeneration uint64
	rtt                                            time.Duration
	err                                            error
	download, upload                               float64
	bytes                                          int64
	reservation                                    string
}

type smartURLWaiter struct {
	remaining map[*smartMember]bool
	result    map[string]uint16
	done      chan struct{}
	err       error
}

type Smart struct {
	outbound.Adapter
	ctx                                                                                  context.Context
	outbound                                                                             adapter.OutboundManager
	logger                                                                               log.ContextLogger
	options                                                                              option.SmartOutboundOptions
	mu                                                                                   sync.Mutex
	persistenceMu                                                                        sync.Mutex
	persistenceKick, persistenceStop, persistenceDone                                    chan struct{}
	members                                                                              []*smartMember
	primary, standby                                                                     *smartMember
	generation, configGeneration, selectionGeneration, memberGeneration                  uint64
	network                                                                              smartNetwork
	history                                                                              *smartHistory
	priors                                                                               []smartPrior
	timezone                                                                             *time.Location
	urlHistory                                                                           *urltest.HistoryStorage
	cache                                                                                adapter.CacheFile
	ledger                                                                               *smartLedger
	seed, directSeed                                                                     []byte
	installID                                                                            string
	clock                                                                                smartClock
	probe                                                                                func(context.Context, N.Dialer, int) (time.Duration, error)
	udpProbe                                                                             func(context.Context, N.Dialer, bool) (time.Duration, error)
	directProbe                                                                          func(context.Context, string, []byte) (time.Duration, error)
	kick                                                                                 chan struct{}
	requests                                                                             chan smartRequest
	requestBits                                                                          atomic.Uint32
	results                                                                              chan smartResult
	healthJobs, otherJobs                                                                chan smartJob
	pending                                                                              []smartJob
	cancel                                                                               context.CancelFunc
	workCtx                                                                              context.Context
	workCancel                                                                           context.CancelFunc
	backgroundCtx                                                                        context.Context
	backgroundCancel                                                                     context.CancelFunc
	workers                                                                              sync.WaitGroup
	loopDone                                                                             chan struct{}
	leases                                                                               map[*smartMember]map[*smartLease]struct{}
	startedAt, selectedAt, lastDemand, lastNetworkRead, lastFullRound                    time.Time
	lastUDP443Demand, lastDiscovery, lastDirect, lastWrite, allDeadSince, ambiguousSince time.Time
	nextEmergency                                                                        time.Time
	lastEmergency, lastStall                                                             time.Time
	wakeEmergency                                                                        bool
	emergencyStep                                                                        int
	paused, closed                                                                       bool
	pauseStarted                                                                         time.Time
	pausedTotal                                                                          time.Duration
	generationSucceeded, offline, notified, dirty, directInflight                        bool
	direct                                                                               smartEvidence
	directRTT                                                                            time.Duration
	interrupt, notify, udpEnabled, throughputEnabled                                     bool
	quicPolicy                                                                           string
	targetMbps                                                                           float64
	heartbeat, standbyInterval, backgroundInterval, timeout                              time.Duration
	pauseManager                                                                         pause.Manager
	pauseCallback                                                                        *list.Element[pause.Callback]
	providerCallbacks                                                                    map[adapter.Provider]*list.Element[adapter.ProviderUpdateCallback]
	providerManager                                                                      adapter.ProviderManager
	challenger                                                                           *smartMember
	challengerUntil, demandStarted                                                       time.Time
	lastChallenger                                                                       time.Time
	urlWaiters                                                                           []*smartURLWaiter
}

func NewSmart(ctx context.Context, _ adapter.Router, logger log.ContextLogger, tag string, options option.SmartOutboundOptions) (adapter.Outbound, error) {
	s := &Smart{
		Adapter: outbound.NewAdapter(C.TypeSmart, tag, []string{N.NetworkTCP, N.NetworkUDP}, options.Outbounds), ctx: ctx,
		outbound: service.FromContext[adapter.OutboundManager](ctx), logger: logger, options: options, clock: smartRealClock{},
		history: newSmartHistory(), urlHistory: service.PtrFromContext[urltest.HistoryStorage](ctx), cache: service.FromContext[adapter.CacheFile](ctx),
		pauseManager: service.FromContext[pause.Manager](ctx), providerManager: service.FromContext[adapter.ProviderManager](ctx),
		generation: 1, configGeneration: 1, selectionGeneration: 1, leases: make(map[*smartMember]map[*smartLease]struct{}),
		kick: make(chan struct{}, 1), requests: make(chan smartRequest, 64), results: make(chan smartResult, 64), healthJobs: make(chan smartJob, 3), otherJobs: make(chan smartJob, 2),
		providerCallbacks: make(map[adapter.Provider]*list.Element[adapter.ProviderUpdateCallback]), probe: smartTCPProbe, udpProbe: smartUDPProbe, directProbe: smartDirectProbe,
	}
	if err := s.configure(); err != nil {
		return nil, err
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	return s, nil
}

func (s *Smart) configure() error {
	boolDefault := func(v *bool) bool { return v == nil || *v }
	s.interrupt, s.notify = boolDefault(s.options.InterruptOnFailover), boolDefault(s.options.NotifyAllDead)
	s.udpEnabled, s.throughputEnabled = boolDefault(s.options.UDP.Enabled), boolDefault(s.options.Throughput.Enabled)
	s.quicPolicy = s.options.UDP.QUICPolicy
	if s.quicPolicy == "" {
		s.quicPolicy = "auto"
	}
	if s.quicPolicy != "auto" && s.quicPolicy != "route_only" && s.quicPolicy != "off" {
		return E.New("invalid smart udp.quic_policy")
	}
	s.heartbeat = time.Duration(s.options.Probe.HeartbeatInterval)
	if s.heartbeat == 0 {
		s.heartbeat = 15 * time.Second
	}
	s.standbyInterval = time.Duration(s.options.Probe.StandbyInterval)
	if s.standbyInterval == 0 {
		s.standbyInterval = time.Minute
	}
	s.backgroundInterval = time.Duration(s.options.Probe.BackgroundInterval)
	if s.backgroundInterval == 0 {
		s.backgroundInterval = 5 * time.Minute
	}
	s.timeout = time.Duration(s.options.Probe.Timeout)
	if s.heartbeat < 0 || s.standbyInterval < 0 || s.backgroundInterval < 0 || s.timeout < 0 {
		return E.New("smart probe intervals must be positive")
	}
	s.targetMbps = s.options.Throughput.TargetMbps
	if s.targetMbps == 0 {
		s.targetMbps = 25
	}
	if s.targetMbps <= 0 || math.IsNaN(s.targetMbps) || math.IsInf(s.targetMbps, 0) {
		return E.New("smart throughput.target_mbps must be positive")
	}
	if s.options.Throughput.DownloadURL == "" {
		s.options.Throughput.DownloadURL = "https://speed.cloudflare.com/__down?bytes={bytes}"
	}
	if s.options.Throughput.UploadURL == "" {
		s.options.Throughput.UploadURL = "https://speed.cloudflare.com/__up"
	}
	for _, raw := range []string{s.options.Throughput.DownloadURL, s.options.Throughput.UploadURL} {
		u, err := url.Parse(strings.ReplaceAll(raw, "{bytes}", "1000000"))
		if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
			return E.New("invalid smart throughput URL")
		}
	}
	if !strings.Contains(s.options.Throughput.DownloadURL, "{bytes}") {
		return E.New("smart download_url must contain {bytes}")
	}
	for _, budget := range []*float64{s.options.Throughput.WIFIDailyBudgetMB, s.options.Throughput.CellularDailyBudgetMB, s.options.Throughput.OtherDailyBudgetMB} {
		if budget != nil && (*budget < 0 || math.IsNaN(*budget) || math.IsInf(*budget, 0)) {
			return E.New("smart budgets must be nonnegative")
		}
	}
	for tag, member := range s.options.MemberOptions {
		if member.Tier < 0 {
			return E.New("smart member tier must be at least 1: ", tag)
		}
	}
	return nil
}

func (s *Smart) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	switch stage {
	case adapter.StartStateStart:
		ledger, err := getSmartLedger(s.cache)
		if err != nil {
			return err
		}
		s.ledger = ledger
		if ledger != nil {
			s.installID, s.seed, s.directSeed = ledger.Installation()
			s.priors = ledger.Metadata().Priors[s.Tag()]
			value := func(p *float64, d float64) float64 {
				if p != nil {
					return *p
				}
				return d
			}
			ledger.Register(s.Tag(), map[string]float64{"wifi": value(s.options.Throughput.WIFIDailyBudgetMB, 300), "cellular": value(s.options.Throughput.CellularDailyBudgetMB, 50), "other": value(s.options.Throughput.OtherDailyBudgetMB, 300)})
		}
		if s.cache != nil {
			stored, err := s.cache.LoadSmartHistory(s.Tag())
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			if stored != nil {
				history, err := decodeSmartHistory(stored.Data)
				if err != nil {
					return err
				}
				s.history = history
			}
		}
		if err := s.loadMembers(); err != nil {
			return err
		}
		s.network = smartReadNetwork(s.ctx, s.seed, s.clock.Now())
		s.timezone = time.FixedZone("", s.network.Offset)
		s.startedAt = s.clock.Now()
		s.selectedAt = s.startedAt
		s.lastDemand = s.startedAt
		s.restoreThroughputLocked(s.startedAt)
		s.rankLocked(s.startedAt)
		for rank, member := range s.members {
			member.startupRank = rank
		}
		s.primary = s.members[0]
	case adapter.StartStateStarted:
		ctx := s.ctx
		s.workCtx, s.workCancel = context.WithCancel(ctx)
		s.backgroundCtx, s.backgroundCancel = context.WithCancel(s.workCtx)
		s.loopDone = make(chan struct{})
		if s.pauseManager != nil {
			s.paused = s.pauseManager.IsPaused()
			s.pauseCallback = s.pauseManager.RegisterCallback(func(int) { s.request("pause") })
		}
		for worker := range 3 {
			s.workers.Add(1)
			go s.worker(ctx, worker == 0)
		}
		go s.loop(ctx)
		scope.Add(s.Close)
	}
	return nil
}

func (s *Smart) loadMembers() error {
	members := make([]adapter.Outbound, 0, len(s.options.Outbounds))
	seen := make(map[string]bool)
	for _, tag := range s.options.Outbounds {
		member, ok := s.outbound.Outbound(tag)
		if !ok {
			return E.New("smart outbound not found: ", tag)
		}
		if !seen[tag] {
			members = append(members, member)
			seen[tag] = true
		}
	}
	include, exclude := (*regexp.Regexp)(s.options.Include), (*regexp.Regexp)(s.options.Exclude)
	providers := s.options.Providers
	if s.options.UseAllProviders && s.providerManager != nil {
		providers = nil
		for _, provider := range s.providerManager.Providers() {
			providers = append(providers, provider.Tag())
		}
	}
	for _, tag := range providers {
		if s.providerManager == nil {
			return E.New("missing smart provider manager")
		}
		provider, ok := s.providerManager.Get(tag)
		if !ok {
			return E.New("smart provider not found: ", tag)
		}
		if _, registered := s.providerCallbacks[provider]; !registered {
			s.providerCallbacks[provider] = provider.RegisterCallback(func(string) error { s.request("provider"); return nil })
		}
		for _, member := range provider.Outbounds() {
			tag := member.Tag()
			if seen[tag] || (include != nil && !include.MatchString(tag)) || (exclude != nil && exclude.MatchString(tag)) {
				continue
			}
			members = append(members, member)
			seen[tag] = true
		}
	}
	if len(members) == 0 {
		return E.New("smart requires at least one member")
	}
	old := make(map[adapter.Outbound]*smartMember)
	for _, member := range s.members {
		old[member.outbound] = member
	}
	nextMembers := make([]*smartMember, 0, len(members))
	for order, member := range members {
		if member == s {
			return E.New("smart group cannot contain itself")
		}
		if existing := old[member]; existing != nil {
			existing.configOrder = order
			nextMembers = append(nextMembers, existing)
			continue
		}
		identity, server, err := smartMemberIdentity(s.ctx, member)
		if err != nil {
			return E.Cause(err, "smart member identity: ", member.Tag())
		}
		options := s.options.MemberOptions[member.Tag()]
		tier := options.Tier
		if tier == 0 {
			tier = 1
		}
		s.history.IdentityServers[identity] = server
		s.memberGeneration++
		nextMembers = append(nextMembers, &smartMember{outbound: member, tag: member.Tag(), identity: identity, serverIdentity: server, tier: tier, configOrder: order, country: strings.ToUpper(options.Country), generation: s.memberGeneration, state: "unknown", queued: make(map[string]bool)})
	}
	s.members = nextMembers
	if s.primary != nil && !slices.Contains(s.members, s.primary) {
		s.primary = nil
	}
	return nil
}

func (s *Smart) All() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	tags := make([]string, len(s.members))
	for i, m := range s.members {
		tags[i] = m.tag
	}
	return tags
}

func (s *Smart) Now() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.primary == nil {
		return ""
	}
	return s.primary.tag
}
func (s *Smart) References() []string             { return []string{s.Now()} }
func (s *Smart) InterfaceUpdated(context.Context) { s.request("network") }
func (s *Smart) PerformUpdateCheck()              { s.request("demand") }
func (s *Smart) request(kind string) {
	for i, name := range smartRequestNames {
		if kind == name {
			s.requestBits.Or(1 << i)
			break
		}
	}
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

var smartRequestNames = []string{"network", "pause", "provider", "udp443", "stall", "force", "demand", "snapshot"}

type smartDepthKey struct{}

func (s *Smart) SelectMember(ctx context.Context, network string, destination M.Socksaddr) (adapter.SmartDecision, error) {
	depth, _ := ctx.Value(smartDepthKey{}).(int)
	if depth >= 8 {
		return adapter.SmartDecision{}, E.New("smart group nesting exceeds 8")
	}
	ctx = context.WithValue(ctx, smartDepthKey{}, depth+1)
	s.mu.Lock()
	if s.closed || s.ctx.Err() != nil {
		s.mu.Unlock()
		return adapter.SmartDecision{}, net.ErrClosed
	}
	now := s.clock.Now()
	if network == N.NetworkUDP && destination.Port == 443 && s.udpEnabled && s.quicPolicy != "off" {
		s.request("udp443")
	}
	member, err := s.selectLocked(network, destination, now)
	if err != nil {
		s.mu.Unlock()
		return adapter.SmartDecision{}, err
	}
	token := member.generation
	selected := member.outbound
	s.mu.Unlock()
	chain := []adapter.SmartSelection{{Group: s, Member: member.tag, Generation: token}}
	return s.resolveDecision(ctx, selected, chain, network, destination)
}

func (s *Smart) resolveDecision(ctx context.Context, selected adapter.Outbound, chain []adapter.SmartSelection, network string, destination M.Socksaddr) (adapter.SmartDecision, error) {
	for range 8 {
		if nested, ok := selected.(adapter.SmartGroup); ok {
			decision, err := nested.SelectMember(ctx, network, destination)
			if err != nil {
				return adapter.SmartDecision{}, err
			}
			return adapter.NewSmartDecision(decision.Outbound(), append(chain, decision.Selections()...)), nil
		}
		group, ok := selected.(adapter.OutboundGroup)
		if !ok {
			return adapter.NewSmartDecision(selected, chain), nil
		}
		var loaded bool
		selected, loaded = s.outbound.Outbound(group.Now())
		if !loaded {
			return adapter.SmartDecision{}, E.New("smart nested member missing")
		}
	}
	return adapter.SmartDecision{}, E.New("smart group nesting exceeds 8")
}

func (s *Smart) selectLocked(network string, destination M.Socksaddr, now time.Time) (*smartMember, error) {
	primary := s.primary
	if primary == nil || primary.state == "quarantined" || primary.state == "probation" && !primary.emergency {
		return nil, E.New("smart: no reachable member")
	}
	if primary.state == "unknown" && now.Sub(s.startedAt) >= 10*time.Second {
		return nil, E.New("smart: startup validation pending")
	}
	if network != N.NetworkUDP || !s.udpEnabled {
		return primary, nil
	}
	if destination.Port != 443 || s.quicPolicy == "off" {
		if primary.udp.value(now) != "failed" {
			return primary, nil
		}
		for _, member := range s.members {
			if member.state == "healthy" && member.udp.value(now) == "ok" {
				return member, nil
			}
		}
		return primary, nil
	}
	if primary.udp443.value(now) != "failed" {
		return primary, nil
	}
	for _, member := range s.members {
		if member.state == "healthy" && member.tier == primary.tier && member.udp443.value(now) == "ok" && s.throughputClass(member, now) <= s.throughputClass(primary, now)+1 {
			return member, nil
		}
	}
	if s.quicPolicy == "auto" {
		return nil, E.New("smart: UDP/443 unreachable (selection generation ", s.selectionGeneration, ")")
	}
	return primary, nil
}

func (s *Smart) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	decision, err := s.SelectMember(ctx, network, destination)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	timeout := s.memberTimeoutLocked(s.primary)
	standby := s.standby
	s.mu.Unlock()
	total := 2 * timeout
	if deadline, ok := ctx.Deadline(); ok {
		total = time.Until(deadline)
		if total <= 0 {
			return nil, context.DeadlineExceeded
		}
	}
	totalCtx, cancel := context.WithTimeout(ctx, total)
	defer cancel()
	firstBudget := timeout
	if standby != nil {
		firstBudget = min(timeout, total/2)
	}
	firstCtx, firstCancel := context.WithTimeout(totalCtx, firstBudget)
	conn, err := decision.Outbound().DialContext(firstCtx, network, destination)
	firstCancel()
	if err == nil {
		return adapter.WrapSmartConn(conn, decision), nil
	}
	if standby == nil || !smartRetryable(err) || totalCtx.Err() != nil {
		return nil, err
	}
	s.mu.Lock()
	eligible := !s.closed && s.ctx.Err() == nil && (standby.state == "healthy" || standby.state == "probation" && standby.probation >= 1)
	token := standby.generation
	s.mu.Unlock()
	if !eligible {
		return nil, err
	}
	retryDecision, retryErr := s.resolveDecision(totalCtx, standby.outbound, []adapter.SmartSelection{{Group: s, Member: standby.tag, Generation: token}}, network, destination)
	if retryErr != nil {
		return nil, retryErr
	}
	conn, err = retryDecision.Outbound().DialContext(totalCtx, network, destination)
	if err != nil {
		return nil, err
	}
	return adapter.WrapSmartConn(conn, retryDecision), nil
}

func smartRetryable(err error) bool {
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, context.Canceled) {
		return false
	}
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "handshake") || strings.Contains(message, "tls")
}

func (s *Smart) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	decision, err := s.SelectMember(ctx, N.NetworkUDP, destination)
	if err != nil {
		return nil, err
	}
	conn, err := decision.Outbound().ListenPacket(ctx, destination)
	if err != nil {
		return nil, err
	}
	return adapter.WrapSmartPacketConn(conn, decision), nil
}

type smartLease struct {
	group                        *Smart
	member                       *smartMember
	closer                       io.Closer
	once                         sync.Once
	started, lastRead, lastWrite time.Time
	read, written                int64
	generation                   uint64
	stalled                      bool
}

func (s *Smart) Attach(decision adapter.SmartDecision, closer io.Closer) adapter.SmartLease {
	var selection adapter.SmartSelection
	found := false
	for _, entry := range decision.Selections() {
		if entry.Group == s {
			selection = entry
			found = true
			break
		}
	}
	lease := &smartLease{group: s, closer: closer}
	s.mu.Lock()
	now := s.clock.Now()
	lease.started, lease.lastRead, lease.lastWrite = now, now, now
	lease.generation = s.generation
	for _, member := range s.members {
		if member.tag == selection.Member {
			lease.member = member
			break
		}
	}
	valid := !s.closed && s.ctx.Err() == nil && found && lease.member != nil && lease.member.generation == selection.Generation
	if valid {
		if s.leases == nil {
			s.leases = make(map[*smartMember]map[*smartLease]struct{})
		}
		if s.leases[lease.member] == nil {
			s.leases[lease.member] = make(map[*smartLease]struct{})
		}
		s.leases[lease.member][lease] = struct{}{}
		s.lastDemand = now
	}
	s.mu.Unlock()
	if !valid {
		_ = closer.Close()
	}
	if valid {
		s.request("demand")
	}
	return lease
}

func (l *smartLease) Close() error {
	l.once.Do(func() { l.group.mu.Lock(); delete(l.group.leases[l.member], l); l.group.mu.Unlock() })
	return nil
}
func (l *smartLease) CountRead(n int)  { l.count(n, true) }
func (l *smartLease) CountWrite(n int) { l.count(n, false) }
func (l *smartLease) count(n int, read bool) {
	if n <= 0 {
		return
	}
	s := l.group
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	now := s.clock.Now()
	s.lastDemand = now
	if read {
		l.read += int64(n)
		l.lastRead = now
		l.stalled = false
	} else {
		l.written += int64(n)
		l.lastWrite = now
	}
	stall := !read && !l.stalled && l.member == s.primary && l.generation == s.generation && now.Sub(l.lastRead) >= 20*time.Second
	if stall {
		l.stalled = true
	}
	elapsed := now.Sub(l.started)
	if l.member != nil && l.generation == s.generation && elapsed >= 10*time.Second && now.Sub(l.lastRead) < 2*time.Second {
		rate := float64(l.read) * 8 / elapsed.Seconds() / 1e6
		if rate > 0 && (l.member.throughput.Source != "active" || l.member.throughput.At.Before(l.started)) && rate > l.member.throughput.Download {
			l.member.throughput = smartThroughput{Download: rate, At: now, Source: "passive"}
		}
	}
	s.mu.Unlock()
	if stall {
		s.request("stall")
	}
}

func (s *Smart) revokeLocked(member *smartMember) []io.Closer {
	s.memberGeneration++
	member.generation = s.memberGeneration
	member.queued = make(map[string]bool)
	member.inflight = false
	var closers []io.Closer
	if s.interrupt {
		for lease := range s.leases[member] {
			closers = append(closers, lease.closer)
		}
	}
	return closers
}

func (s *Smart) worker(ctx context.Context, reserved bool) {
	defer s.workers.Done()
	for {
		var job smartJob
		if reserved {
			select {
			case <-ctx.Done():
				return
			case job = <-s.healthJobs:
			}
		} else {
			select {
			case job = <-s.healthJobs:
			default:
				select {
				case <-ctx.Done():
					return
				case job = <-s.healthJobs:
				case job = <-s.otherJobs:
				}
			}
		}
		if ctx.Err() != nil {
			return
		}
		result := smartResult{member: job.member, kind: job.kind, operator: job.operator, confirm: job.confirm, generation: job.generation, memberGeneration: job.memberGeneration, configGeneration: job.configGeneration, reservation: job.reservation}
		workCtx, cancel := context.WithTimeout(job.ctx, job.timeout)
		if job.ctx.Err() != nil {
			result.err = job.ctx.Err()
		} else {
			switch job.kind {
			case "tcp":
				result.rtt, result.err = s.probe(workCtx, job.member.outbound, job.operator)
			case "udp", "udp443":
				result.rtt, result.err = s.udpProbe(workCtx, job.member.outbound, job.kind == "udp443")
			case "direct":
				result.rtt, result.err = s.directProbe(workCtx, job.country, s.directSeed)
			case "throughput":
				result.download, result.upload, result.bytes, result.err = smartMeasure(workCtx, job.member.outbound, s.options.Throughput.DownloadURL, s.options.Throughput.UploadURL, job.capBytes, job.networkType == "wifi" || s.cellularUploadAllowed())
			}
		}
		cancel()
		if job.kind == "throughput" && s.ledger != nil {
			s.mu.Lock()
			uncertain := job.generation != s.generation || job.networkType != s.network.Type
			s.mu.Unlock()
			result.err = errors.Join(result.err, s.ledger.Reconcile(job.reservation, result.bytes, uncertain))
		}
		select {
		case s.results <- result:
		case <-ctx.Done():
			return
		}
	}
}

func (s *Smart) cellularUploadAllowed() bool {
	return s.options.Throughput.CellularDailyBudgetMB != nil && *s.options.Throughput.CellularDailyBudgetMB >= 500
}

func (s *Smart) loop(ctx context.Context) {
	defer close(s.loopDone)
	for {
		if ctx.Err() != nil {
			return
		}
		bits := s.requestBits.Swap(0)
		for i, kind := range smartRequestNames {
			if bits&(1<<i) != 0 {
				s.handleRequest(smartRequest{kind})
			}
		}
		s.mu.Lock()
		now := s.clock.Now()
		closers := s.tickLocked(now)
		s.dispatchLocked()
		paused := s.paused
		s.mu.Unlock()
		for _, closer := range closers {
			_ = closer.Close()
		}
		s.requestHistoryFlush()
		var timer smartTimer
		var timerC <-chan time.Time
		if !paused {
			timer = s.clock.NewTimer(time.Second)
			timerC = timer.C()
		}
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return
		case result := <-s.results:
			s.mu.Lock()
			closers = s.applyResultLocked(result, s.clock.Now())
			s.mu.Unlock()
			for _, closer := range closers {
				_ = closer.Close()
			}
		case request := <-s.requests:
			s.handleRequest(request)
		case <-s.kick:
		case <-timerC:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

func (s *Smart) handleRequest(request smartRequest) {
	if request.kind == "network" || request.kind == "pause" || request.kind == "snapshot" {
		if request.kind != "snapshot" {
			smartForceNetwork(s.ctx)
		}
		network := smartReadNetwork(s.ctx, s.seed, s.clock.Now())
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		now := s.clock.Now()
		paused := s.pauseManager != nil && s.pauseManager.IsPaused()
		if paused && !s.paused {
			s.paused = true
			s.pauseStarted = now
			s.wakeEmergency = false
			if s.workCancel != nil {
				s.workCancel()
			}
			s.clearPendingLocked()
			for _, m := range s.members {
				m.inflight = false
				m.queued = make(map[string]bool)
			}
			s.directInflight = false
			s.updateNotificationLocked(now)
		}
		if paused {
			s.cancelURLWaitersLocked(context.Canceled)
		}
		if !paused && s.paused {
			s.paused = false
			s.pausedTotal += now.Sub(s.pauseStarted)
			s.workCtx, s.workCancel = context.WithCancel(s.ctx)
			s.backgroundCtx, s.backgroundCancel = context.WithCancel(s.workCtx)
			s.lastFullRound = time.Time{}
			s.lastNetworkRead = time.Time{}
			s.wakeEmergency = true
			s.nextEmergency = time.Time{}
			for _, member := range []*smartMember{s.primary, s.standby} {
				if member != nil {
					member.lastProbe = time.Time{}
				}
			}
		}
		if network.Fingerprint != s.network.Fingerprint || network.Link != s.network.Link {
			s.newGenerationLocked(network, now)
		} else {
			keyChanged := network.Key != s.network.Key
			s.network = network
			if keyChanged {
				s.restoreThroughputLocked(now)
				s.lastChallenger = time.Time{}
			}
		}
		s.timezone = time.FixedZone("", s.network.Offset)
		s.mu.Unlock()
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	var closers []io.Closer
	defer func() {
		s.mu.Unlock()
		for _, closer := range closers {
			_ = closer.Close()
		}
	}()
	now := s.clock.Now()
	switch request.kind {
	case "provider":
		if s.workCancel != nil {
			s.workCancel()
		}
		s.workCtx, s.workCancel = context.WithCancel(s.ctx)
		s.configGeneration++
		s.clearPendingLocked()
		old := slices.Clone(s.members)
		if err := s.loadMembers(); err != nil {
			if s.logger != nil {
				s.logger.Error("smart provider update: ", err)
			}
			return
		}
		for _, member := range old {
			if !slices.Contains(s.members, member) {
				closers = append(closers, s.revokeLocked(member)...)
			}
		}
		s.newGenerationLocked(s.network, now)
	case "udp443":
		s.lastUDP443Demand = now
		if s.lastDiscovery.IsZero() || now.Sub(s.lastDiscovery) >= 10*time.Minute {
			s.lastDiscovery = now
			for _, member := range s.members {
				if member.state == "healthy" {
					s.queueLocked(member, "udp443", 0, false, now)
				}
			}
		}
	case "stall":
		if s.primary != nil && !s.primary.inflight && (s.lastStall.IsZero() || now.Sub(s.lastStall) >= s.heartbeat) {
			s.lastStall = now
			s.queueLocked(s.primary, "tcp", int(s.generation%3), false, now)
		}
	case "force":
		s.lastFullRound = time.Time{}
		for _, member := range s.members {
			s.queueLocked(member, "tcp", int(s.generation%3), false, now)
		}
	}
}

func (s *Smart) newGenerationLocked(network smartNetwork, now time.Time) {
	if s.workCancel != nil {
		s.workCancel()
	}
	s.workCtx, s.workCancel = context.WithCancel(s.ctx)
	s.backgroundCtx, s.backgroundCancel = context.WithCancel(s.workCtx)
	s.clearPendingLocked()
	s.generation++
	s.selectionGeneration++
	s.network = network
	s.startedAt = now
	s.generationSucceeded = false
	s.offline = false
	s.ambiguousSince = time.Time{}
	s.allDeadSince = time.Time{}
	s.direct = smartEvidence{}
	s.directInflight = false
	s.lastDiscovery = time.Time{}
	s.lastDirect = time.Time{}
	s.lastFullRound = time.Time{}
	s.pending = nil
	s.nextEmergency = time.Time{}
	s.emergencyStep = 0
	s.wakeEmergency = false
	s.lastStall = time.Time{}
	s.challenger = nil
	s.timezone = time.FixedZone("", network.Offset)
	s.cancelURLWaitersLocked(context.Canceled)
	for _, member := range s.members {
		s.memberGeneration++
		member.generation = s.memberGeneration
		member.state = "unknown"
		member.tcp = smartEvidence{}
		member.udp = smartEvidence{}
		member.udp443 = smartEvidence{}
		member.inflight = false
		member.queued = make(map[string]bool)
		member.lastProbe = time.Time{}
		member.rtts = nil
		member.emergency = false
		member.probation = 0
	}
	s.restoreThroughputLocked(now)
	s.rankLocked(now)
	for rank, member := range s.members {
		member.startupRank = rank
	}
	if s.primary == nil && len(s.members) > 0 {
		s.primary = s.members[0]
	}
	s.eventLocked("network_change", "", "", network.Key, "default network changed", now)
}

func (s *Smart) clearPendingLocked() {
	for _, job := range s.pending {
		if job.reservation != "" && s.ledger != nil {
			_ = s.ledger.Reconcile(job.reservation, 0, false)
		}
	}
	s.pending = nil
}

func (s *Smart) restoreThroughputLocked(now time.Time) {
	for _, member := range s.members {
		member.throughput = smartThroughput{}
		member.lastThroughput = time.Time{}
		if stat := s.history.Stats[s.network.Key][member.identity]; stat != nil {
			member.rtts = slices.Clone(stat.RTTs)
			if stat.ThroughputAt > 0 {
				at := time.Unix(stat.ThroughputAt, 0)
				member.lastThroughput = at
				if now.Sub(at) < 24*time.Hour {
					member.throughput = smartThroughput{Download: stat.Download, Upload: stat.Upload, At: at, Source: "active"}
				}
			}
		}
	}
}

func (s *Smart) queueLocked(member *smartMember, kind string, operator int, confirm bool, now time.Time) {
	if s.closed || s.ctx.Err() != nil || s.paused || !s.network.Link {
		return
	}
	if member != nil {
		if member.queued == nil {
			member.queued = make(map[string]bool)
		}
		if member.queued[kind] || kind == "tcp" && member.inflight {
			return
		}
		member.queued[kind] = true
		if kind == "tcp" {
			member.inflight = true
			member.lastProbe = now
		}
	}
	if kind == "direct" {
		if s.directInflight {
			return
		}
		s.directInflight = true
		s.lastDirect = now
	}
	ctx := s.workCtx
	if ctx == nil {
		ctx = s.ctx
	}
	job := smartJob{member: member, kind: kind, operator: operator, confirm: confirm, generation: s.generation, configGeneration: s.configGeneration, ctx: ctx, timeout: 8 * time.Second, country: s.network.Country, networkType: s.network.Type}
	if member != nil {
		job.memberGeneration = member.generation
		job.timeout = s.memberTimeoutLocked(member)
		job.urgent = kind == "tcp" && (member == s.primary || member == s.standby || !s.generationSucceeded && member.startupRank < 2)
	}
	if !job.urgent && s.backgroundCtx != nil {
		job.ctx = s.backgroundCtx
	}
	if len(s.pending) >= 256 {
		if member != nil {
			delete(member.queued, kind)
			if kind == "tcp" {
				member.inflight = false
			}
		}
		if kind == "direct" {
			s.directInflight = false
		}
		return
	}
	s.pending = append(s.pending, job)
}

func smartJobPriority(job smartJob) int {
	if job.urgent {
		return 0
	}
	if job.kind == "udp443" {
		return 1
	}
	if job.kind == "direct" {
		return 2
	}
	if job.kind == "tcp" || job.kind == "udp" {
		return 3
	}
	return 4
}

func (s *Smart) dispatchLocked() {
	slices.SortStableFunc(s.pending, func(a, b smartJob) int { return smartJobPriority(a) - smartJobPriority(b) })
	for len(s.pending) > 0 {
		job := s.pending[0]
		channel := s.otherJobs
		if job.urgent {
			channel = s.healthJobs
		}
		select {
		case channel <- job:
			s.pending = s.pending[1:]
		default:
			return
		}
	}
}

func (s *Smart) memberIntervalLocked(member *smartMember, now time.Time) time.Duration {
	if now.Sub(s.lastDemand) <= 30*time.Second {
		if member == s.primary {
			return s.heartbeat
		}
		if member == s.standby {
			return s.standbyInterval
		}
	}
	if now.Sub(s.lastDemand) <= 10*time.Minute {
		return s.backgroundInterval
	}
	return 30 * time.Minute
}

func (s *Smart) tickLocked(now time.Time) []io.Closer {
	if s.closed || s.ctx.Err() != nil || s.paused {
		return nil
	}
	if s.network.Link && now.Sub(s.lastNetworkRead) >= 30*time.Second && now.Sub(s.lastDemand) <= 30*time.Second {
		s.lastNetworkRead = now
		s.request("snapshot")
	}
	if !s.network.Link {
		s.offline = true
		s.updateNotificationLocked(now)
		return nil
	}
	demand := now.Sub(s.lastDemand) <= 30*time.Second
	if demand {
		for _, member := range []*smartMember{s.primary, s.standby} {
			if member == nil || member.state == "quarantined" {
				continue
			}
			interval := s.heartbeat
			if member != s.primary {
				interval = s.standbyInterval
			}
			if member.lastProbe.IsZero() || now.Sub(member.lastProbe) >= interval {
				s.queueLocked(member, "tcp", int(s.generation%3), false, now)
			}
		}
	}
	if s.lastFullRound.IsZero() || now.Sub(s.lastFullRound) >= 2*time.Second {
		s.lastFullRound = now
		for _, member := range s.members {
			if member.state == "quarantined" && !now.Before(member.quarantineUntil) {
				member.state = "probation"
				member.probation = 0
				member.lastProbation = time.Time{}
			}
			interval := s.memberIntervalLocked(member, now)
			if member.state == "unknown" {
				interval = 2 * time.Second
			}
			if member.state == "probation" {
				interval = 20 * time.Second
			}
			if member.state != "quarantined" && (member.lastProbe.IsZero() || now.Sub(member.lastProbe) >= interval) {
				s.queueLocked(member, "tcp", int((s.generation+uint64(now.Unix()/15))%3), false, now)
			}
		}
	}
	if demand && s.primary != nil && s.udpEnabled {
		if now.Sub(s.primary.lastUDP) >= 5*time.Minute {
			s.primary.lastUDP = now
			s.queueLocked(s.primary, "udp", 0, false, now)
		}
		if now.Sub(s.primary.lastUDP443) >= 5*time.Minute {
			s.primary.lastUDP443 = now
			s.queueLocked(s.primary, "udp443", 0, false, now)
		}
	}
	if demand && (s.lastDirect.IsZero() || now.Sub(s.lastDirect) >= 5*time.Minute) {
		s.queueLocked(nil, "direct", 0, false, now)
	}
	if s.hasEligiblePrimaryLocked(now) {
		s.wakeEmergency = false
	}
	if !s.hasEligiblePrimaryLocked(now) && now.Sub(s.startedAt) >= 10*time.Second && (s.lastEmergency.IsZero() || now.Sub(s.lastEmergency) >= 2*time.Second) && (s.nextEmergency.IsZero() || !now.Before(s.nextEmergency)) && (s.wakeEmergency || demand || now.Sub(s.startedAt) <= 30*time.Second) {
		s.lastEmergency = now
		for _, member := range s.members {
			s.queueLocked(member, "tcp", int(s.generation%3), false, now)
		}
		delays := []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 30 * time.Second}
		s.nextEmergency = now.Add(delays[min(s.emergencyStep, len(delays)-1)])
		s.emergencyStep++
		s.eventLocked("emergency", "", "", s.network.Key, "no eligible primary", now)
	}
	s.chooseLocked(now)
	s.scheduleThroughputLocked(now)
	s.updateNotificationLocked(now)
	return nil
}

func (s *Smart) memberTimeoutLocked(member *smartMember) time.Duration {
	value := 2500 * time.Millisecond
	if member != nil && len(member.rtts) > 0 {
		sorted := slices.Clone(member.rtts)
		slices.Sort(sorted)
		p90 := sorted[(len(sorted)-1)*9/10]
		value = time.Duration(3*p90)*time.Millisecond + time.Second
	}
	if member == nil || member.lastOK.Before(s.startedAt) {
		value = max(value, 6*time.Second)
	}
	if s.timeout > 0 {
		value = max(value, s.timeout)
	}
	return min(max(value, 2500*time.Millisecond), 8*time.Second)
}

func (s *Smart) applyResultLocked(result smartResult, now time.Time) []io.Closer {
	if s.closed || s.ctx.Err() != nil || result.generation != s.generation || result.configGeneration != s.configGeneration || s.paused {
		return nil
	}
	member := result.member
	if member != nil && member.generation != result.memberGeneration {
		return nil
	}
	if member != nil {
		delete(member.queued, result.kind)
		if result.kind == "tcp" {
			member.inflight = false
		}
	}
	if result.kind == "direct" {
		s.directInflight = false
	}
	if result.kind == "tcp" && (result.err == nil || result.confirm || errors.Is(result.err, context.Canceled) || errors.Is(result.err, errSmartResolve)) {
		s.finishURLWaitersLocked(result)
	}
	if errors.Is(result.err, context.Canceled) || errors.Is(result.err, errSmartResolve) || !s.network.Link {
		return nil
	}
	state := "ok"
	if result.err != nil {
		state = "failed"
	}
	switch result.kind {
	case "direct":
		s.direct = smartEvidence{state: state, at: now, expires: now.Add(2 * time.Minute)}
		s.directRTT = result.rtt
		if state == "failed" && !s.generationSucceeded && s.ambiguousSince.IsZero() {
			s.ambiguousSince = now
		}
	case "udp", "udp443":
		evidence := smartEvidence{state: state, at: now, expires: now.Add(30 * time.Minute)}
		if result.kind == "udp443" {
			evidence.expires = now.Add(10 * time.Minute)
			member.udp443 = evidence
			s.selectionGeneration++
		} else {
			member.udp = evidence
		}
	case "throughput":
		member.lastThroughput = now
		if result.err == nil {
			member.throughput = smartThroughput{Download: result.download, Upload: result.upload, At: now, Source: "active"}
			member.lastThroughput = now
			s.eventLocked("speedtest", "", member.tag, s.network.Key, "active throughput measurement", now)
			s.dirty = true
			for _, key := range s.network.Path {
				stat := s.history.statistic(key, member.identity, now)
				stat.Download = result.download
				stat.Upload = result.upload
				stat.ThroughputAt = now.Unix()
				points := s.history.Timeline[key][member.identity]
				if len(points) > 0 {
					points[len(points)-1].MBPS = result.download
				}
			}
			if s.challenger == member && result.download >= s.targetMbps && !now.After(s.challengerUntil) && s.demandChallengerEligibleLocked(member, now) {
				s.switchLocked(member, "switch", "demand challenger meets target", now)
				s.challenger = nil
			}
		}
	case "tcp":
		if result.err == nil {
			wasHealthy := member.state == "healthy"
			member.lastOK = now
			member.tcp = smartEvidence{state: "ok", at: now, expires: now.Add(2 * s.memberIntervalLocked(member, now))}
			member.rtts = append(member.rtts, result.rtt.Seconds()*1000)
			if len(member.rtts) > 64 {
				member.rtts = member.rtts[len(member.rtts)-64:]
			}
			if member.state == "probation" || member.state == "quarantined" {
				member.state = "probation"
				if member.lastProbation.IsZero() || now.Sub(member.lastProbation) >= 20*time.Second {
					member.probation++
					member.lastProbation = now
				}
				if member.probation >= 3 {
					member.state = "healthy"
					member.emergency = false
					member.healthySince = now
					s.eventLocked("recover", "", member.tag, s.network.Key, "three half-open successes", now)
				}
			} else {
				member.state = "healthy"
				if !wasHealthy {
					member.healthySince = now
				}
			}
			if !s.hasEligiblePrimaryLocked(now) {
				member.emergency = member.state != "healthy"
				s.switchLocked(member, "recover", "first reachable member", now)
			}
			if !s.generationSucceeded {
				s.generationSucceeded = true
				if member.state == "healthy" {
					s.switchLocked(member, "recover", "first success this network", now)
				}
			}
			if member.quarantineLevel > 0 && !member.quarantineDecayAt.IsZero() {
				steps := int(now.Sub(member.quarantineDecayAt) / (6 * time.Hour))
				if steps > 0 {
					member.quarantineLevel = max(member.quarantineLevel-steps, 0)
					member.quarantineDecayAt = member.quarantineDecayAt.Add(time.Duration(steps) * 6 * time.Hour)
				}
			}
			s.recordObservationLocked(member, true, result.rtt, now)
			if s.urlHistory != nil {
				s.urlHistory.StoreURLTestHistory(member.tag, &adapter.URLTestHistory{Time: now, Delay: uint16(min(result.rtt.Milliseconds(), 65535))})
			}
			s.offline = false
			s.ambiguousSince = time.Time{}
		} else {
			if !s.generationSucceeded && s.direct.value(now) == "failed" {
				if s.ambiguousSince.IsZero() {
					s.ambiguousSince = now
				}
				if now.Sub(s.ambiguousSince) >= time.Minute {
					s.offline = true
				}
				return nil
			}
			if !result.confirm {
				if member.state == "healthy" {
					member.state = "suspect"
				}
				if member == s.primary && s.backgroundCancel != nil {
					s.backgroundCancel()
					s.backgroundCtx, s.backgroundCancel = context.WithCancel(s.workCtx)
				}
				s.queueLocked(member, "tcp", (result.operator+1)%3, true, now)
				if member == s.primary && s.standby != nil {
					s.queueLocked(s.standby, "tcp", (result.operator+1)%3, false, now)
				}
				s.queueLocked(nil, "direct", 0, false, now)
				s.notifyUpdatedLocked()
				return nil
			}
			member.lastFail = now
			member.quarantineDecayAt = now
			member.tcp = smartEvidence{state: "failed", at: now, expires: now.Add(2 * s.heartbeat)}
			member.emergency = false
			member.state = "quarantined"
			member.probation = 0
			member.healthySince = time.Time{}
			backoff := min(30*time.Second*time.Duration(1<<min(member.quarantineLevel, 5)), 15*time.Minute)
			member.quarantineUntil = now.Add(backoff)
			member.quarantineLevel = min(member.quarantineLevel+1, 8)
			s.recordObservationLocked(member, false, 0, now)
			s.eventLocked("quarantine", member.tag, "", s.network.Key, "two probe operators failed", now)
			closers := s.revokeLocked(member)
			if member == s.primary {
				s.primary = nil
				s.chooseLocked(now)
				s.eventLocked("failover", member.tag, s.primaryTagLocked(), s.network.Key, "primary corroborated unreachable", now)
			}
			s.selectionGeneration++
			s.notifyUpdatedLocked()
			return closers
		}
	}
	s.chooseLocked(now)
	s.updateNotificationLocked(now)
	s.notifyUpdatedLocked()
	return nil
}

func (s *Smart) recordObservationLocked(member *smartMember, success bool, rtt time.Duration, now time.Time) {
	s.history.IdentityServers[member.identity] = member.serverIdentity
	s.history.Observe(s.network.Path, member.identity, success, rtt.Seconds()*1000, s.historyTime(now), s.pausedTotal)
	for index, key := range s.network.Path {
		label := s.network.Labels[key]
		parent := ""
		if index+1 < len(s.network.Path) {
			parent = s.network.Path[index+1]
		}
		s.history.Networks[key] = &smartHistoryNetwork{Type: s.network.Type, Label: label, Carrier: s.network.Carrier, Radio: s.network.Radio, LastSeen: now.Unix(), Parent: parent}
		if label != "" {
			s.history.Labels[key] = label
		}
	}
	s.dirty = true
}

func (s *Smart) hasEligiblePrimaryLocked(now time.Time) bool {
	return s.primary != nil && (s.primary.state == "healthy" || s.primary.state == "suspect" || s.primary.emergency || s.primary.state == "unknown" && now.Sub(s.startedAt) < 10*time.Second)
}

func (s *Smart) primaryTagLocked() string {
	if s.primary != nil {
		return s.primary.tag
	}
	return ""
}
func (s *Smart) priorsLocked() []smartPrior { return s.priors }
func (s *Smart) historyTime(now time.Time) time.Time {
	if s.timezone != nil {
		return now.In(s.timezone)
	}
	return now
}

func smartRTTQuantiles(values []float64) (p50, p90 float64) {
	if len(values) == 0 {
		return 0, 0
	}
	var buffer [64]float64
	n := copy(buffer[:], values[max(len(values)-64, 0):])
	sorted := buffer[:n]
	slices.Sort(sorted)
	return sorted[n/2], sorted[(n-1)*9/10]
}

func (s *Smart) rankLocked(now time.Time) {
	priors := s.priorsLocked()
	previousCountry := ""
	if s.primary != nil {
		previousCountry = s.primary.country
	}
	for _, member := range s.members {
		member.rankState = 3
		if member.state == "healthy" {
			member.rankState = 0
		} else if member.emergency {
			member.rankState = 1
		} else if member.state == "suspect" {
			member.rankState = 2
		}
		member.rankStability = s.history.Class(s.network.Path, member.identity, member.serverIdentity, s.historyTime(now), priors)
		member.rankThroughput = s.throughputClass(member, now)
		_, member.rankLatency = smartRTTQuantiles(member.rtts)
		if len(member.rtts) == 0 {
			member.rankLatency = math.Inf(1)
		}
	}
	slices.SortStableFunc(s.members, func(a, b *smartMember) int {
		if a.tier != b.tier {
			return a.tier - b.tier
		}
		if d := a.rankState - b.rankState; d != 0 {
			return d
		}
		if d := a.rankStability - b.rankStability; d != 0 {
			return d
		}
		if d := a.rankThroughput - b.rankThroughput; d != 0 {
			return d
		}
		if a.rankLatency != b.rankLatency {
			if a.rankLatency < b.rankLatency {
				return -1
			}
			return 1
		}
		if previousCountry != "" && a.country != b.country {
			if a.country == previousCountry {
				return -1
			}
			if b.country == previousCountry {
				return 1
			}
		}
		return a.configOrder - b.configOrder
	})
}

func (s *Smart) throughputClass(member *smartMember, now time.Time) int {
	value := member.throughput
	ttl := 24 * time.Hour
	if value.Source == "passive" {
		ttl = 6 * time.Hour
	}
	if value.At.IsZero() || now.Sub(value.At) >= ttl {
		return 1
	}
	ratio := value.Download / s.targetMbps
	if ratio >= 1 {
		return 0
	}
	if value.Source == "passive" {
		return 1
	}
	if ratio >= .75 {
		return 2
	}
	if ratio >= .5 {
		return 3
	}
	if ratio >= .25 {
		return 4
	}
	return 5
}

func (s *Smart) chooseLocked(now time.Time) {
	s.rankLocked(now)
	if !s.hasEligiblePrimaryLocked(now) {
		for _, member := range s.members {
			if member.state == "healthy" || member.emergency {
				s.switchLocked(member, "failover", "best reachable member", now)
				break
			}
		}
	}
	s.standby = nil
	for _, member := range s.members {
		if member != s.primary && member.state == "healthy" {
			s.standby = member
			break
		}
	}
	if s.standby == nil {
		for _, member := range s.members {
			if member != s.primary && member.state == "probation" && member.probation >= 1 {
				s.standby = member
				break
			}
		}
	}
	if s.primary == nil {
		return
	}
	for _, candidate := range s.members {
		if candidate == s.primary || candidate.state != "healthy" {
			continue
		}
		if candidate.tier < s.primary.tier && now.Sub(candidate.healthySince) >= time.Minute {
			s.switchLocked(candidate, "tier_restore", "preferred tier healthy for 60 seconds", now)
			return
		}
		candidateStability := candidate.rankStability
		primaryStability := s.primary.rankStability
		better := candidateStability < primaryStability || candidateStability == primaryStability && s.throughputClass(candidate, now) < s.throughputClass(s.primary, now)
		signature := candidateStability*10 + s.throughputClass(candidate, now)
		if !better {
			candidate.improvementAt = time.Time{}
			candidate.improvementObservations = 0
			continue
		}
		observation := candidate.lastOK
		if candidate.throughput.At.After(observation) {
			observation = candidate.throughput.At
		}
		if candidate.improvementAt.IsZero() || signature != candidate.improvementSignature {
			candidate.improvementAt = observation
			candidate.improvementObservations = 1
			candidate.improvementSignature = signature
		} else if observation.Sub(candidate.improvementAt) >= 30*time.Second {
			candidate.improvementAt = observation
			candidate.improvementObservations++
		}
		if candidate.tier == s.primary.tier && now.Sub(s.selectedAt) >= 10*time.Minute && candidate.improvementObservations >= 2 {
			s.switchLocked(candidate, "switch", "confirmed stability or throughput improvement", now)
			return
		}
	}
}

func (s *Smart) switchLocked(member *smartMember, kind, reason string, now time.Time) {
	if s.primary == member {
		return
	}
	previous := s.primaryTagLocked()
	s.primary = member
	s.selectedAt = now
	s.selectionGeneration++
	s.eventLocked(kind, previous, member.tag, s.network.Key, reason, now)
	s.notifyUpdatedLocked()
}

func (s *Smart) eventLocked(kind, from, to, network, reason string, now time.Time) {
	pointer := func(value string) *string {
		if value == "" {
			return nil
		}
		return &value
	}
	s.history.Events = append(s.history.Events, smartEvent{Time: now.Unix(), Kind: kind, From: pointer(from), To: pointer(to), Network: pointer(network), Reason: reason})
	if len(s.history.Events) > 500 {
		s.history.Events = s.history.Events[len(s.history.Events)-500:]
	}
	s.dirty = true
}

func (s *Smart) notifyUpdatedLocked() {
	if s.urlHistory != nil {
		s.urlHistory.NotifyUpdated()
	}
}

func (s *Smart) demandChallengerEligibleLocked(member *smartMember, now time.Time) bool {
	if s.primary == nil || member == nil || member == s.primary || member.state != "healthy" || member.tier != s.primary.tier {
		return false
	}
	at := s.historyTime(now)
	return s.history.Class(s.network.Path, member.identity, member.serverIdentity, at, s.priors) <= s.history.Class(s.network.Path, s.primary.identity, s.primary.serverIdentity, at, s.priors)
}

func (s *Smart) scheduleThroughputLocked(now time.Time) {
	if !s.throughputEnabled || s.ledger == nil || s.network.PowerSave || s.paused || s.primary == nil || !s.generationSucceeded {
		return
	}
	busy := false
	for _, entries := range s.leases {
		for lease := range entries {
			if now.Sub(lease.lastRead) < 2*time.Second || now.Sub(lease.lastWrite) < 2*time.Second {
				busy = true
			}
		}
	}
	if busy {
		if s.demandStarted.IsZero() {
			s.demandStarted = now
		}
		if s.primary.throughput.Source == "active" && s.throughputClass(s.primary, now) >= 2 && now.Sub(s.demandStarted) >= 20*time.Second && s.challenger == nil {
			for _, member := range s.members {
				if s.demandChallengerEligibleLocked(member, now) {
					s.challenger = member
					s.challengerUntil = now.Add(5 * time.Minute)
					break
				}
			}
		}
		return
	}
	s.demandStarted = time.Time{}
	if s.challenger != nil && (now.After(s.challengerUntil) || !s.demandChallengerEligibleLocked(s.challenger, now)) {
		s.challenger = nil
	}
	var candidate *smartMember
	if s.challenger != nil {
		candidate = s.challenger
	} else {
		ordered := []*smartMember{s.primary, s.standby}
		ordered = append(ordered, s.members...)
		oldest := now
		for _, member := range ordered {
			if member == nil || member.state != "healthy" {
				continue
			}
			if member.throughput.Source != "active" || member.throughput.At.IsZero() || now.Sub(member.throughput.At) >= 24*time.Hour {
				if candidate == nil || member.lastThroughput.Before(oldest) {
					candidate = member
					oldest = member.lastThroughput
				}
			}
		}
	}
	if candidate != nil && candidate != s.primary && candidate != s.standby && s.challenger != candidate && !s.lastChallenger.IsZero() && now.Sub(s.lastChallenger) < time.Hour {
		return
	}
	if candidate == nil || candidate.queued["throughput"] {
		return
	}
	id, capBytes, err := s.ledger.Reserve(s.ctx, s.network.Type, now, s.network.Offset)
	if err != nil {
		return
	}
	if candidate != s.primary && candidate != s.standby {
		s.lastChallenger = now
	}
	s.queueLocked(candidate, "throughput", 0, false, now)
	for i := len(s.pending) - 1; i >= 0; i-- {
		if s.pending[i].member == candidate && s.pending[i].kind == "throughput" {
			s.pending[i].reservation = id
			s.pending[i].capBytes = capBytes
			s.pending[i].timeout = 30 * time.Second
			return
		}
	}
	_ = s.ledger.Reconcile(id, 0, false)
}

func (s *Smart) updateNotificationLocked(now time.Time) {
	allFailed := len(s.members) > 0
	for _, member := range s.members {
		if member.lastFail.Before(s.startedAt) || !member.lastFail.After(member.lastOK) {
			allFailed = false
			break
		}
	}
	condition := !s.closed && s.network.Link && !s.paused && allFailed && s.direct.value(now) == "ok"
	if condition {
		if s.allDeadSince.IsZero() {
			s.allDeadSince = now
		}
	} else {
		s.allDeadSince = time.Time{}
	}
	active := condition && now.Sub(s.allDeadSince) >= 45*time.Second
	platform := service.FromContext[adapter.PlatformInterface](s.ctx)
	if active && !s.notified && s.notify && platform != nil {
		err := platform.SendNotification(&adapter.Notification{Identifier: "smart/" + s.Tag(), TypeName: "smart", TypeID: 7401, Title: "VPN servers unreachable — direct internet works"})
		if err == nil {
			s.notified = true
			s.eventLocked("all_dead", "", "", s.network.Key, "all members failed while direct internet works", now)
		}
	}
	if !active && s.notified {
		if platform != nil {
			_ = platform.CancelNotification("smart/"+s.Tag(), 7401)
		}
		s.notified = false
	}
}

func cloneSmartHistory(history *smartHistory) *smartHistory {
	snapshot := *history
	snapshot.Networks = maps.Clone(history.Networks)
	for key, network := range snapshot.Networks {
		if network != nil {
			value := *network
			snapshot.Networks[key] = &value
		}
	}
	snapshot.Stats = maps.Clone(history.Stats)
	for key, statistics := range snapshot.Stats {
		cloned := maps.Clone(statistics)
		for identity, statistic := range cloned {
			if statistic != nil {
				value := *statistic
				value.RTTs = slices.Clone(statistic.RTTs)
				value.Flaps = slices.Clone(statistic.Flaps)
				cloned[identity] = &value
			}
		}
		snapshot.Stats[key] = cloned
	}
	snapshot.Events = slices.Clone(history.Events)
	for index := range snapshot.Events {
		event := &snapshot.Events[index]
		for _, pointer := range []**string{&event.From, &event.To, &event.Network} {
			if *pointer != nil {
				value := **pointer
				*pointer = &value
			}
		}
	}
	snapshot.Labels = maps.Clone(history.Labels)
	snapshot.IdentityServers = maps.Clone(history.IdentityServers)
	snapshot.recency = maps.Clone(history.recency)
	snapshot.Timeline = maps.Clone(history.Timeline)
	for key, members := range snapshot.Timeline {
		cloned := maps.Clone(members)
		for identity, points := range cloned {
			cloned[identity] = slices.Clone(points)
		}
		snapshot.Timeline[key] = cloned
	}
	return &snapshot
}

func (s *Smart) startHistoryPersistenceLocked() {
	if s.persistenceDone != nil {
		return
	}
	s.persistenceKick = make(chan struct{}, 1)
	s.persistenceStop = make(chan struct{})
	s.persistenceDone = make(chan struct{})
	go func() {
		defer close(s.persistenceDone)
		for {
			select {
			case <-s.persistenceStop:
				s.flushHistory(true)
				return
			case <-s.persistenceKick:
				s.flushHistory(false)
			}
		}
	}()
}

func (s *Smart) requestHistoryFlush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cache == nil || s.closed || !s.dirty || s.clock.Now().Sub(s.lastWrite) < time.Minute {
		return
	}
	s.startHistoryPersistenceLocked()
	select {
	case s.persistenceKick <- struct{}{}:
	default:
	}
}

func (s *Smart) flushHistory(force bool) {
	if s.cache == nil {
		return
	}
	s.persistenceMu.Lock()
	defer s.persistenceMu.Unlock()
	s.mu.Lock()
	now := s.clock.Now()
	if !s.dirty || !force && now.Sub(s.lastWrite) < time.Minute {
		s.mu.Unlock()
		return
	}
	snapshot := cloneSmartHistory(s.history)
	s.dirty = false
	s.lastWrite = now
	s.mu.Unlock()
	data, err := snapshot.Encode()
	if err == nil {
		err = s.cache.SaveSmartHistory(s.Tag(), &adapter.SmartHistory{Data: data})
	}
	if err != nil {
		s.mu.Lock()
		s.dirty = true
		s.mu.Unlock()
		if s.logger != nil {
			s.logger.Error("save smart history: ", err)
		}
	}
}

func (s *Smart) URLTest(ctx context.Context) (map[string]uint16, error) {
	waiter := &smartURLWaiter{remaining: make(map[*smartMember]bool), result: make(map[string]uint16), done: make(chan struct{})}
	s.mu.Lock()
	if s.closed || s.ctx.Err() != nil {
		s.mu.Unlock()
		return nil, net.ErrClosed
	}
	if s.paused || !s.network.Link {
		s.mu.Unlock()
		return nil, E.New("smart probes unavailable while paused or offline")
	}
	for _, member := range s.members {
		waiter.remaining[member] = true
	}
	s.urlWaiters = append(s.urlWaiters, waiter)
	s.mu.Unlock()
	s.request("force")
	select {
	case <-waiter.done:
		return waiter.result, waiter.err
	case <-ctx.Done():
		s.mu.Lock()
		s.urlWaiters = slices.DeleteFunc(s.urlWaiters, func(other *smartURLWaiter) bool { return other == waiter })
		s.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (s *Smart) finishURLWaitersLocked(result smartResult) {
	for _, waiter := range s.urlWaiters {
		if !waiter.remaining[result.member] {
			continue
		}
		delete(waiter.remaining, result.member)
		if result.err == nil {
			waiter.result[result.member.tag] = uint16(min(result.rtt.Milliseconds(), 65535))
		}
		if len(waiter.remaining) == 0 {
			close(waiter.done)
		}
	}
	s.urlWaiters = slices.DeleteFunc(s.urlWaiters, func(waiter *smartURLWaiter) bool { return len(waiter.remaining) == 0 })
}

func (s *Smart) cancelURLWaitersLocked(err error) {
	for _, waiter := range s.urlWaiters {
		waiter.err = err
		close(waiter.done)
	}
	s.urlWaiters = nil
}

func (s *Smart) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	for _, member := range s.members {
		s.memberGeneration++
		member.generation = s.memberGeneration
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.workCancel != nil {
		s.workCancel()
	}
	s.clearPendingLocked()
	s.cancelURLWaitersLocked(context.Canceled)
	s.updateNotificationLocked(s.clock.Now())
	var closers []io.Closer
	for _, entries := range s.leases {
		for lease := range entries {
			closers = append(closers, lease.closer)
		}
	}
	s.mu.Unlock()
	for _, closer := range closers {
		_ = closer.Close()
	}
	if s.loopDone != nil {
		<-s.loopDone
	}
	if s.pauseManager != nil && s.pauseCallback != nil {
		s.pauseManager.UnregisterCallback(s.pauseCallback)
	}
	for provider, callback := range s.providerCallbacks {
		provider.UnregisterCallback(callback)
	}
	s.workers.Wait()
	s.mu.Lock()
	if s.cache != nil {
		s.startHistoryPersistenceLocked()
		close(s.persistenceStop)
	}
	persistenceDone := s.persistenceDone
	s.mu.Unlock()
	if persistenceDone != nil {
		<-persistenceDone
	}
	if s.ledger != nil {
		s.ledger.Unregister(s.Tag())
	}
	return nil
}
