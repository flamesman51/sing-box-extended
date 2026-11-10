package group

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func TestSmartPanicScrubbing(t *testing.T) {
	message := smartScrubMessage("probe https://secret.example.test/token from 192.0.2.4:443 [2001:db8::1]:53 host=router failed")
	for _, secret := range []string{"secret.example.test", "token", "192.0.2.4", "2001:db8::1", "router"} {
		if strings.Contains(message, secret) {
			t.Fatalf("panic message leaked %q: %s", secret, message)
		}
	}
	for _, replacement := range []string{"<url>", "<ip>", "<host>"} {
		if !strings.Contains(message, replacement) {
			t.Fatalf("missing %s in %s", replacement, message)
		}
	}
	stack := smartScrubStack([]byte("goroutine 17 [running]:\nexample.test/module/group.(*Smart).worker(0xc000012340, {0x123, 0x456})\n\t/private/build/group/smart.go:789 +0x123\ncreated by example.test/module/group.(*Smart).Start in goroutine 1\n\t/private/build/group/smart.go:362 +0x456\n"))
	if stack != "group.(*Smart).worker\nsmart.go:789\ngroup.(*Smart).Start\nsmart.go:362" {
		t.Fatalf("unexpected scrubbed stack: %q", stack)
	}
	if len(smartScrubMessage(strings.Repeat("é", 1000))) > 512 {
		t.Fatal("message exceeded byte limit")
	}
	for _, value := range []string{"dial tcp router:443", "lookup gateway on 192.0.2.53:53", "failed [fe80::1%wlan0]:443", "failed ::ffff:192.0.2.4"} {
		message := smartScrubMessage(value)
		for _, private := range []string{"router", "gateway", "192.0.2.53", "fe80", "wlan0", "ffff", "192.0.2.4"} {
			if strings.Contains(message, private) {
				t.Fatalf("panic message leaked %q: %s", private, message)
			}
		}
	}
}

func TestSmartContainedOperationsLeaveHealthUnchanged(t *testing.T) {
	for _, kind := range []string{"tcp", "udp", "udp443", "direct", "throughput"} {
		t.Run(kind, func(t *testing.T) {
			s, clock := smartTestGroup()
			defer s.Close()
			member := s.primary
			if kind != "direct" {
				member.queued = map[string]bool{kind: true}
			}
			member.inflight = kind == "tcp"
			waiter := &smartURLWaiter{remaining: map[*smartMember]bool{member: true}, result: make(map[string]uint16), done: make(chan struct{})}
			if kind == "tcp" {
				s.urlWaiters = append(s.urlWaiters, waiter)
			}
			s.testHook = func(site string) {
				if site == "probe/"+kind {
					panic("probe secret.example.test 192.0.2.1")
				}
			}
			job := smartJob{member: member, kind: kind, ctx: s.ctx, timeout: time.Second, generation: s.generation, configGeneration: s.configGeneration, memberGeneration: member.generation}
			if kind == "direct" {
				job.member = nil
				s.directInflight = true
			}
			result := s.runJob(job)
			if !result.contained || !errors.Is(result.err, errSmartContained) {
				t.Fatalf("result not contained: %+v", result)
			}
			s.applyResultLocked(result, clock.Now())
			if member.state != "healthy" || member.inflight || member.queued[kind] || member.lastFail != (time.Time{}) || member.throughput.Source != "" || s.directInflight {
				t.Fatalf("contained operation changed health or left work queued: %+v", member)
			}
			if kind == "tcp" {
				select {
				case <-waiter.done:
				default:
					t.Fatal("contained probe stranded URL waiter")
				}
				if len(waiter.result) != 0 || !errors.Is(waiter.err, errSmartContained) {
					t.Fatal("contained probe fabricated latency")
				}
			}
			snapshot := s.panicSnapshot()
			if snapshot.Total != 1 || strings.Contains(snapshot.Recent[0].Message, "secret.example.test") || strings.Contains(snapshot.Recent[0].Message, "192.0.2.1") {
				t.Fatalf("unsafe panic snapshot: %+v", snapshot)
			}
			conn, err := s.DialContext(context.Background(), "tcp", M.Socksaddr{})
			if err != nil {
				t.Fatalf("contained probe stopped traffic: %v", err)
			}
			_ = conn.Close()
		})
	}
}

func TestSmartWorkerSurvivesIndividualProbePanics(t *testing.T) {
	s, _ := smartTestGroup()
	defer s.Close()
	s.healthJobs, s.otherJobs = make(chan smartJob, 2), make(chan smartJob, 1)
	var calls atomic.Int32
	s.probe = func(context.Context, N.Dialer, int) (time.Duration, error) {
		if calls.Add(1) == 1 {
			panic("first operation")
		}
		return time.Millisecond, nil
	}
	s.workers.Add(1)
	go s.worker(s.ctx, true)
	for range 2 {
		s.healthJobs <- smartJob{kind: "tcp", member: s.primary, ctx: s.ctx, timeout: time.Second}
	}
	if result := smartLedgerWait(t, s.results); !result.contained {
		t.Fatal("first result was not contained")
	}
	if result := smartLedgerWait(t, s.results); result.contained || result.err != nil || result.rtt != time.Millisecond {
		t.Fatalf("persistent worker did not resume: %+v", result)
	}
}

func TestSmartContainedThroughputSettlesReservation(t *testing.T) {
	for _, started := range []bool{false, true} {
		t.Run(map[bool]string{false: "unstarted", true: "started"}[started], func(t *testing.T) {
			s, clock := smartTestGroup()
			defer s.Close()
			ledger, err := getSmartLedger(new(smartLedgerTestCache))
			if err != nil {
				t.Fatal(err)
			}
			ledger.Register(s.Tag(), map[string]float64{"wifi": 100})
			s.ledger = ledger
			id, capBytes, err := ledger.Reserve(s.ctx, "wifi", clock.Now(), 0)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(s.ctx)
			defer cancel()
			if !started {
				cancel()
			}
			s.testHook = func(site string) {
				if site == "probe/throughput" {
					panic("measurement interrupted")
				}
			}
			result := s.runJob(smartJob{kind: "throughput", member: s.primary, ctx: ctx, timeout: time.Second, generation: s.generation, networkType: "wifi", reservation: id, capBytes: capBytes})
			budget := ledger.Snapshot("wifi", clock.Now(), 0)
			want := 0.0
			if started {
				want = float64(capBytes) / 1e6
			}
			if budget.ReservedMB != 0 || budget.UsedMB != want || result.contained != started {
				t.Fatalf("wrong settlement: result=%+v budget=%+v", result, budget)
			}
		})
	}
}

func TestSmartContainedStartupAndNetworkChangeKeepServing(t *testing.T) {
	for _, change := range []bool{false, true} {
		t.Run(map[bool]string{false: "startup", true: "network_change"}[change], func(t *testing.T) {
			s, clock := smartTestGroup()
			defer s.Close()
			s.generationSucceeded = change
			s.primary = s.members[1]
			if change {
				s.newGenerationLocked(s.network, clock.Now())
			} else {
				for _, member := range s.members {
					member.state = "unknown"
					member.tcp = smartEvidence{}
				}
			}
			s.testHook = func(site string) {
				if strings.HasPrefix(site, "probe/") {
					panic("all validation interrupted")
				}
			}
			for _, member := range s.members {
				result := s.runJob(smartJob{kind: "tcp", member: member, ctx: s.ctx, timeout: time.Second, generation: s.generation, configGeneration: s.configGeneration, memberGeneration: member.generation})
				s.applyResultLocked(result, clock.Now())
			}
			clock.Advance(11 * time.Second)
			if s.Now() != "a" {
				t.Fatal("display primary did not follow declared-order fallback")
			}
			// Ranking may differ from declaration order; no-data fallback must not.
			s.members[0], s.members[1] = s.members[1], s.members[0]
			conn, err := s.DialContext(context.Background(), "tcp", M.Socksaddr{})
			if err != nil {
				t.Fatalf("no-data validation stopped traffic: %v", err)
			}
			_ = conn.Close()
			decision, err := s.SelectMember(context.Background(), "tcp", M.Socksaddr{})
			if err != nil || decision.Outbound().Tag() != "a" {
				t.Fatalf("fallback ignored declared order: %v %v", decision, err)
			}
			if !s.degradedLocked() || s.generationSucceeded {
				t.Fatal("fallback fabricated successful evidence")
			}
			for _, member := range s.members {
				if member.state != "unknown" || member.tcp.state != "" {
					t.Fatal("fallback changed member health")
				}
			}
			member := s.members[0]
			s.applyResultLocked(smartResult{kind: "tcp", member: member, generation: s.generation, configGeneration: s.configGeneration, memberGeneration: member.generation, rtt: time.Millisecond}, clock.Now())
			if s.degradedLocked() || !s.generationSucceeded {
				t.Fatal("real successful evidence did not exit fallback")
			}
		})
	}
}

func TestSmartSynchronousContainmentContracts(t *testing.T) {
	for _, site := range []string{"select", "attach", "dial", "listen", "passive", "status", "networks", "export", "import", "reset", "afterfunc", "shutdown"} {
		t.Run(site, func(t *testing.T) {
			s, _ := smartTestGroup()
			ledger, err := getSmartLedger(nil)
			if err != nil {
				t.Fatal(err)
			}
			s.ledger = ledger
			s.testHook = func(current string) {
				if current == site {
					panic("injected " + site)
				}
			}
			manager := smartTestManager{values: []adapter.Outbound{s}}
			decision := adapter.NewSmartDecision(s.primary.outbound, []adapter.SmartSelection{{Group: s, Member: s.primary.tag, Generation: s.primary.generation}})
			switch site {
			case "select":
				got, selectErr := s.SelectMember(s.ctx, "tcp", M.Socksaddr{})
				if selectErr != nil || got.Outbound() != s.primary.outbound || !s.syncDegraded {
					t.Fatal("selection did not preserve primary")
				}
			case "attach":
				closer := &smartTestCloser{}
				lease := s.Attach(decision, closer)
				if lease == nil || lease.Close() != nil || closer.closed != 0 || len(s.leases[s.primary]) != 0 {
					t.Fatal("attachment did not carry on untracked")
				}
			case "dial":
				_, err = s.DialContext(s.ctx, "tcp", M.Socksaddr{})
			case "listen":
				_, err = s.ListenPacket(s.ctx, M.Socksaddr{})
			case "passive":
				lease := s.Attach(decision, &smartTestCloser{})
				lease.CountRead(123)
				_ = lease.Close()
				if s.primary.throughput.Source != "" {
					t.Fatal("interrupted sample changed throughput")
				}
			case "status":
				_, err = SmartStatus(s.ctx, manager, "")
			case "networks":
				_, err = SmartNetworks(s.ctx, manager, "")
			case "export":
				_, err = SmartExport(s.ctx, manager, false)
			case "import":
				_, err = SmartImport(s.ctx, manager, `{"version":1,"install_id":"test","exported_at":1800000000,"groups":{}}`, .25)
			case "reset":
				err = SmartReset(s.ctx, manager, "")
			case "afterfunc":
				ctx, cancel := context.WithCancel(context.WithValue(s.ctx, smartContainmentKey{}, s))
				done := make(chan struct{})
				context.AfterFunc(ctx, func() { defer close(done); smartCloseCallback(ctx, &smartTestCloser{}) })
				cancel()
				smartLedgerWait(t, done)
			case "shutdown":
				err = s.Close()
			}
			switch site {
			case "dial", "listen", "status", "networks", "export", "import", "reset":
				if !errors.Is(err, errSmartContained) {
					t.Fatalf("expected containment error, got %v", err)
				}
			}
			if s.panicSnapshot().Total != 1 {
				t.Fatalf("panic not recorded exactly once: %+v", s.panicSnapshot())
			}
			// Every boundary must have released its mutex before recording.
			unlocked := make(chan struct{})
			go func() { s.mu.Lock(); s.mu.Unlock(); close(unlocked) }()
			smartLedgerWait(t, unlocked)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSmartLoopRestartsAndShutdownRemainBounded(t *testing.T) {
	for _, site := range []string{"tick", "dispatch", "apply", "restart"} {
		t.Run(site, func(t *testing.T) {
			s, clock := smartTestGroup()
			var fired atomic.Bool
			s.testHook = func(current string) {
				if site == "restart" && current == "tick" && !fired.Load() {
					panic("restart trigger")
				}
				if current == site && fired.CompareAndSwap(false, true) {
					panic("locked " + site)
				}
			}
			smartStartTestOwner(t, s, clock, nil)
			smartAwaitGroup(t, s, clock, func() bool { return s.panicSnapshot().Total >= 1 && s.generationContained })
			select {
			case <-s.loopDone:
				t.Fatal("loopDone closed before supervisor shutdown")
			default:
			}
			conn, err := s.DialContext(s.ctx, "tcp", M.Socksaddr{})
			if err != nil {
				t.Fatal(err)
			}
			_ = conn.Close()
			clock.Advance(time.Second)
			s.request("demand")
			smartAwaitGroup(t, s, clock, func() bool { return s.generationSucceeded })
			closed := make(chan error, 1)
			go func() { closed <- s.Close() }()
			if err := smartLedgerWait(t, closed); err != nil {
				t.Fatal(err)
			}
			select {
			case <-s.loopDone:
			default:
				t.Fatal("supervisor did not signal shutdown")
			}
		})
	}
}

func TestSmartStatusPanicRingAndAllClassBudgets(t *testing.T) {
	s, clock := smartTestGroup()
	defer s.Close()
	ledger, err := getSmartLedger(new(smartLedgerTestCache))
	if err != nil {
		t.Fatal(err)
	}
	ledger.Register(s.Tag(), map[string]float64{"wifi": 100, "cellular": 50, "other": 75})
	s.ledger = ledger
	for range 25 {
		s.contain("test", func() { panic("https://secret.example.test/path 192.0.2.1") })
	}
	status, err := SmartStatus(s.ctx, smartTestManager{values: []adapter.Outbound{s}}, "")
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Version int                    `json:"version"`
		Panics  smartPanics            `json:"panics"`
		Budgets map[string]smartBudget `json:"budgets"`
	}
	if err = json.Unmarshal([]byte(status), &document); err != nil {
		t.Fatal(err)
	}
	if document.Version != 1 || document.Panics.Total != 25 || len(document.Panics.Recent) != 20 {
		t.Fatalf("wrong panic ring: %+v", document)
	}
	for _, class := range []string{"wifi", "cellular", "other"} {
		budget, exists := document.Budgets[class]
		if !exists || budget.Day != clock.Now().Format("2006-01-02") || budget.LimitMB == 0 {
			t.Fatalf("missing class snapshot: %+v", document.Budgets)
		}
	}
	if strings.Contains(status, "secret.example.test") || strings.Contains(status, "192.0.2.1") {
		t.Fatal("status leaked panic endpoint")
	}
}

type smartPanicReadConn struct{ net.Conn }

func (c smartPanicReadConn) Read([]byte) (int, error) { panic("read secret.example.test") }

func TestSmartThroughputCounterPanicReturnsError(t *testing.T) {
	s, _ := smartTestGroup()
	defer s.Close()
	ctx := context.WithValue(s.ctx, smartContainmentKey{}, s)
	conn := &smartMeasureConn{Conn: smartPanicReadConn{}, ctx: ctx, meter: &smartMeasureMeter{remaining: 100}}
	if _, err := conn.Read(make([]byte, 10)); !errors.Is(err, errSmartContained) {
		t.Fatalf("counter panic escaped: %v", err)
	}
	if s.panicSnapshot().Total != 1 {
		t.Fatal("counter panic not recorded")
	}
}

func TestSmartLoopStopsAfterTenPanicsInOneHour(t *testing.T) {
	s, clock := smartTestGroup()
	defer s.Close()
	s.testHook = func(site string) {
		if site == "tick" {
			panic("persistent scheduler bug")
		}
	}
	s.loopDone = make(chan struct{})
	go s.loop(s.ctx)
	delay := time.Second
	for attempt := 1; attempt < 10; attempt++ {
		smartAwaitGroup(t, s, clock, func() bool {
			clock.mu.Lock()
			defer clock.mu.Unlock()
			return s.panicSnapshot().Total == uint64(attempt) && len(clock.timers) >= attempt
		})
		select {
		case <-s.loopDone:
			t.Fatal("supervisor stopped before the restart limit")
		default:
		}
		clock.Advance(delay)
		delay = min(delay*2, 5*time.Minute)
	}
	smartLedgerWait(t, s.loopDone)
	if s.panicSnapshot().Total != 10 || !s.loopStopped || !s.degradedLocked() {
		t.Fatal("restart limit did not freeze degraded selection")
	}
	conn, err := s.DialContext(s.ctx, "tcp", M.Socksaddr{})
	if err != nil {
		t.Fatalf("stopped scheduler stopped traffic: %v", err)
	}
	_ = conn.Close()
}

type smartPanicMetadataCache struct {
	smartLedgerTestCache
	panicSave atomic.Bool
}

func (c *smartPanicMetadataCache) SaveSmartMetadata(metadata *adapter.SmartMetadata) error {
	if c.panicSave.Load() {
		panic("metadata write interrupted")
	}
	return c.smartLedgerTestCache.SaveSmartMetadata(metadata)
}

func TestSmartLedgerBackgroundPanicsReleaseOperations(t *testing.T) {
	s, clock := smartTestGroup()
	defer s.Close()
	cache := new(smartPanicMetadataCache)
	ledger, err := getSmartLedger(cache)
	if err != nil {
		t.Fatal(err)
	}
	s.ledger = ledger
	ledger.Register(s.Tag(), map[string]float64{"wifi": 100})
	ctx := context.WithValue(s.ctx, smartContainmentKey{}, s)
	cache.panicSave.Store(true)
	if _, _, err = ledger.Reserve(ctx, "wifi", clock.Now(), 0); !errors.Is(err, errSmartContained) {
		t.Fatalf("admission panic escaped: %v", err)
	}
	cache.panicSave.Store(false)
	id, _, err := ledger.Reserve(ctx, "wifi", clock.Now(), 0)
	if err != nil {
		t.Fatalf("admission operation remained locked: %v", err)
	}
	cache.panicSave.Store(true)
	if err = ledger.Reconcile(ctx, id, 1234, false); !errors.Is(err, errSmartContained) {
		t.Fatalf("accounting panic escaped: %v", err)
	}
	if budget := ledger.Snapshot("wifi", clock.Now(), 0); budget.ReservedMB == 0 || budget.UsedMB != 0 {
		t.Fatalf("failed accounting lost reservation: %+v", budget)
	}
	cache.panicSave.Store(false)
	next, _, err := ledger.Reserve(ctx, "wifi", clock.Now(), 0)
	if err != nil {
		t.Fatalf("accounting operation remained locked: %v", err)
	}
	if err = ledger.Reconcile(ctx, next, 0, false); err != nil {
		t.Fatal(err)
	}
	if budget := ledger.Snapshot("wifi", clock.Now(), 0); budget.ReservedMB != 0 || budget.UsedMB != .001234 {
		t.Fatalf("pending accounting did not resume: %+v", budget)
	}
	if s.panicSnapshot().Total != 2 {
		t.Fatalf("background panics not recorded once: %+v", s.panicSnapshot())
	}
}
