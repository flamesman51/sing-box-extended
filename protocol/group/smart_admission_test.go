package group

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/service"
)

type smartAdmissionCountingPlatform struct {
	smartNetworkTestPlatform
	checks atomic.Int64
	power  atomic.Bool
}

func (p *smartAdmissionCountingPlatform) IsPowerSaveMode() bool {
	p.checks.Add(1)
	return p.power.Load()
}

func TestSmartOwnerDenialCannotReadmitItself(t *testing.T) {
	s, clock := smartTestGroup()
	cache := new(smartLedgerBlockingCache)
	ledger, err := getSmartLedger(cache)
	if err != nil {
		t.Fatal(err)
	}
	ledger.Register(s.Tag(), map[string]float64{"wifi": 1})
	platform := new(smartAdmissionCountingPlatform)
	s.ctx = service.ContextWith[adapter.PlatformInterface](s.ctx, platform)
	s.ledger, s.throughputEnabled, s.generationSucceeded, s.udpEnabled = ledger, true, true, false
	s.lastDirect, s.lastFullRound, s.lastNetworkRead = clock.Now(), clock.Now(), clock.Now().Add(time.Hour)
	for _, member := range s.members {
		member.lastProbe = clock.Now()
	}
	smartStartTestOwner(t, s, clock, nil)
	smartAwaitGroup(t, s, clock, func() bool { return s.measurementBudgetExhausted })
	before := platform.checks.Load()
	select {
	case <-time.After(30 * time.Millisecond):
	case <-s.ctx.Done():
		t.Fatal("owner unexpectedly closed")
	}
	if attempts := platform.checks.Load() - before; attempts > 2 {
		t.Fatalf("denial repeatedly woke and readmitted itself: %d platform checks", attempts)
	}
	s.mu.Lock()
	plan := s.regularMeasurementPlanLocked(s.primary, clock.Now())
	backoff := s.primary.measurementBackoff[smartMeasurementPlanKey(plan)]
	retry := backoff.until
	backedOff := s.measurementPlanBackedOffLocked(s.primary, plan, clock.Now())
	s.mu.Unlock()
	local := clock.Now().In(time.FixedZone("smart-local", s.network.Offset))
	year, month, day := local.Date()
	midnight := time.Date(year, month, day+1, 0, 0, 0, 0, local.Location())
	if !backedOff || !retry.Equal(midnight) {
		t.Fatalf("member budget denial did not back off until local midnight: until=%v want=%v", retry, midnight)
	}
}

func TestSmartDisabledThroughputNeverEntersAdmission(t *testing.T) {
	for _, disabled := range []string{"cache", "budget", "option"} {
		t.Run(disabled, func(t *testing.T) {
			s, clock := smartTestGroup()
			var cache adapter.CacheFile = new(smartLedgerBlockingCache)
			if disabled == "cache" {
				cache = nil
			}
			ledger, err := getSmartLedger(cache)
			if err != nil {
				t.Fatal(err)
			}
			limit := 100.0
			if disabled == "budget" {
				limit = 0
			}
			ledger.Register(s.Tag(), map[string]float64{"wifi": limit})
			s.ledger, s.throughputEnabled, s.generationSucceeded = ledger, disabled != "option", true
			defer s.Close()
			for range 100 {
				s.scheduleThroughputLocked(clock.Now())
			}
			if s.admission != nil {
				t.Fatalf("permanently disabled %s created an admission", disabled)
			}
		})
	}
}

func TestSmartAdmissionDenialRetryDeadlines(t *testing.T) {
	for _, test := range []struct {
		err   error
		delay time.Duration
	}{
		{errSmartTestBusy, 5 * time.Second},
		{errSmartTestRate, time.Minute},
		{errSmartTestsDisabled, time.Minute},
		{context.Canceled, time.Second},
		{errors.New("temporary storage error"), 30 * time.Second},
	} {
		s, clock := smartTestGroup()
		job := smartJob{member: s.primary, ctx: s.ctx, generation: s.generation, configGeneration: s.configGeneration, networkType: s.network.Type}
		admission := &smartAdmission{job: job, ack: make(chan bool, 1)}
		s.applyAdmissionLocked(smartResult{admission: admission, err: test.err}, clock.Now())
		if got := s.admissionRetryAt.Sub(clock.Now()); got != test.delay {
			t.Fatalf("%v retry = %v; want %v", test.err, got, test.delay)
		}
		_ = s.Close()
	}
}

func TestSmartAdmissionBudgetDenialKeepsSearchingOtherMembers(t *testing.T) {
	s, clock := smartTestGroup()
	ledger, err := getSmartLedger(new(smartLedgerBlockingCache))
	if err != nil {
		t.Fatal(err)
	}
	ledger.Register(s.Tag(), map[string]float64{"wifi": 100})
	s.ledger, s.throughputEnabled, s.generationSucceeded = ledger, true, true
	t.Cleanup(func() { _ = s.Close() })
	primary, candidate := s.primary, s.members[1]
	primary.throughput = smartThroughput{Download: 200, Source: "active", At: clock.Now().Add(-25 * time.Hour)}
	plan := s.regularMeasurementPlanLocked(primary, clock.Now())
	job := smartJob{member: primary, ctx: s.ctx, generation: s.generation, configGeneration: s.configGeneration, networkType: s.network.Type, plan: plan}
	admission := &smartAdmission{job: job, ack: make(chan bool, 1)}
	s.applyAdmissionLocked(smartResult{admission: admission, err: errSmartBudget}, clock.Now())
	if <-admission.ack {
		t.Fatal("over-budget plan was admitted")
	}
	if !s.measurementPlanBackedOffLocked(primary, plan, clock.Now()) || !s.admissionRetryAt.IsZero() {
		t.Fatal("member budget denial either failed to back off or blocked the whole group")
	}
	s.scheduleThroughputLocked(clock.Now())
	if s.admission == nil || s.admission.job.member != candidate {
		t.Fatal("budget denial prevented an affordable member from admission")
	}
	result := smartLedgerWait(t, s.results)
	s.applyResultLocked(result, clock.Now())
	s.admissionWorkers.Wait()
	if !candidate.queued["throughput"] || primary.queued["throughput"] {
		t.Fatal("scheduler did not queue only the affordable member")
	}
	if snapshot := ledger.Snapshot("wifi", clock.Now(), 0); snapshot.UsedMB != 0 || snapshot.ReservedMB != 100 {
		t.Fatalf("affordable plan did not reserve the full daily remainder: %+v", snapshot)
	}
}

func TestSmartAdmissionMemberBackoffInvalidation(t *testing.T) {
	for _, change := range []string{"budget", "download", "upload", "midnight"} {
		t.Run(change, func(t *testing.T) {
			s, clock := smartTestGroup()
			ledger, err := getSmartLedger(new(smartLedgerBlockingCache))
			if err != nil {
				t.Fatal(err)
			}
			ledger.Register(s.Tag(), map[string]float64{"wifi": 1})
			s.ledger = ledger
			t.Cleanup(func() { _ = s.Close() })
			now := clock.Now()
			plan := s.regularMeasurementPlanLocked(s.primary, now)
			s.backoffMeasurementPlanLocked(s.primary, plan, now)
			if !s.measurementPlanBackedOffLocked(s.primary, plan, now) {
				t.Fatal("unchanged plan was not backed off")
			}
			switch change {
			case "budget":
				ledger.Register(s.Tag(), map[string]float64{"wifi": 100})
			case "download":
				s.primary.throughput.At = now
			case "upload":
				s.primary.throughput.UploadAt = now
			case "midnight":
				now = s.primary.measurementBackoff[smartMeasurementPlanKey(plan)].until
			}
			if s.measurementPlanBackedOffLocked(s.primary, plan, now) {
				t.Fatalf("%s change did not invalidate member backoff", change)
			}
		})
	}
}

func TestSmartRegularAdmissionSpacingAndIdleBoundaries(t *testing.T) {
	for _, test := range []struct {
		name      string
		spacing   time.Duration
		readIdle  time.Duration
		writeIdle time.Duration
		admit     bool
	}{
		{"member_spacing", 10*time.Minute - time.Nanosecond, 2 * time.Second, 2 * time.Second, false},
		{"read_idle", 10 * time.Minute, 2*time.Second - time.Nanosecond, 2 * time.Second, false},
		{"write_idle", 10 * time.Minute, 2 * time.Second, 2*time.Second - time.Nanosecond, false},
		{"exact_boundaries", 10 * time.Minute, 2 * time.Second, 2 * time.Second, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, clock := smartTestGroup()
			ledger, err := getSmartLedger(new(smartLedgerBlockingCache))
			if err != nil {
				t.Fatal(err)
			}
			ledger.Register(s.Tag(), map[string]float64{"wifi": 100})
			s.ledger, s.throughputEnabled, s.generationSucceeded = ledger, true, true
			s.members, s.standby = s.members[:1], nil
			s.primary.lastThroughput = clock.Now().Add(-test.spacing)
			lease := &smartLease{member: s.primary, group: s, lastRead: clock.Now().Add(-test.readIdle), lastWrite: clock.Now().Add(-test.writeIdle)}
			s.leases[s.primary] = map[*smartLease]struct{}{lease: {}}
			t.Cleanup(func() {
				s.leases = make(map[*smartMember]map[*smartLease]struct{})
				_ = s.Close()
			})
			s.scheduleThroughputLocked(clock.Now())
			if (s.admission != nil) != test.admit {
				t.Fatalf("admitted=%t want=%t", s.admission != nil, test.admit)
			}
			if test.admit {
				result := smartLedgerWait(t, s.results)
				s.applyResultLocked(result, clock.Now())
				s.admissionWorkers.Wait()
				if !s.primary.queued["throughput"] {
					t.Fatal("boundary-eligible plan was not queued")
				}
			}
		})
	}
}
