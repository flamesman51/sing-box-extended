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
	smartAwaitGroup(t, s, clock, func() bool { return !s.admissionRetryAt.IsZero() })
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
	retry := s.admissionRetryAt
	s.mu.Unlock()
	if retry.Sub(clock.Now()) < time.Hour {
		t.Fatal("budget denial did not back off until the next local day")
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
