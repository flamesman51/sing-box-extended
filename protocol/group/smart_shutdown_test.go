package group

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

type smartSharedWriterCache struct {
	*smartLedgerBlockingCache
	history         *smartBlockingHistoryCache
	writer          <-chan struct{}
	metadataStarted chan struct{}
	once            sync.Once
}

func (c *smartSharedWriterCache) SaveSmartMetadata(metadata *adapter.SmartMetadata) error {
	if c.writer != nil {
		c.once.Do(func() { close(c.metadataStarted) })
		<-c.writer
	}
	return c.smartLedgerBlockingCache.SaveSmartMetadata(metadata)
}

func (c *smartSharedWriterCache) SaveSmartHistory(tag string, history *adapter.SmartHistory) error {
	return c.history.SaveSmartHistory(tag, history)
}

func smartEnableBlockedThroughput(t *testing.T, s *Smart, history *smartBlockingHistoryCache) <-chan struct{} {
	t.Helper()
	cache := &smartSharedWriterCache{
		smartLedgerBlockingCache: new(smartLedgerBlockingCache), history: history,
		metadataStarted: make(chan struct{}),
	}
	ledger, err := getSmartLedger(cache)
	if err != nil {
		t.Fatal(err)
	}
	ledger.Register(s.Tag(), map[string]float64{"wifi": 300, "cellular": 50, "other": 300})
	cache.writer = history.release
	s.cache, s.ledger, s.throughputEnabled = cache, ledger, true
	return cache.metadataStarted
}

func TestSmartCloseBoundsPermanentlyBlockedHistoryAndMetadata(t *testing.T) {
	s, clock := smartTestGroup()
	cache := &smartBlockingHistoryCache{started: make(chan struct{}), release: make(chan struct{})}
	metadataStarted := smartEnableBlockedThroughput(t, s, cache)
	s.generationSucceeded, s.udpEnabled = true, false
	s.lastDirect, s.lastFullRound, s.lastNetworkRead = clock.Now(), clock.Now(), clock.Now().Add(time.Hour)
	s.recordObservationLocked(s.primary, true, time.Millisecond, clock.Now())
	for _, member := range s.members {
		member.lastProbe = clock.Now()
	}
	smartStartTestOwner(t, s, clock, nil)
	smartLedgerWait(t, cache.started)
	smartLedgerWait(t, metadataStarted)
	selected := make(chan error, 1)
	go func() {
		_, err := s.SelectMember(context.Background(), "tcp", M.Socksaddr{})
		selected <- err
	}()
	if err := smartLedgerWait(t, selected); err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	started := time.Now()
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(smartShutdownTimeout + time.Second):
		t.Fatal("VPN shutdown waited indefinitely for blocked storage")
	}
	if elapsed := time.Since(started); elapsed < smartShutdownTimeout-100*time.Millisecond {
		t.Fatalf("shutdown abandoned persistence before the configured grace period: %v", elapsed)
	}
	unlocked := make(chan struct{})
	go func() {
		s.mu.Lock()
		abandoned := s.persistenceAbandoned
		s.mu.Unlock()
		if abandoned {
			close(unlocked)
		}
	}()
	smartLedgerWait(t, unlocked)
	// Both stores remain blocked after Close; no worker retains the state mutex.
}

func TestSmartCloseReconcilesSharedLedgerUnstartedJobs(t *testing.T) {
	for _, location := range []string{"pending", "buffered", "dequeued"} {
		t.Run(location, func(t *testing.T) {
			s, clock := smartTestGroup()
			cache := new(smartLedgerBlockingCache)
			ledger, err := getSmartLedger(cache)
			if err != nil {
				t.Fatal(err)
			}
			ledger.Register(s.Tag(), map[string]float64{"wifi": 100})
			ledger.Register("other-group", map[string]float64{"wifi": 100})
			t.Cleanup(func() { ledger.Unregister("other-group") })
			s.ledger = ledger
			id, _, err := ledger.Reserve(context.Background(), "wifi", clock.Now(), 0)
			if err != nil {
				t.Fatal(err)
			}
			job := smartJob{kind: "throughput", member: s.primary, reservation: id, ctx: s.ctx, timeout: time.Second}
			s.healthJobs, s.otherJobs = make(chan smartJob, 1), make(chan smartJob, 1)
			switch location {
			case "pending":
				s.pending = append(s.pending, job)
			case "buffered":
				s.otherJobs <- job
			case "dequeued":
				s.healthJobs <- job
				s.cancel()
				s.workers.Add(1)
				go s.worker(s.ctx, false)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			next, _, err := ledger.Reserve(context.Background(), "wifi", clock.Now().Add(time.Minute), 0)
			if err != nil {
				t.Fatalf("closed group left shared ledger busy: %v", err)
			}
			if snapshot := ledger.Snapshot("wifi", clock.Now(), 0); snapshot.UsedMB != 0 {
				t.Fatalf("unstarted test charged bytes: %+v", snapshot)
			}
			if err = ledger.Reconcile(context.Background(), next, 0, false); err != nil {
				t.Fatal(err)
			}
		})
	}
}
