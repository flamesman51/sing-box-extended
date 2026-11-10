package group

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/experimental/cachefile"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/service"
)

type smartCacheTestManager struct {
	adapter.OutboundManager
	member adapter.Outbound
}

func (m *smartCacheTestManager) Outbound(tag string) (adapter.Outbound, bool) {
	return m.member, tag == m.member.Tag()
}

func newSmartCacheTestGroup(t *testing.T, cache adapter.CacheFile, tag string) *Smart {
	t.Helper()
	ctx := service.ContextWith[adapter.CacheFile](context.Background(), cache)
	ctx = service.ContextWith[adapter.OutboundManager](ctx, &smartCacheTestManager{member: &smartFakeOutbound{tag: "member"}})
	ctx = service.ContextWith[adapter.SmartIdentityProvider](ctx, smartTestIdentityProvider{})
	created, err := NewSmart(ctx, nil, nil, tag, option.SmartOutboundOptions{GroupCommonOption: option.GroupCommonOption{Outbounds: []string{"member"}}})
	if err != nil {
		t.Fatal(err)
	}
	group := created.(*Smart)
	t.Cleanup(func() {
		if err := group.Close(); err != nil {
			t.Error(err)
		}
	})
	return group
}

func TestSmartUsesCacheRegisteredAfterConstruction(t *testing.T) {
	ctx := service.ContextWithDefaultRegistry(context.Background())
	ctx = service.ContextWith[adapter.OutboundManager](ctx, &smartCacheTestManager{member: &smartFakeOutbound{tag: "member"}})
	ctx = service.ContextWith[adapter.SmartIdentityProvider](ctx, smartTestIdentityProvider{})
	created, err := NewSmart(ctx, nil, nil, "auto", option.SmartOutboundOptions{GroupCommonOption: option.GroupCommonOption{Outbounds: []string{"member"}}})
	if err != nil {
		t.Fatal(err)
	}
	group := created.(*Smart)
	// box.New registers and initializes the cache file only after every outbound is constructed.
	cache := cachefile.New(ctx, nil, option.CacheFileOptions{Path: filepath.Join(t.TempDir(), "cache.db")})
	service.MustRegister[adapter.CacheFile](ctx, cache)
	scope := adapter.NewScope(ctx, nil)
	if err = cache.Start(adapter.StartStateInitialize, scope); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := scope.Close(); err != nil {
			t.Error(err)
		}
	})
	t.Cleanup(func() {
		if err := group.Close(); err != nil {
			t.Error(err)
		}
	})
	if err = group.Start(adapter.StartStateStart, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = cache.LoadSmartMetadata(); err != nil {
		t.Fatalf("installation metadata was not persisted: %v", err)
	}
}

func TestSmartStartsWithMissingCacheDocuments(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cache.db")
	cache := cachefile.New(ctx, nil, option.CacheFileOptions{Path: path, CacheID: "first"})
	scope := adapter.NewScope(ctx, nil)
	if err := cache.Start(adapter.StartStateInitialize, scope); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := scope.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := cache.LoadSmartMetadata(); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fresh metadata error = %v", err)
	}
	if _, err := cache.LoadSmartHistory("auto"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fresh history error = %v", err)
	}
	fresh := newSmartCacheTestGroup(t, cache, "auto")
	if err := fresh.Start(adapter.StartStateStart, nil); err != nil {
		t.Fatalf("fresh cache startup: %v", err)
	}
	installID := fresh.installID
	if installID == "" || len(fresh.seed) != 32 || len(fresh.directSeed) != 32 || len(fresh.history.Stats) != 0 || len(fresh.members) != 1 {
		t.Fatal("fresh cache did not initialize installation, empty history, and members")
	}
	if _, err := cache.LoadSmartMetadata(); err != nil {
		t.Fatalf("installation metadata was not persisted: %v", err)
	}
	fresh.history.Observe([]string{"wifi"}, fresh.members[0].identity, true, 100, time.Unix(1800000000, 0), 0)
	data, err := fresh.history.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err = cache.SaveSmartHistory("auto", &adapter.SmartHistory{Data: data, Generation: fresh.historyGeneration}); err != nil {
		t.Fatal(err)
	}
	if err = fresh.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"new_group", "new_cache_id", "deleted_history"} {
		t.Run(name, func(t *testing.T) {
			var activeCache adapter.CacheFile = cache
			tag := "auto"
			switch name {
			case "new_group":
				tag = "new-group"
			case "new_cache_id":
				other := cachefile.New(ctx, nil, option.CacheFileOptions{Path: path, CacheID: "second"})
				other.DB = cache.DB
				activeCache = other
			case "deleted_history":
				if err := cache.ResetSmartHistory(tag); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := activeCache.LoadSmartHistory(tag); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("missing history error = %v", err)
			}
			group := newSmartCacheTestGroup(t, activeCache, tag)
			if err := group.Start(adapter.StartStateStart, nil); err != nil {
				t.Fatalf("missing history startup: %v", err)
			}
			if group.installID != installID || len(group.history.Stats) != 0 || len(group.members) != 1 {
				t.Fatal("missing history did not preserve installation and initialize empty group")
			}
		})
	}
}

type smartCacheHistoryError struct {
	adapter.CacheFile
	err error
}

func (c *smartCacheHistoryError) LoadSmartHistory(string) (*adapter.SmartHistory, error) {
	return nil, c.err
}

func TestSmartStartPreservesHistoryLoadErrors(t *testing.T) {
	want := errors.New("history read failed")
	cache := &smartCacheHistoryError{CacheFile: new(smartLedgerTestCache), err: want}
	group := newSmartCacheTestGroup(t, cache, "auto")
	if err := group.Start(adapter.StartStateStart, nil); err != want {
		t.Fatalf("history load error = %v, want %v", err, want)
	}
}

func TestSmartFailedReplacementKeepsLiveHistoryOwnership(t *testing.T) {
	for _, failure := range []string{"missing-member", "invalid-history"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			cache := cachefile.New(ctx, nil, option.CacheFileOptions{Path: filepath.Join(t.TempDir(), "cache.db")})
			scope := adapter.NewScope(ctx, nil)
			if err := cache.Start(adapter.StartStateInitialize, scope); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = scope.Close() })
			live := newSmartCacheTestGroup(t, cache, "auto")
			if err := live.Start(adapter.StartStateStart, scope); err != nil {
				t.Fatal(err)
			}
			replacement := newSmartCacheTestGroup(t, cache, "auto")
			if failure == "missing-member" {
				replacement.options.Outbounds = []string{"missing"}
			} else if err := cache.SaveSmartHistory("auto", &adapter.SmartHistory{Data: []byte("invalid"), Generation: live.historyGeneration}); err != nil {
				t.Fatal(err)
			}
			if err := replacement.Start(adapter.StartStateStart, scope); err == nil {
				t.Fatal("invalid replacement unexpectedly started")
			}
			live.recordObservationLocked(live.primary, true, time.Millisecond, live.clock.Now())
			live.flushHistory(true)
			stored, err := cache.LoadSmartHistory("auto")
			if err != nil {
				t.Fatal(err)
			}
			history, err := decodeSmartHistory(stored.Data)
			if err != nil {
				t.Fatal(err)
			}
			if history.Stats[live.network.Key][live.primary.identity] == nil {
				t.Fatal("failed replacement fenced out the surviving group's history")
			}
		})
	}
}

type smartStartupHistoryGate struct {
	*cachefile.CacheFile
	loaded, release chan struct{}
}

func (c *smartStartupHistoryGate) LoadSmartHistory(group string) (*adapter.SmartHistory, error) {
	history, err := c.CacheFile.LoadSmartHistory(group)
	close(c.loaded)
	<-c.release
	return history, err
}

func TestSmartStartupSerializesHistoryLoadAndClaimWithReset(t *testing.T) {
	for _, target := range []string{"old", "replacement"} {
		t.Run(target, func(t *testing.T) {
			ctx := context.Background()
			cache := cachefile.New(ctx, nil, option.CacheFileOptions{Path: filepath.Join(t.TempDir(), "cache.db")})
			scope := adapter.NewScope(ctx, nil)
			if err := cache.Start(adapter.StartStateInitialize, scope); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = scope.Close() })
			live := newSmartCacheTestGroup(t, cache, "auto")
			if err := live.Start(adapter.StartStateStart, scope); err != nil {
				t.Fatal(err)
			}
			live.recordObservationLocked(live.primary, true, time.Millisecond, live.clock.Now())
			live.flushHistory(true)
			gate := &smartStartupHistoryGate{CacheFile: cache, loaded: make(chan struct{}), release: make(chan struct{})}
			replacement := newSmartCacheTestGroup(t, gate, "auto")
			resume := sync.OnceFunc(func() { close(gate.release) })
			t.Cleanup(resume)
			started := make(chan error, 1)
			go func() { started <- replacement.Start(adapter.StartStateStart, scope) }()
			smartLedgerWait(t, gate.loaded)
			if smartMutationAccess.TryLock() {
				smartMutationAccess.Unlock()
				t.Fatal("reset can run between history load and generation claim")
			}
			group := live
			if target == "replacement" {
				group = replacement
			}
			resetStarted, resetDone := make(chan struct{}), make(chan error, 1)
			go func() {
				close(resetStarted)
				resetDone <- SmartReset(ctx, smartTestManager{values: []adapter.Outbound{group}}, "")
			}()
			smartLedgerWait(t, resetStarted)
			resume()
			if err := smartLedgerWait(t, started); err != nil {
				t.Fatal(err)
			}
			err := smartLedgerWait(t, resetDone)
			if target == "old" {
				if !errors.Is(err, os.ErrInvalid) {
					t.Fatalf("old reset after replacement claim = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			replacement.recordObservationLocked(replacement.primary, true, time.Millisecond, replacement.clock.Now())
			replacement.flushHistory(true)
			stored, err := cache.LoadSmartHistory("auto")
			if err != nil {
				t.Fatal(err)
			}
			history, err := decodeSmartHistory(stored.Data)
			if err != nil {
				t.Fatal(err)
			}
			if statistic := history.Stats[replacement.network.Key][replacement.primary.identity]; statistic == nil || statistic.S != 1 {
				t.Fatalf("startup restored observations from before the successful reset: %+v", statistic)
			}
		})
	}
}
