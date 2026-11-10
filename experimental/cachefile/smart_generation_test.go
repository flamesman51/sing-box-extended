package cachefile

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/bbolt"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
)

type smartHistoryTransactionGate struct {
	bbolt.Logger
	access  sync.Mutex
	entered chan struct{}
	release chan struct{}
}

func (g *smartHistoryTransactionGate) Debugf(format string, args ...any) {
	// bbolt emits this message before acquiring its writer lock or opening a transaction.
	if format != "Starting a new transaction [writable: %t]" || len(args) != 1 || args[0] != true {
		return
	}
	g.access.Lock()
	entered, release := g.entered, g.release
	g.entered, g.release = nil, nil
	g.access.Unlock()
	if entered != nil {
		close(entered)
		<-release
	}
}

func (g *smartHistoryTransactionGate) blockNext(t *testing.T) (<-chan struct{}, func()) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	g.access.Lock()
	g.entered, g.release = entered, release
	g.access.Unlock()
	var once sync.Once
	resume := func() { once.Do(func() { close(release) }) }
	t.Cleanup(resume)
	return entered, resume
}

func newSmartHistoryGenerationCache(t *testing.T, cacheID string) (*CacheFile, *smartHistoryTransactionGate) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cache.db")
	gate := &smartHistoryTransactionGate{Logger: &bbolt.DefaultLogger{Logger: log.New(io.Discard, "", 0)}}
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Logger: gate})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	cache := New(context.Background(), nil, option.CacheFileOptions{Path: path, CacheID: cacheID})
	cache.DB = db
	return cache, gate
}

func TestSmartHistoryRejectsDetachedGenerationInsideTransaction(t *testing.T) {
	for _, name := range []string{"replacement_save", "reset", "replacement_reset", "replacement_reset_save", "legacy_reset"} {
		t.Run(name, func(t *testing.T) {
			cache, gate := newSmartHistoryGenerationCache(t, "profile")
			var generation uint64
			if name != "legacy_reset" {
				generation = cache.NewSmartHistoryGeneration("auto")
			}
			if err := cache.SaveSmartHistory("auto", &adapter.SmartHistory{Data: []byte("original"), Generation: generation}); err != nil {
				t.Fatal(err)
			}
			entered, resume := gate.blockNext(t)
			done := make(chan error, 1)
			go func() {
				done <- cache.SaveSmartHistory("auto", &adapter.SmartHistory{Data: []byte("detached"), Generation: generation})
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("detached save did not reach the transaction boundary")
			}
			if name != "reset" && name != "legacy_reset" {
				generation = cache.NewSmartHistoryGeneration("auto")
			}
			if name != "replacement_save" {
				if err := cache.ResetSmartHistory("auto"); err != nil {
					t.Fatal(err)
				}
				generation = cache.NewSmartHistoryGeneration("auto")
			}
			want := ""
			if name == "replacement_save" || name == "replacement_reset_save" {
				want = "replacement"
				if err := cache.SaveSmartHistory("auto", &adapter.SmartHistory{Data: []byte(want), Generation: generation}); err != nil {
					t.Fatal(err)
				}
			}
			resume()
			select {
			case err := <-done:
				if !errors.Is(err, os.ErrInvalid) {
					t.Fatalf("detached save error = %v; want os.ErrInvalid", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("detached save did not finish after release")
			}
			history, err := cache.LoadSmartHistory("auto")
			if want == "" {
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("detached save resurrected reset history: %v, %v", history, err)
				}
			} else if err != nil || string(history.Data) != want {
				t.Fatalf("detached save replaced current history: %v, %v; want %q", history, err, want)
			}
		})
	}
}

func TestSmartHistoryGenerationScopeAndLegacyWrites(t *testing.T) {
	first, _ := newSmartHistoryGenerationCache(t, "")
	second := New(context.Background(), nil, option.CacheFileOptions{CacheID: "second"})
	second.DB = first.DB
	if err := first.SaveSmartHistory("legacy", &adapter.SmartHistory{Data: []byte("legacy")}); err != nil {
		t.Fatal(err)
	}
	if err := first.SaveSmartHistory("unclaimed", &adapter.SmartHistory{Data: []byte("invalid"), Generation: 1}); !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("unissued generation error = %v; want os.ErrInvalid", err)
	}
	firstGeneration := first.NewSmartHistoryGeneration("auto")
	otherGeneration := first.NewSmartHistoryGeneration("other")
	secondGeneration := second.NewSmartHistoryGeneration("auto")
	if firstGeneration == 0 || otherGeneration != firstGeneration || secondGeneration != firstGeneration {
		t.Fatalf("initial generations are not independent: %d, %d, %d", firstGeneration, otherGeneration, secondGeneration)
	}
	replacement := first.NewSmartHistoryGeneration("auto")
	if replacement == 0 || replacement == firstGeneration {
		t.Fatalf("replacement generation = %d; previous = %d", replacement, firstGeneration)
	}
	for _, generation := range []uint64{0, firstGeneration} {
		if err := first.SaveSmartHistory("auto", &adapter.SmartHistory{Data: []byte("stale"), Generation: generation}); !errors.Is(err, os.ErrInvalid) {
			t.Fatalf("stale generation %d error = %v; want os.ErrInvalid", generation, err)
		}
	}
	for _, item := range []struct {
		cache      *CacheFile
		group      string
		generation uint64
		data       string
	}{
		{first, "auto", replacement, "replacement"},
		{first, "other", otherGeneration, "other-group"},
		{second, "auto", secondGeneration, "other-cache"},
	} {
		if err := item.cache.SaveSmartHistory(item.group, &adapter.SmartHistory{Data: []byte(item.data), Generation: item.generation}); err != nil {
			t.Fatal(err)
		}
	}
	if err := first.ResetSmartHistory("auto"); err != nil {
		t.Fatal(err)
	}
	if err := first.SaveSmartHistory("auto", &adapter.SmartHistory{Data: []byte("stale"), Generation: replacement}); !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("pre-reset generation error = %v; want os.ErrInvalid", err)
	}
	if err := first.ResetSmartHistory("legacy"); err != nil {
		t.Fatal(err)
	}
	if err := first.SaveSmartHistory("legacy", &adapter.SmartHistory{Data: []byte("stale")}); !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("legacy write after reset error = %v; want os.ErrInvalid", err)
	}
	for _, item := range []struct {
		cache      *CacheFile
		group      string
		generation uint64
		data       string
	}{
		{first, "other", otherGeneration, "other-group"},
		{second, "auto", secondGeneration, "other-cache"},
	} {
		if err := item.cache.SaveSmartHistory(item.group, &adapter.SmartHistory{Data: []byte(item.data), Generation: item.generation}); err != nil {
			t.Fatalf("reset invalidated another scope: %v", err)
		}
		history, err := item.cache.LoadSmartHistory(item.group)
		if err != nil || string(history.Data) != item.data || history.Generation != 0 {
			t.Fatalf("scoped persisted history = %v, %v; want %q without a generation", history, err, item.data)
		}
	}
}

func TestSmartHistoryGenerationCannotWrapToLegacy(t *testing.T) {
	cache := New(context.Background(), nil, option.CacheFileOptions{})
	cache.smartGenerations = map[string]uint64{"auto": ^uint64(0)}
	defer func() {
		if recover() == nil {
			t.Error("generation exhaustion did not panic")
		}
		if cache.smartGenerations["auto"] != ^uint64(0) {
			t.Error("generation exhaustion enabled legacy writes")
		}
	}()
	cache.NewSmartHistoryGeneration("auto")
}

func TestSmartHistoryResetRejectsStaleOwnerInsideTransaction(t *testing.T) {
	cache, gate := newSmartHistoryGenerationCache(t, "profile")
	old := cache.NewSmartHistoryGeneration("auto")
	entered, resume := gate.blockNext(t)
	type resetResult struct {
		generation uint64
		err        error
	}
	done := make(chan resetResult, 1)
	go func() {
		generation, err := cache.ResetSmartHistoryGeneration("auto", old)
		done <- resetResult{generation, err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("reset did not reach the transaction boundary")
	}
	replacement := cache.NewSmartHistoryGeneration("auto")
	if err := cache.SaveSmartHistory("auto", &adapter.SmartHistory{Data: []byte("replacement"), Generation: replacement}); err != nil {
		t.Fatal(err)
	}
	resume()
	select {
	case result := <-done:
		if !errors.Is(result.err, os.ErrInvalid) || result.generation != old {
			t.Fatalf("stale reset returned %d, %v", result.generation, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stale reset did not complete")
	}
	history, err := cache.LoadSmartHistory("auto")
	if err != nil || string(history.Data) != "replacement" {
		t.Fatalf("stale reset changed replacement history: %v, %v", history, err)
	}
	next, err := cache.ResetSmartHistoryGeneration("auto", replacement)
	if err != nil || next <= replacement {
		t.Fatalf("current owner reset returned %d, %v", next, err)
	}
	if err = cache.SaveSmartHistory("auto", &adapter.SmartHistory{Data: []byte("after reset"), Generation: next}); err != nil {
		t.Fatal(err)
	}
	if err = cache.SaveSmartHistory("auto", &adapter.SmartHistory{Data: []byte("stale"), Generation: replacement}); !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("pre-reset owner save returned %v", err)
	}
}
