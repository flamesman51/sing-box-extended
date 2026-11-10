package group

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/sagernet/sing-box/adapter"
)

type smartResetGenerationCache struct {
	*smartBlockingHistoryCache
	generation uint64
	err        error
	afterReset func()
}

func (c *smartResetGenerationCache) NewSmartHistoryGeneration(string) uint64 {
	c.generation++
	return c.generation
}

func (c *smartResetGenerationCache) ResetSmartHistory(string) error {
	c.generation++
	return c.err
}

func (c *smartResetGenerationCache) ResetSmartHistoryGeneration(_ string, generation uint64) (uint64, error) {
	if generation != c.generation {
		return generation, os.ErrInvalid
	}
	c.generation++
	next := c.generation
	if c.afterReset != nil {
		c.afterReset()
	}
	return next, c.err
}

func TestSmartResetFailureRefreshesHistoryWriteGeneration(t *testing.T) {
	s, clock := smartTestGroup()
	cache := &smartResetGenerationCache{smartBlockingHistoryCache: new(smartBlockingHistoryCache), err: errors.New("reset write failed")}
	s.cache = cache
	s.historyGeneration = cache.NewSmartHistoryGeneration(s.Tag())
	s.recordObservationLocked(s.primary, true, 1, clock.Now())
	previous := s.historyGeneration
	if err := SmartReset(context.Background(), smartTestManager{values: []adapter.Outbound{s}}, ""); !errors.Is(err, cache.err) {
		t.Fatalf("reset error = %v", err)
	}
	if s.historyGeneration == previous || s.historyGeneration != cache.generation {
		t.Fatal("failed reset left future history writes fenced out")
	}
	if s.history.Stats["wifi"][s.primary.identity] == nil {
		t.Fatal("failed reset cleared learned history")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSmartOldResetCannotReclaimReplacementGeneration(t *testing.T) {
	for _, name := range []string{"success", "failure"} {
		t.Run(name, func(t *testing.T) {
			s, _ := smartTestGroup()
			cache := &smartResetGenerationCache{smartBlockingHistoryCache: new(smartBlockingHistoryCache)}
			if name == "failure" {
				cache.err = errors.New("reset commit failed")
			}
			s.cache = cache
			s.historyGeneration = cache.NewSmartHistoryGeneration(s.Tag())
			var replacement uint64
			cache.afterReset = func() { replacement = cache.NewSmartHistoryGeneration(s.Tag()) }
			err := SmartReset(context.Background(), smartTestManager{values: []adapter.Outbound{s}}, "")
			if !errors.Is(err, cache.err) {
				t.Fatalf("reset error = %v; want %v", err, cache.err)
			}
			if cache.generation != replacement || s.historyGeneration >= replacement {
				t.Fatal("old reset reclaimed the replacement's write ownership")
			}
			if _, err = cache.ResetSmartHistoryGeneration(s.Tag(), s.historyGeneration); !errors.Is(err, os.ErrInvalid) {
				t.Fatalf("stale reset error = %v", err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
