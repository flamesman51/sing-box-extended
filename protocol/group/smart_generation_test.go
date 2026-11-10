package group

import (
	"context"
	"errors"
	"testing"

	"github.com/sagernet/sing-box/adapter"
)

type smartResetGenerationCache struct {
	*smartBlockingHistoryCache
	generation uint64
	err        error
}

func (c *smartResetGenerationCache) NewSmartHistoryGeneration(string) uint64 {
	c.generation++
	return c.generation
}

func (c *smartResetGenerationCache) ResetSmartHistory(string) error {
	c.generation++
	return c.err
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
