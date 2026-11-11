package rate

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	E "github.com/sagernet/sing/common/exceptions"

	"github.com/AliRizaAynaci/gorl/v2"
	"github.com/AliRizaAynaci/gorl/v2/core"
	"github.com/shtorm-7/go-cache/v2"
)

type (
	ConnIDGetter = func(context.Context, *adapter.InboundContext) (string, bool)
	RateGetter   = func(id string) error

	RateStrategy interface {
		request(ctx context.Context, metadata *adapter.InboundContext) error
	}
)

type DefaultRateStrategy struct {
	limiter core.Limiter
	queue   chan struct{}
}

func NewDefaultRateStrategy(strategy string, count int, interval time.Duration) (*DefaultRateStrategy, error) {
	limiter, err := gorl.New(core.Config{
		Strategy: core.StrategyType(strings.ReplaceAll(strategy, "-", "_")),
		Limit:    count,
		Window:   interval,
	})
	if err != nil {
		return nil, err
	}
	return &DefaultRateStrategy{limiter: limiter, queue: make(chan struct{}, 1)}, nil
}

func (s *DefaultRateStrategy) request(ctx context.Context, metadata *adapter.InboundContext) error {
	select {
	case s.queue <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() {
		<-s.queue
	}()
	r, err := s.limiter.Allow(ctx, metadata.Destination.String())
	if err != nil {
		return err
	}
	if !r.Allowed {
		select {
		case <-time.After(r.RetryAfter):
			_, err = s.limiter.Allow(ctx, metadata.Destination.String())
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (s *DefaultRateStrategy) close() error {
	return s.limiter.Close()
}

type ConnectionRateStrategy struct {
	connIDGetter   ConnIDGetter
	createStrategy func() (*DefaultRateStrategy, error)
	limiters       *cache.Cache[string, *DefaultRateStrategy]

	mtx sync.Mutex
}

func NewConnectionRateStrategy(connIDGetter ConnIDGetter, strategy string, count int, interval time.Duration) (*ConnectionRateStrategy, error) {
	limiters := cache.New[string, *DefaultRateStrategy](interval, time.Second)
	limiters.OnEvicted(func(s string, strategy *DefaultRateStrategy) {
		strategy.close()
	})
	return &ConnectionRateStrategy{
		connIDGetter: connIDGetter,
		createStrategy: func() (*DefaultRateStrategy, error) {
			return NewDefaultRateStrategy(strategy, count, interval)
		},
		limiters: limiters,
	}, nil
}

func (s *ConnectionRateStrategy) request(ctx context.Context, metadata *adapter.InboundContext) error {
	id, ok := s.connIDGetter(ctx, metadata)
	if !ok {
		return E.New("id not found")
	}
	s.mtx.Lock()
	strategy, ok := s.limiters.Get(id)
	if !ok {
		newStrategy, err := s.createStrategy()
		if err != nil {
			return err
		}
		s.limiters.SetDefault(id, newStrategy)
		strategy = newStrategy
	} else {
		s.limiters.UpdateExpirationDefault(id)
	}
	s.mtx.Unlock()
	return strategy.request(ctx, metadata)
}

type UsersRateStrategy struct {
	strategies map[string]RateStrategy
	mtx        sync.Mutex
}

func NewUsersRateStrategy(strategies map[string]RateStrategy) *UsersRateStrategy {
	return &UsersRateStrategy{
		strategies: strategies,
	}
}

func (s *UsersRateStrategy) request(ctx context.Context, metadata *adapter.InboundContext) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	var user string
	if metadata != nil {
		user = metadata.User
	}
	strategy, ok := s.strategies[user]
	if ok {
		return strategy.request(ctx, metadata)
	}
	return E.New("user strategy not found: ", user)
}

type ManagerRateStrategy struct {
	*UsersRateStrategy
}

func NewManagerRateStrategy() *ManagerRateStrategy {
	return &ManagerRateStrategy{
		UsersRateStrategy: NewUsersRateStrategy(map[string]RateStrategy{}),
	}
}

func (s *ManagerRateStrategy) UpdateStrategies(strategies map[string]RateStrategy) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.strategies = strategies
}

type BypassRateStrategy struct{}

func NewBypassRateStrategy() *BypassRateStrategy {
	return &BypassRateStrategy{}
}

func (s *BypassRateStrategy) request(ctx context.Context, metadata *adapter.InboundContext) error {
	return nil
}

func CreateStrategy(strategy string, connectionType string, count int, interval time.Duration) (RateStrategy, error) {
	if strategy == "bypass" {
		return NewBypassRateStrategy(), nil
	}
	var connIDGetter ConnIDGetter
	switch connectionType {
	case "hwid":
		connIDGetter = func(ctx context.Context, metadata *adapter.InboundContext) (string, bool) {
			id, ok := ctx.Value("hwid").(string)
			return id, ok
		}
	case "mux":
		connIDGetter = func(ctx context.Context, metadata *adapter.InboundContext) (string, bool) {
			id, ok := log.MuxIDFromContext(ctx)
			if !ok {
				return "", ok
			}
			return strconv.FormatUint(uint64(id.ID), 10), ok
		}
	case "source_ip":
		connIDGetter = func(ctx context.Context, metadata *adapter.InboundContext) (string, bool) {
			return metadata.Source.IPAddr().String(), true
		}
	case "default", "":
		return NewDefaultRateStrategy(strategy, count, interval)
	default:
		return nil, E.New("connection type not found: ", connectionType)
	}
	return NewConnectionRateStrategy(connIDGetter, strategy, count, interval)
}
