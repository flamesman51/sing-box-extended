package endpoint

import (
	"context"
	"maps"
	"slices"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
)

type ConstructorFunc[T any] func(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options T) (adapter.Endpoint, error)

func Register[Options any](registry *Registry, outboundType string, constructor ConstructorFunc[Options]) {
	registry.register(outboundType, func() any {
		return new(Options)
	}, func(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, rawOptions any) (adapter.Endpoint, error) {
		var options *Options
		if rawOptions != nil {
			options = rawOptions.(*Options)
		}
		return constructor(ctx, router, logger, tag, common.PtrValueOrDefault(options))
	})
}

var _ adapter.EndpointRegistry = (*Registry)(nil)

type (
	optionsConstructorFunc func() any
	constructorFunc        func(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options any) (adapter.Endpoint, error)
)

type Registry struct {
	access      sync.Mutex
	optionsType map[string]optionsConstructorFunc
	constructor map[string]constructorFunc
	identities  map[adapter.Outbound]smartIdentity
}

type smartIdentity struct {
	local, server string
	err           error
}

func NewRegistry() *Registry {
	return &Registry{
		optionsType: make(map[string]optionsConstructorFunc),
		constructor: make(map[string]constructorFunc),
		identities:  make(map[adapter.Outbound]smartIdentity),
	}
}

func (m *Registry) OptionTypes() []string {
	m.access.Lock()
	defer m.access.Unlock()
	return slices.Sorted(maps.Keys(m.optionsType))
}

func (m *Registry) CreateOptions(outboundType string) (any, bool) {
	m.access.Lock()
	defer m.access.Unlock()
	optionsConstructor, loaded := m.optionsType[outboundType]
	if !loaded {
		return nil, false
	}
	return optionsConstructor(), true
}

func (m *Registry) Create(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, outboundType string, options any) (adapter.Endpoint, error) {
	m.access.Lock()
	defer m.access.Unlock()
	constructor, loaded := m.constructor[outboundType]
	if !loaded {
		return nil, E.New("outbound type not found: " + outboundType)
	}
	result, err := constructor(ctx, router, logger, tag, options)
	if err == nil {
		local, server, identityErr := adapter.ComputeSmartIdentity(outboundType, options)
		m.identities[result] = smartIdentity{local, server, identityErr}
	}
	return result, err
}

func (m *Registry) SmartIdentity(outbound adapter.Outbound) (string, string, error) {
	m.access.Lock()
	defer m.access.Unlock()
	identity, loaded := m.identities[outbound]
	if !loaded {
		return "", "", E.New("endpoint identity unavailable")
	}
	return identity.local, identity.server, identity.err
}

func (m *Registry) register(outboundType string, optionsConstructor optionsConstructorFunc, constructor constructorFunc) {
	m.access.Lock()
	defer m.access.Unlock()
	m.optionsType[outboundType] = optionsConstructor
	m.constructor[outboundType] = constructor
}
