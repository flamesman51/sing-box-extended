package endpoint

import (
	"context"
	"runtime"
	"testing"
	"weak"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	N "github.com/sagernet/sing/common/network"
)

type smartIdentityTestEndpoint struct {
	Adapter
	adapter.Lifecycle
	N.Dialer
}

type smartIdentityTestOptions struct {
	Server string `json:"server"`
}

func TestSmartIdentityDoesNotRetainEndpoint(t *testing.T) {
	registry := NewRegistry()
	Register(registry, "identity-test", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ smartIdentityTestOptions) (adapter.Endpoint, error) {
		return &smartIdentityTestEndpoint{Adapter: NewAdapter("identity-test", tag, nil, nil)}, nil
	})
	pointer := createSmartIdentityTestEndpoint(t, registry)
	for range 10 {
		runtime.GC()
		if pointer.Value() == nil {
			runtime.KeepAlive(registry)
			return
		}
	}
	runtime.KeepAlive(registry)
	t.Fatal("registry retained an otherwise unreachable endpoint")
}

func createSmartIdentityTestEndpoint(t *testing.T, registry *Registry) weak.Pointer[smartIdentityTestEndpoint] {
	t.Helper()
	options := &smartIdentityTestOptions{Server: "example.test"}
	endpoint, err := registry.Create(context.Background(), nil, nil, "member", "identity-test", options)
	if err != nil {
		t.Fatal(err)
	}
	local, server, err := registry.SmartIdentity(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	expectedLocal, expectedServer, err := adapter.ComputeSmartIdentity("identity-test", options)
	if err != nil || local != expectedLocal || server != expectedServer {
		t.Fatalf("identity unavailable during instance lifetime: %q %q %v", local, server, err)
	}
	return weak.Make(endpoint.(*smartIdentityTestEndpoint))
}
