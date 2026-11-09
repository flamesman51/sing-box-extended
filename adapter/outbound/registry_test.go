package outbound

import (
	"context"
	"runtime"
	"testing"
	"weak"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	N "github.com/sagernet/sing/common/network"
)

type smartIdentityTestOutbound struct {
	Adapter
	N.Dialer
}

type smartIdentityTestOptions struct {
	Server string `json:"server"`
}

func TestSmartIdentityDoesNotRetainOutbound(t *testing.T) {
	registry := NewRegistry()
	Register(registry, "identity-test", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ smartIdentityTestOptions) (adapter.Outbound, error) {
		return &smartIdentityTestOutbound{Adapter: NewAdapter("identity-test", tag, nil, nil)}, nil
	})
	pointer := createSmartIdentityTestOutbound(t, registry)
	for range 10 {
		runtime.GC()
		if pointer.Value() == nil {
			runtime.KeepAlive(registry)
			return
		}
	}
	runtime.KeepAlive(registry)
	t.Fatal("registry retained an otherwise unreachable outbound")
}

func createSmartIdentityTestOutbound(t *testing.T, registry *Registry) weak.Pointer[smartIdentityTestOutbound] {
	t.Helper()
	options := &smartIdentityTestOptions{Server: "example.test"}
	outbound, err := registry.CreateOutbound(context.Background(), nil, nil, "member", "identity-test", options)
	if err != nil {
		t.Fatal(err)
	}
	local, server, err := registry.SmartIdentity(outbound)
	if err != nil {
		t.Fatal(err)
	}
	expectedLocal, expectedServer, err := adapter.ComputeSmartIdentity("identity-test", options)
	if err != nil || local != expectedLocal || server != expectedServer {
		t.Fatalf("identity unavailable during instance lifetime: %q %q %v", local, server, err)
	}
	return weak.Make(outbound.(*smartIdentityTestOutbound))
}
