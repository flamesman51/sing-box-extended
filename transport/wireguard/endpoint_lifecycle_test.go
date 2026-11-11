package wireguard

import (
	"context"
	"encoding/base64"
	"net/netip"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-tun"
	M "github.com/sagernet/sing/common/metadata"
)

func endpointLifecycleOptions() EndpointOptions {
	return EndpointOptions{
		Context: context.Background(), Logger: log.NewNOPFactory().Logger(),
		PrivateKey: base64.StdEncoding.EncodeToString(make([]byte, 32)),
		Address:    []netip.Prefix{netip.MustParsePrefix("192.0.2.1/32"), netip.MustParsePrefix("2001:db8::1/128")},
		Workers:    1,
	}
}

func TestEndpointConstructionDoesNotStartDevice(t *testing.T) {
	if !tun.WithGVisor {
		t.Skip("userspace construction requires gVisor")
	}
	for _, system := range []bool{false, true} {
		t.Run(map[bool]string{false: "userspace", true: "system"}[system], func(t *testing.T) {
			baseline := runtime.NumGoroutine()
			for range 20 {
				options := endpointLifecycleOptions()
				options.System = system
				endpoint, err := NewEndpoint(options)
				if err != nil {
					t.Fatal(err)
				}
				inet4, inet6 := endpoint.PortAddresses()
				if inet4 != options.Address[0].Addr() || inet6 != options.Address[1].Addr() {
					t.Fatalf("pre-start port addresses = %s, %s", inet4, inet6)
				}
				if err = endpoint.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if count := runtime.NumGoroutine(); count > baseline+5 {
				t.Fatalf("construction leaks goroutines: before=%d after=%d", baseline, count)
			}
		})
	}
}

func TestEndpointDeviceValidation(t *testing.T) {
	for _, test := range []struct {
		name    string
		address []netip.Prefix
		error   string
	}{
		{"valid", endpointLifecycleOptions().Address, ""},
		{"invalid", []netip.Prefix{{}}, "invalid local address"},
		{"duplicate_ipv4", []netip.Prefix{netip.MustParsePrefix("192.0.2.1/32"), netip.MustParsePrefix("192.0.2.1/24")}, "duplicate address"},
		{"duplicate_ipv6", []netip.Prefix{netip.MustParsePrefix("2001:db8::1/128"), netip.MustParsePrefix("2001:db8::1/64")}, "duplicate address"},
		{"broadcast", []netip.Prefix{netip.MustParsePrefix("255.255.255.255/32")}, "duplicate address"},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, system := range []bool{false, true} {
				options := endpointLifecycleOptions()
				options.System, options.Address = system, test.address
				endpoint, err := NewEndpoint(options)
				want := test.error
				if !tun.WithGVisor {
					want = ""
					if !system {
						want = "gVisor is not included"
					}
				}
				if want != "" {
					if err == nil || !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), "create WireGuard device") {
						t.Fatalf("system=%v: want %q, got %v", system, want, err)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					if err = endpoint.Close(); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

func TestEndpointUnstartedDial(t *testing.T) {
	if !tun.WithGVisor {
		t.Skip("userspace construction requires gVisor")
	}
	endpoint, err := NewEndpoint(endpointLifecycleOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()
	destination := M.Socksaddr{Addr: netip.MustParseAddr("192.0.2.2"), Port: 9}
	if _, err = endpoint.DialContext(context.Background(), "tcp", destination); err == nil || !strings.Contains(err.Error(), "WireGuard device is not ready") {
		t.Fatalf("dial before Start: %v", err)
	}
	if _, err = endpoint.ListenPacket(context.Background(), destination); err == nil || !strings.Contains(err.Error(), "WireGuard device is not ready") {
		t.Fatalf("listen before Start: %v", err)
	}
	if err = endpoint.WritePackets(nil); err == nil {
		t.Fatal("packet input before Start succeeded")
	}
}

func TestEndpointConcurrentStart(t *testing.T) {
	if !tun.WithGVisor {
		t.Skip("userspace construction requires gVisor")
	}
	options := endpointLifecycleOptions()
	var err error
	options.Dialer, err = dialer.NewDefault(options.Context, option.DialerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := NewEndpoint(options)
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for ctx.Err() == nil {
				destination := M.Socksaddr{Addr: netip.MustParseAddr("192.0.2.2"), Port: 9}
				if connection, dialErr := endpoint.DialContext(ctx, "udp", destination); dialErr == nil {
					connection.Close()
				}
				if connection, listenErr := endpoint.ListenPacket(ctx, destination); listenErr == nil {
					connection.Close()
				}
				endpoint.PortAddresses()
				endpoint.Lookup(destination.Addr)
				_ = endpoint.WritePackets(nil)
			}
		})
	}
	err = endpoint.Start(false)
	cancel()
	readers.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if connection, listenErr := endpoint.ListenPacket(context.Background(), M.Socksaddr{Addr: netip.MustParseAddr("192.0.2.2")}); listenErr != nil {
		t.Fatal(listenErr)
	} else {
		connection.Close()
	}
	if err = endpoint.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = endpoint.ListenPacket(context.Background(), M.Socksaddr{Addr: netip.MustParseAddr("192.0.2.2")}); err == nil {
		t.Fatal("listen after Close succeeded")
	}
}
