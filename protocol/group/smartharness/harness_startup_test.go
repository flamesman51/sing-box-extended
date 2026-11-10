//go:build linux && with_wireguard && with_gvisor

package smartharness_test

import (
	"errors"
	"net"
	"syscall"
	"testing"
)

func TestSmartHarnessServerRetriesBindCollision(t *testing.T) {
	for _, network := range []string{"udp4", "udp6"} {
		t.Run(network, func(t *testing.T) {
			occupied, err := net.ListenUDP(network, &net.UDPAddr{})
			if errors.Is(err, syscall.EAFNOSUPPORT) || errors.Is(err, syscall.EADDRNOTAVAIL) {
				t.Skip("address family unavailable")
			}
			if err != nil {
				t.Fatal(err)
			}
			defer occupied.Close()
			port := uint16(occupied.LocalAddr().(*net.UDPAddr).Port)
			private, _ := wireGuardKeys(t)
			_, peerPublic := wireGuardKeys(t)
			endpoint := endpointConfig("server", "10.200.254.1/24", private, port, map[string]any{"public_key": peerPublic, "allowed_ips": []string{"10.200.254.2/32"}})
			config := map[string]any{"log": map[string]any{"disabled": true}, "endpoints": []any{endpoint}, "outbounds": []any{map[string]any{"type": "direct", "tag": "local"}}}
			bound := startHarnessServer(t, config, endpoint)
			probeOccupied, occupyErr := net.ListenUDP(network, &net.UDPAddr{Port: int(port)})
			if occupyErr == nil {
				_ = probeOccupied.Close()
				t.Fatal("occupying wildcard socket no longer holds its port")
			}
			if !errors.Is(occupyErr, syscall.EADDRINUSE) {
				t.Fatal(occupyErr)
			}
			if bound == port {
				t.Fatal("server reused the occupied port")
			}
			probe, err := net.ListenUDP(network, &net.UDPAddr{Port: int(bound)})
			if err == nil {
				_ = probe.Close()
				t.Fatal("server did not bind its replacement port")
			}
			if !errors.Is(err, syscall.EADDRINUSE) {
				t.Fatal(err)
			}
		})
	}
}
