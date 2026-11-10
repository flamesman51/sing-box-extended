//go:build linux && with_wireguard && with_gvisor

package smartharness_test

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/group"
	sJSON "github.com/sagernet/sing/common/json"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"
)

type harnessBox struct {
	*box.Box
	ctx    context.Context
	cancel context.CancelFunc
}

func tryStartHarnessBox(config map[string]any) (*harnessBox, error) {
	ctx, cancel := context.WithCancel(include.Context(service.ContextWithDefaultRegistry(context.Background())))
	content, err := json.Marshal(config)
	if err != nil {
		cancel()
		return nil, err
	}
	var options option.Options
	if err = sJSON.UnmarshalContext(ctx, content, &options); err != nil {
		cancel()
		return nil, err
	}
	instance, err := box.New(box.Options{Context: ctx, Options: options})
	if err != nil {
		cancel()
		return nil, err
	}
	if err = instance.Start(); err != nil {
		closeErr := instance.Close()
		cancel()
		if closeErr != nil {
			return nil, fmt.Errorf("close box after startup failure (%v): %w", err, closeErr)
		}
		return nil, err
	}
	return &harnessBox{Box: instance, ctx: ctx, cancel: cancel}, nil
}

func startHarnessBox(t *testing.T, config map[string]any) *harnessBox {
	t.Helper()
	h, err := tryStartHarnessBox(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.close(t) })
	return h
}

func startHarnessServer(t *testing.T, config, endpoint map[string]any) uint16 {
	t.Helper()
	var err error
	for attempt := range 5 {
		if attempt > 0 {
			endpoint["listen_port"] = reserveUDPPort(t)
		}
		var h *harnessBox
		h, err = tryStartHarnessBox(config)
		if err == nil {
			t.Cleanup(func() { h.close(t) })
			return endpoint["listen_port"].(uint16)
		}
		if !errors.Is(err, syscall.EADDRINUSE) {
			t.Fatal(err)
		}
	}
	t.Fatalf("server bind failed after five complete startup attempts: %v", err)
	return 0
}

func (h *harnessBox) close(t *testing.T) {
	t.Helper()
	if h.Box != nil {
		if err := h.Box.Close(); err != nil {
			t.Error(err)
		}
		h.cancel()
		h.Box = nil
	}
}

func wireGuardKeys(t *testing.T) (string, string) {
	t.Helper()
	key, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(key.Bytes()), base64.StdEncoding.EncodeToString(key.PublicKey().Bytes())
}

func endpointConfig(tag, address, private string, port uint16, peer map[string]any) map[string]any {
	return map[string]any{"type": "wireguard", "tag": tag, "system": false, "address": []string{address}, "private_key": private, "listen_port": port, "mtu": 1280, "workers": 2, "peers": []any{peer}}
}

func reserveUDPPort(t *testing.T) uint16 {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	port := uint16(conn.LocalAddr().(*net.UDPAddr).Port)
	_ = conn.Close()
	return port
}

type harnessMember struct {
	fixtures *probeFixtures
	relay    *faultRelay
	faults   relayFaults
}

type harnessStatus struct {
	Version  int `json:"version"`
	Selected struct {
		TCP string `json:"tcp"`
	} `json:"selected"`
	Budget  harnessBudget            `json:"budget"`
	Budgets map[string]harnessBudget `json:"budgets"`
	Panics  *struct {
		Total int `json:"total"`
	} `json:"panics"`
	Members []struct {
		Tag      string   `json:"tag"`
		State    string   `json:"state"`
		UDP      string   `json:"udp"`
		QUIC     string   `json:"udp443"`
		Download *float64 `json:"download_mbps"`
		Source   string   `json:"throughput_source"`
		At       int64    `json:"throughput_at"`
	} `json:"members"`
	Events []struct {
		Kind string `json:"kind"`
	} `json:"events"`
}

type harnessBudget struct {
	Day      string  `json:"day"`
	Used     float64 `json:"used_mb"`
	Reserved float64 `json:"reserved_mb"`
	Limit    float64 `json:"limit_mb"`
}

func statusHarness(t *testing.T, client *harnessBox) harnessStatus {
	t.Helper()
	raw, err := group.SmartStatus(client.ctx, client.Outbound(), "smart")
	if err != nil {
		t.Fatal(err)
	}
	var status harnessStatus
	if err = json.Unmarshal([]byte(raw), &status); err != nil {
		t.Fatal(err)
	}
	if status.Version != 1 {
		t.Fatalf("status version=%d", status.Version)
	}
	if status.Panics == nil {
		t.Fatal("smart status lacks required panics field")
	}
	if status.Panics.Total != 0 {
		t.Fatalf("contained panics=%d", status.Panics.Total)
	}
	if status.Budget.Used < 0 || status.Budget.Reserved < 0 || status.Budget.Used+status.Budget.Reserved > status.Budget.Limit {
		t.Fatalf("active throughput exceeded budget: %+v", status.Budget)
	}
	return status
}

func awaitHarness(t *testing.T, client *harnessBox, timeout time.Duration, description string, predicate func(harnessStatus) bool) harnessStatus {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		status := statusHarness(t, client)
		if predicate(status) {
			return status
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s: %+v", description, status)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func requestHarness(t *testing.T, client *harnessBox) {
	t.Helper()
	outbound, ok := client.Outbound().Outbound("smart")
	if !ok {
		t.Fatal("smart outbound missing")
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		return outbound.DialContext(ctx, network, M.ParseSocksaddr(address))
	}}
	defer transport.CloseIdleConnections()
	httpClient := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	response, err := httpClient.Get("https://1.1.1.1/cdn-cgi/trace")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusOK || string(body) != "h=1.1.1.1\n" {
		t.Fatalf("tunneled request: status=%d body=%q error=%v", response.StatusCode, body, err)
	}
}

func metadataHarness(t *testing.T, client *harnessBox) map[string]json.RawMessage {
	t.Helper()
	cache := service.FromContext[adapter.CacheFile](client.ctx)
	if cache == nil {
		t.Fatal("box did not register its cache")
	}
	metadata, err := cache.LoadSmartMetadata()
	if err != nil {
		t.Fatal(err)
	}
	if metadata == nil {
		t.Fatal("installation metadata missing")
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(metadata.Data, &fields); err != nil {
		t.Fatal(err)
	}
	return fields
}

func TestSmartHarness(t *testing.T) {
	started := time.Now()
	owner := t
	t.Cleanup(func() { t.Logf("real-component harness runtime: %s", time.Since(started)) })
	var members []harnessMember
	var endpoints []any
	for i, entry := range []struct {
		tag   string
		mbps  float64
		delay time.Duration
	}{{"fast", 20, 0}, {"medium", 5, 10 * time.Millisecond}, {"slow", 1.5, 20 * time.Millisecond}} {
		overlay := netip.AddrFrom4([4]byte{10, 200, byte(i + 1), 1})
		fixtures := newProbeFixtures(t, overlay)
		serverPrivate, serverPublic := wireGuardKeys(t)
		clientPrivate, clientPublic := wireGuardKeys(t)
		serverPort := reserveUDPPort(t)
		rules := []any{}
		for _, route := range []struct {
			network string
			ports   []int
			target  uint16
		}{{"tcp", []int{443}, fixtures.httpsPort}, {"tcp", []int{53}, fixtures.dnsPort}, {"udp", []int{3478, 19302}, fixtures.stunPort}, {"udp", []int{443}, fixtures.quicPort}} {
			rules = append(rules, map[string]any{"network": route.network, "port": route.ports, "action": "route", "outbound": "local", "override_address": "127.0.0.1", "override_port": route.target})
		}
		rules = append(rules, map[string]any{"action": "reject"})
		serverEndpoint := endpointConfig("server", overlay.String()+"/24", serverPrivate, serverPort, map[string]any{"public_key": clientPublic, "allowed_ips": []string{fmt.Sprintf("10.200.%d.2/32", i+1)}})
		serverConfig := map[string]any{"log": map[string]any{"disabled": true}, "endpoints": []any{serverEndpoint}, "outbounds": []any{map[string]any{"type": "direct", "tag": "local"}}, "route": map[string]any{"rules": rules}}
		serverPort = startHarnessServer(t, serverConfig, serverEndpoint)
		faults := relayFaults{bytesPerSecond: int(entry.mbps * 1_000_000 / 8), delay: entry.delay}
		relay := newFaultRelay(t, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: int(serverPort)}, faults)
		clientEndpoint := endpointConfig(entry.tag, fmt.Sprintf("10.200.%d.2/24", i+1), clientPrivate, 0, map[string]any{"address": "127.0.0.1", "port": relay.conn.LocalAddr().(*net.UDPAddr).Port, "public_key": serverPublic, "allowed_ips": []string{"0.0.0.0/0"}})
		endpoints = append(endpoints, clientEndpoint)
		members = append(members, harnessMember{fixtures: fixtures, relay: relay, faults: faults})
	}
	config := map[string]any{
		"log":       map[string]any{"disabled": true},
		"endpoints": endpoints,
		"outbounds": []any{map[string]any{
			"type":      "smart",
			"tag":       "smart",
			"outbounds": []string{"fast", "medium", "slow"},
			"probe": map[string]any{
				"disable_direct":      true,
				"heartbeat_interval":  "30s",
				"standby_interval":    "30s",
				"background_interval": "30s",
				"timeout":             "2s",
			},
			"throughput": map[string]any{
				"target_mbps":              10,
				"download_url":             "https://fixture.invalid/download?bytes={bytes}",
				"upload_url":               "https://fixture.invalid/upload",
				"wifi_daily_budget_mb":     100,
				"cellular_daily_budget_mb": 100,
				"other_daily_budget_mb":    100,
			},
		}},
		"route":        map[string]any{"final": "smart"},
		"experimental": map[string]any{"cache_file": map[string]any{"enabled": true, "path": filepath.Join(t.TempDir(), "cache.db")}},
	}
	client := startHarnessBox(t, config)
	requestHarness(t, client)
	var calibrated harnessStatus
	if !t.Run("initial_selection_after_calibration", func(t *testing.T) {
		calibrated = awaitHarness(t, client, 90*time.Second, "all three active calibrations", func(status harnessStatus) bool {
			for _, member := range status.Members {
				if member.Download == nil || member.Source != "active" {
					return false
				}
			}
			return len(status.Members) == 3 && status.Budget.Reserved == 0
		})
		if calibrated.Selected.TCP != "fast" {
			t.Fatalf("selected=%s; only fast should meet 10 Mbit/s", calibrated.Selected.TCP)
		}
		for _, member := range calibrated.Members {
			rate := *member.Download
			t.Logf("%s calibrated %.2f Mbit/s", member.Tag, rate)
			if member.Tag == "fast" && rate < 10 || member.Tag != "fast" && rate >= 10 {
				t.Fatalf("%s calibrated %.2f Mbit/s, inconsistent with configured link", member.Tag, rate)
			}
		}
	}) {
		return
	}
	t.Run("udp_through_wireguard_gvisor", func(t *testing.T) {
		awaitHarness(t, client, 20*time.Second, "real STUN and QUIC probes", func(status harnessStatus) bool {
			for _, member := range status.Members {
				if member.Tag == "fast" {
					return member.UDP == "ok" && member.QUIC == "ok"
				}
			}
			return false
		})
		if members[0].fixtures.stun.Load() == 0 || members[0].fixtures.quic.Load() == 0 || members[0].fixtures.dns.Load() == 0 || members[0].relay.forwarded.Load() == 0 {
			t.Fatal("probe did not traverse fixtures and encrypted UDP relay")
		}
	})
	before := metadataHarness(t, client)
	if !t.Run("budget", func(t *testing.T) {
		if calibrated.Budget.Used <= 0 || calibrated.Budget.Reserved != 0 || calibrated.Budget.Limit != 100 {
			t.Fatalf("budget accounting missing: %+v", calibrated.Budget)
		}
		var ledger struct {
			Starts       []int64        `json:"starts"`
			Reservations map[string]any `json:"reservations"`
		}
		if err := json.Unmarshal(before["ledger"], &ledger); err != nil {
			t.Fatal(err)
		}
		if len(ledger.Starts) != 3 || len(ledger.Reservations) != 0 {
			t.Fatalf("wanted three reconciled active tests: starts=%d reservations=%d", len(ledger.Starts), len(ledger.Reservations))
		}
	}) {
		return
	}
	if !t.Run("restart_persistence", func(t *testing.T) {
		client.close(t)
		client = startHarnessBox(owner, config)
		after := metadataHarness(t, client)
		for _, field := range []string{"install_id", "hmac_seed", "direct_seed", "ledger"} {
			if len(before[field]) == 0 || !reflect.DeepEqual(before[field], after[field]) {
				t.Fatalf("installation field %s was not restored", field)
			}
		}
		restored := statusHarness(t, client)
		if restored.Budget.Used != calibrated.Budget.Used || restored.Budget.Reserved != 0 {
			t.Fatalf("ledger not restored: before=%+v after=%+v", calibrated.Budget, restored.Budget)
		}
		for _, original := range calibrated.Members {
			found := false
			for _, member := range restored.Members {
				if member.Tag == original.Tag {
					found = member.Download != nil && *member.Download == *original.Download && member.Source == "active" && member.At == original.At
				}
			}
			if !found {
				t.Fatalf("active history for %s was not restored", original.Tag)
			}
		}
		var speedtests int
		for _, event := range restored.Events {
			if event.Kind == "speedtest" {
				speedtests++
			}
		}
		if speedtests != 3 {
			t.Fatalf("restored speedtest events=%d", speedtests)
		}
		requestHarness(t, client)
	}) {
		return
	}
	t.Run("blackhole_failover", func(t *testing.T) {
		awaitHarness(t, client, 10*time.Second, "fast member revalidation", func(status harnessStatus) bool { return status.Selected.TCP == "fast" })
		faults := members[0].faults
		faults.blackhole = true
		members[0].relay.set(faults)
		awaitHarness(t, client, 45*time.Second, "corroborated failure and reachable failover", func(status harnessStatus) bool {
			if status.Selected.TCP == "" || status.Selected.TCP == "fast" {
				return false
			}
			failed, reachable := false, false
			for _, member := range status.Members {
				if member.Tag == "fast" {
					failed = member.State == "quarantined"
				}
				if member.Tag == status.Selected.TCP {
					reachable = member.State == "healthy"
				}
			}
			return failed && reachable
		})
		requestHarness(t, client)
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if statusHarness(t, client).Selected.TCP == "fast" {
				t.Fatal("unreachable member selected despite a reachable replacement")
			}
			time.Sleep(100 * time.Millisecond)
		}
		if members[0].relay.dropped.Load() == 0 {
			t.Fatal("blackhole dropped no outer WireGuard datagrams")
		}
	})
	t.Run("no_panics", func(t *testing.T) {
		status := statusHarness(t, client)
		if status.Panics == nil {
			t.Fatal("smart status lacks required panics field; integrate crash containment")
		}
		if len(status.Budgets) != 3 {
			t.Fatal("smart status lacks all three budget classes; integrate crash containment")
		}
	})
}
