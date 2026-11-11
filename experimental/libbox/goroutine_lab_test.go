//go:build with_gvisor && with_wireguard

package libbox

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"

	"github.com/sagernet/sing-box/daemon"
	"github.com/sagernet/sing/service/filemanager"

	"google.golang.org/grpc"
)

// TestGoroutineLifecycleLab exercises the Android command protocol without Android or a VPN.
func TestGoroutineLifecycleLab(t *testing.T) {
	path := os.Getenv("GOROUTINE_LAB_CONFIG")
	if path == "" {
		t.Skip("set GOROUTINE_LAB_CONFIG to run the optional lifecycle lab")
	}
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		t.Skip("optional GOROUTINE_LAB_CONFIG is absent")
	}
	if err != nil {
		t.Fatal("cannot read optional lab configuration")
	}
	config := string(content)
	checkGoroutineLabConfig(t, config)
	directory := t.TempDir()
	ctx := filemanager.WithDefault(baseContext(nil), directory, directory, os.Getuid(), os.Getgid())
	server := daemon.NewStartedService(daemon.ServiceOptions{Context: ctx, LogMaxLines: 10})
	defer server.Close()
	defer server.CloseService()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	daemon.RegisterStartedServiceServer(grpcServer, server)
	go grpcServer.Serve(listener)
	defer grpcServer.Stop()
	start := time.Now()
	cycles := 10
	dwell := 100 * time.Millisecond
	if value := os.Getenv("GOROUTINE_LAB_CYCLES"); value != "" {
		cycles, err = strconv.Atoi(value)
		if err != nil || cycles < 1 {
			t.Fatal("GOROUTINE_LAB_CYCLES must be positive")
		}
	}
	if value := os.Getenv("GOROUTINE_LAB_DWELL"); value != "" {
		dwell, err = time.ParseDuration(value)
		if err != nil || dwell <= 0 {
			t.Fatal("GOROUTINE_LAB_DWELL must be positive")
		}
	}
	if output := os.Getenv("GOROUTINE_LAB_OUTPUT"); output != "" {
		if err = os.MkdirAll(output, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	baseline := runtime.NumGoroutine()
	for cycle := range cycles {
		if err = server.StartOrReloadService(ctx, config, &daemon.OverrideOptions{}); err != nil {
			t.Fatal("lab service startup failed (configuration details suppressed)")
		}
		options := &CommandClientOptions{StatusInterval: int64(10 * time.Millisecond)}
		options.AddCommand(CommandStatus)
		options.AddCommand(CommandGroup)
		client := NewCommandClient(goroutineLabHandler{}, options)
		err = client.connect(func(ctx context.Context) (*grpc.ClientConn, daemon.StartedServiceClient, error) {
			return client.dialWithRetry(ctx, listener.Addr().String(), localDialOptions(nil), false)
		})
		if err != nil {
			t.Fatal(err)
		}
		for range 10 {
			if _, err = client.GetSmartStatus(""); err != nil {
				t.Fatal(err)
			}
		}
		var traffic *http.Client
		if proxyAddress := os.Getenv("GOROUTINE_LAB_PROXY"); proxyAddress != "" {
			proxy, parseErr := url.Parse(proxyAddress)
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			traffic = &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxy)}, Timeout: time.Second}
		}
		for until := time.Now().Add(dwell); time.Now().Before(until); {
			if traffic != nil {
				response, requestErr := traffic.Get("https://1.1.1.1/cdn-cgi/trace")
				if requestErr == nil {
					_ = response.Body.Close()
				}
			}
			if os.Getenv("GOROUTINE_LAB_EVENTS") != "" {
				instance := server.Instance()
				instance.Box().CloseIdleConnections()
				instance.PauseManager().DevicePause()
				time.Sleep(time.Millisecond)
				instance.PauseManager().DeviceWake()
				instance.Box().Network().ResetNetwork(context.Background())
				instance.Box().Network().UpdateWIFIState(context.Background())
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Logf("active cycle=%d elapsed=%s goroutines=%d", cycle+1, time.Since(start).Round(time.Millisecond), runtime.NumGoroutine())
		if traffic != nil {
			traffic.CloseIdleConnections()
		}
		if output := os.Getenv("GOROUTINE_LAB_OUTPUT"); output != "" {
			file, openErr := os.Create(filepath.Join(output, fmt.Sprintf("active-%02d.txt", cycle)))
			if openErr != nil {
				t.Fatal(openErr)
			}
			writeErr := pprof.Lookup("goroutine").WriteTo(file, 1)
			closeErr := file.Close()
			if writeErr != nil {
				t.Fatal(writeErr)
			}
			if closeErr != nil {
				t.Fatal(closeErr)
			}
		}
		if err = client.Disconnect(); err != nil {
			t.Fatal(err)
		}
		if err = server.CloseService(); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for runtime.NumGoroutine() > baseline+5 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		count := runtime.NumGoroutine()
		t.Logf("stopped cycle=%d elapsed=%s baseline=%d goroutines=%d", cycle+1, time.Since(start).Round(time.Millisecond), baseline, count)
		if count > baseline+5 {
			t.Fatalf("goroutines did not return to baseline: before=%d after=%d", baseline, count)
		}
	}
	if output := os.Getenv("GOROUTINE_LAB_OUTPUT"); output != "" {
		if err = os.MkdirAll(output, 0o700); err != nil {
			t.Fatal(err)
		}
		file, openErr := os.Create(filepath.Join(output, "goroutine.txt"))
		if openErr != nil {
			t.Fatal(openErr)
		}
		err = pprof.Lookup("goroutine").WriteTo(file, 1)
		closeErr := file.Close()
		if err != nil {
			t.Fatal(err)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	}
	if count := runtime.NumGoroutine(); count > baseline+5 {
		t.Fatalf("goroutines did not return to baseline: before=%d after=%d", baseline, count)
	}
}

func checkGoroutineLabConfig(t *testing.T, config string) {
	t.Helper()
	var options struct {
		Inbounds []struct {
			Type, Listen string
		}
		Endpoints []struct {
			Type   string
			System bool
		}
		Services     []json.RawMessage
		Experimental struct {
			Debug    struct{ Listen string }
			ClashAPI struct {
				Listen string `json:"external_controller"`
			} `json:"clash_api"`
		}
	}
	if err := json.Unmarshal([]byte(config), &options); err != nil {
		t.Fatal("lab configuration must be JSON")
	}
	for _, inbound := range options.Inbounds {
		if inbound.Type != "mixed" || !net.ParseIP(inbound.Listen).IsLoopback() {
			t.Fatal("lab permits only loopback mixed inbounds, never TUN or routing changes")
		}
	}
	for _, endpoint := range options.Endpoints {
		if endpoint.Type != "wireguard" || endpoint.System {
			t.Fatal("lab permits only userspace WireGuard endpoints")
		}
	}
	if len(options.Services) != 0 {
		t.Fatal("lab does not permit services")
	}
	for _, listen := range []string{options.Experimental.Debug.Listen, options.Experimental.ClashAPI.Listen} {
		if listen == "" {
			continue
		}
		host, _, err := net.SplitHostPort(listen)
		if err != nil || !net.ParseIP(host).IsLoopback() {
			t.Fatal("lab debug and Clash listeners must use loopback")
		}
	}
}

func goroutineLabConfig(t *testing.T) string {
	t.Helper()
	key, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	peer, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var endpoints []any
	var tags []string
	for index := range 13 {
		tag := fmt.Sprintf("lab-%d", index)
		tags = append(tags, tag)
		endpoints = append(endpoints, map[string]any{
			"type": "wireguard", "tag": tag, "address": []string{"10.0.0.1/32"}, "workers": 1,
			"private_key": base64.StdEncoding.EncodeToString(key.Bytes()),
			"peers":       []any{map[string]any{"address": "127.0.0.1", "port": 9, "public_key": base64.StdEncoding.EncodeToString(peer.PublicKey().Bytes()), "allowed_ips": []string{"0.0.0.0/0"}}},
		})
	}
	config := map[string]any{
		"log":          map[string]any{"disabled": true},
		"endpoints":    endpoints,
		"outbounds":    []any{map[string]any{"type": "smart", "tag": "smart", "outbounds": tags, "throughput": map[string]any{"enabled": false}, "probe": map[string]any{"disable_direct": true, "timeout": "20ms", "heartbeat_interval": "20ms", "standby_interval": "20ms", "background_interval": "20ms"}}},
		"experimental": map[string]any{"cache_file": map[string]any{"enabled": true}},
	}
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

type goroutineLabHandler struct{}

func (goroutineLabHandler) Connected()                                 {}
func (goroutineLabHandler) Disconnected(string)                        {}
func (goroutineLabHandler) SetDefaultLogLevel(int32)                   {}
func (goroutineLabHandler) ClearLogs()                                 {}
func (goroutineLabHandler) WriteLogs(LogIterator)                      {}
func (goroutineLabHandler) WriteStatus(*StatusMessage)                 {}
func (goroutineLabHandler) WriteGroups(OutboundGroupIterator)          {}
func (goroutineLabHandler) WriteOutbounds(OutboundGroupItemIterator)   {}
func (goroutineLabHandler) InitializeClashMode(StringIterator, string) {}
func (goroutineLabHandler) UpdateClashMode(string)                     {}
func (goroutineLabHandler) WriteConnectionEvents(*ConnectionEvents)    {}
