//go:build linux && with_wireguard && with_gvisor

package smartharness_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

type harnessBulk struct {
	bytes  atomic.Int64
	done   chan error
	cancel context.CancelFunc
}

func startHarnessBulk(t *testing.T, client *harnessBox) *harnessBulk {
	t.Helper()
	outbound, ok := client.Outbound().Outbound("smart")
	if !ok {
		t.Fatal("smart outbound missing")
	}
	ctx, cancel := context.WithCancel(context.Background())
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		return outbound.DialContext(ctx, network, M.ParseSocksaddr(address))
	}}
	bulk := &harnessBulk{done: make(chan error, 1), cancel: cancel}
	go func() {
		defer close(bulk.done)
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://1.1.1.1/bulk", nil)
		if err != nil {
			bulk.done <- err
			return
		}
		response, err := (&http.Client{Transport: transport}).Do(request)
		if err != nil {
			bulk.done <- err
			return
		}
		defer response.Body.Close()
		var buffer [32 * 1024]byte
		for {
			n, readErr := response.Body.Read(buffer[:])
			bulk.bytes.Add(int64(n))
			if readErr != nil {
				bulk.done <- readErr
				return
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		transport.CloseIdleConnections()
		select {
		case <-bulk.done:
		case <-time.After(5 * time.Second):
			t.Error("bulk download did not stop after cancellation")
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for bulk.bytes.Load() < 256*1024 {
		select {
		case err := <-bulk.done:
			t.Fatalf("bulk download stopped before sustained demand: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("bulk download made no sustained progress")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return bulk
}

func assertHarnessEpisode(t *testing.T, status harnessStatus, primary string) {
	t.Helper()
	found := false
	for _, member := range status.Members {
		if member.Tag == primary {
			found = true
		}
	}
	if !found {
		t.Fatalf("primary %q missing", primary)
	}
	for _, event := range status.Events {
		if event.Kind == "failover" && event.From != nil && *event.From == primary {
			t.Fatalf("throughput scenario used health failover: %+v", event)
		}
	}
	allowance := status.Budget.Degradation
	if allowance.Limit != 100 || allowance.Used < 0 || allowance.Reserved < 0 || allowance.Used+allowance.Reserved > allowance.Limit {
		t.Fatalf("degradation allowance violated: %+v", allowance)
	}
}

func harnessLedgerStarts(t *testing.T, client *harnessBox) int {
	t.Helper()
	var ledger struct {
		Starts []int64 `json:"starts"`
	}
	if err := json.Unmarshal(metadataHarness(t, client)["ledger"], &ledger); err != nil {
		t.Fatal(err)
	}
	return len(ledger.Starts)
}

func TestSmartHarnessDegradation(t *testing.T) {
	for _, scenario := range []string{"degraded_primary", "local_bottleneck", "two_slow_one_fast", "shared_link_correction", "burst_then_slow"} {
		t.Run(scenario, func(t *testing.T) {
			started := time.Now()
			beforeGoroutines := runtime.NumGoroutine()
			t.Cleanup(func() {
				deadline := time.Now().Add(5 * time.Second)
				for runtime.NumGoroutine() > beforeGoroutines && time.Now().Before(deadline) {
					time.Sleep(20 * time.Millisecond)
				}
				afterGoroutines := runtime.NumGoroutine()
				t.Logf("scenario teardown goroutines: before=%d after=%d", beforeGoroutines, afterGoroutines)
				if afterGoroutines > beforeGoroutines {
					stack := make([]byte, 256*1024)
					n := runtime.Stack(stack, true)
					t.Errorf("scenario leaked goroutines:\n%s", stack[:n])
				}
			})
			t.Cleanup(func() { t.Logf("slice-D scenario runtime: %s", time.Since(started)) })
			links := []harnessLink{{tag: "a", mbps: 20}, {tag: "b", mbps: 20, delay: 5 * time.Millisecond}}
			if scenario == "two_slow_one_fast" {
				// Keep C out of startup calibration so the fresh pair retains two hourly slots.
				links = append(links, harnessLink{tag: "c", mbps: 20, blackhole: true})
			}
			client, members, _ := newHarnessNetwork(t, links, 1000, "")
			requestHarness(t, client)
			awaitHarness(t, client, 75*time.Second, "two target-meeting startup calibrations", func(status harnessStatus) bool {
				count := 0
				for _, member := range status.Members {
					if member.Tag != "a" && member.Tag != "b" {
						continue
					}
					if member.Download == nil || *member.Download < 10 || member.Source != "active" {
						return false
					}
					count++
				}
				if count != 2 {
					return false
				}
				return status.Selected.TCP == "a" && status.Budget.Reserved == 0
			})
			if starts := harnessLedgerStarts(t, client); starts != 2 {
				t.Fatalf("startup used %d hourly slots, want two", starts)
			}
			beforeDownloads := make([]int64, len(members))
			for i, member := range members {
				beforeDownloads[i] = member.fixtures.downloads.Load()
			}
			primary := members[0].fixtures
			if scenario == "shared_link_correction" || scenario == "burst_then_slow" {
				primary.bulkRate.Store(187_500)
			}
			bulk := startHarnessBulk(t, client)
			startBytes := bulk.bytes.Load()
			degradedAt := time.Now()
			switchExpected := scenario == "degraded_primary" || scenario == "burst_then_slow"
			switch scenario {
			case "degraded_primary":
				faults := members[0].faults
				faults.bytesPerSecond = 187_500
				members[0].relay.set(faults)
			case "local_bottleneck":
				shared := &sharedDownstream{bytesPerSecond: 375_000}
				for _, member := range members {
					faults := member.faults
					faults.downstream = shared
					member.relay.set(faults)
				}
			case "two_slow_one_fast":
				faults := members[2].faults
				faults.blackhole = false
				members[2].relay.set(faults)
				awaitHarness(t, client, 90*time.Second, "untried C healthy before degrading A and B", func(status harnessStatus) bool {
					assertHarnessEpisode(t, status, "a")
					for _, member := range status.Members {
						if member.Tag == "c" {
							return member.State == "healthy"
						}
					}
					return false
				})
				if harnessLedgerStarts(t, client) != 2 {
					t.Fatal("C consumed an hourly slot despite sustained user demand")
				}
				degradedAt = time.Now()
				for i, member := range members {
					faults := member.faults
					faults.blackhole = false
					if i < 2 {
						faults.bytesPerSecond = 187_500
					}
					member.relay.set(faults)
				}
			case "shared_link_correction":
				shared := &sharedDownstream{bytesPerSecond: 2_500_000}
				for _, member := range members {
					faults := member.faults
					faults.downstream = shared
					member.relay.set(faults)
				}
				// Suspicion sees paced demand; confirmation gets a saturated same-primary flow.
				primary.downloadRate.Store(375_000)
				primary.unpaceBulkOnDownload.Store(true)
			case "burst_then_slow":
				primary.downloadBurst.Store(1_000_000 + 781_250)
				primary.downloadRate.Store(187_500)
			}
			calibrated := statusHarness(t, client)
			calibratedRates := make(map[string]float64)
			calibratedAt := make(map[string]int64)
			for _, member := range calibrated.Members {
				if member.Download != nil {
					calibratedRates[member.Tag] = *member.Download
					calibratedAt[member.Tag] = member.At
				}
			}
			want := "shared_bottleneck_suspected"
			if switchExpected {
				want = "switched"
			} else if scenario == "shared_link_correction" {
				want = "primary_fast"
			}
			var seenLow bool
			sampledAt, sampledBytes := time.Now(), bulk.bytes.Load()
			finished := awaitHarness(t, client, 120*time.Second, want+" within the unchanged 120-second degradation bound", func(status harnessStatus) bool {
				assertHarnessEpisode(t, status, "a")
				if elapsed := time.Since(sampledAt); elapsed >= time.Second {
					bytes := bulk.bytes.Load()
					observed := float64(0)
					if status.Degradation.Observed != nil {
						observed = *status.Degradation.Observed
					}
					t.Logf("demand sample: age=%s rate=%.3f Mbit/s bytes=%d state=%s observed=%.3f flows=%d", time.Since(degradedAt).Round(time.Millisecond), float64(bytes-sampledBytes)*8/elapsed.Seconds()/1e6, bytes, status.Degradation.State, observed, status.Degradation.Flows)
					sampledAt, sampledBytes = time.Now(), bytes
				}
				if status.Degradation.Provisional != nil && *status.Degradation.Provisional < 5 {
					seenLow = true
				}
				if scenario == "burst_then_slow" && status.Degradation.Blocked == "primary_fast" {
					t.Fatal("startup burst received a fast confirmation")
				}
				if !switchExpected && status.Selected.TCP != "a" {
					t.Fatalf("provisional low evidence switched to %s", status.Selected.TCP)
				}
				return status.Degradation.State == want && status.Budget.Reserved == 0
			})
			if elapsed := time.Since(degradedAt); elapsed > 120*time.Second {
				t.Fatalf("degradation response took %s, bound 120s", elapsed)
			}
			if switchExpected {
				if !seenLow || finished.Selected.TCP != "b" {
					t.Fatalf("switch lacks fresh low primary evidence: %+v", finished)
				}
				found := false
				for _, event := range finished.Events {
					if event.Kind == "switch" && event.From != nil && *event.From == "a" && event.To != nil && *event.To == "b" && event.Reason == "demand challenger meets target" {
						found = true
					}
				}
				if !found {
					t.Fatal("missing throughput-driven demand switch event")
				}
				for _, member := range finished.Members {
					if member.Tag == "b" && (member.State != "healthy" || member.Download == nil || *member.Download < 10 || member.At <= calibratedAt["b"]) {
						t.Fatalf("replacement lacks healthy, fresh target-meeting evidence: %+v", member)
					}
				}
				beforeB := members[1].fixtures.https.Load()
				requestHarness(t, client)
				if members[1].fixtures.https.Load() <= beforeB {
					t.Fatal("new connection did not reach replacement fixture")
				}
			} else {
				if finished.Degradation.Retry < time.Now().Add(29*time.Minute).Unix() || finished.Degradation.Blocked != want {
					t.Fatalf("missing purpose cooldown: %+v", finished.Degradation)
				}
				if scenario == "shared_link_correction" {
					corrected := false
					for _, member := range finished.Members {
						if member.Tag == "a" && member.Download != nil && *member.Download >= 10 {
							corrected = true
						}
					}
					if !corrected || primary.bulkRate.Load() != 0 {
						t.Fatal("same-interval passive traffic did not correct the sub-half-target active probe")
					}
				} else {
					for _, member := range finished.Members {
						if member.Tag == "c" {
							continue
						}
						if member.At != calibratedAt[member.Tag] || member.Download == nil || *member.Download != calibratedRates[member.Tag] {
							t.Fatal("provisional slow pair was published to ordinary throughput")
						}
					}
				}
				starts := harnessLedgerStarts(t, client)
				used := finished.Budget.Degradation.Used
				deadline := time.Now().Add(3 * time.Second)
				for time.Now().Before(deadline) {
					status := statusHarness(t, client)
					assertHarnessEpisode(t, status, "a")
					if status.Selected.TCP != "a" || status.Budget.Degradation.Used != used || status.Budget.Degradation.Reserved != 0 {
						t.Fatal("cooldown switched primary or spent more allowance")
					}
					time.Sleep(100 * time.Millisecond)
				}
				if harnessLedgerStarts(t, client) != starts {
					t.Fatal("cooldown admitted another active test")
				}
			}
			wantStarts := 4
			if scenario == "shared_link_correction" {
				wantStarts = 3
			}
			if starts := harnessLedgerStarts(t, client); starts != wantStarts || finished.Budget.Degradation.Used <= 0 {
				t.Fatalf("fresh episode accounting: starts=%d want=%d budget=%+v", starts, wantStarts, finished.Budget)
			}
			for i := range 2 {
				wantDownloads := beforeDownloads[i] + 1
				if scenario == "shared_link_correction" && i == 1 {
					wantDownloads--
				}
				if got := members[i].fixtures.downloads.Load(); got != wantDownloads {
					t.Fatalf("member %d download attempts=%d want=%d", i, got, wantDownloads)
				}
			}
			if bulk.bytes.Load() <= startBytes || members[1].fixtures.bulkBytes.Load() != 0 {
				t.Fatal("original download made no episode progress or moved to replacement")
			}
			select {
			case err := <-bulk.done:
				t.Fatalf("original download terminated before cancellation: %v", err)
			default:
			}
			if scenario == "two_slow_one_fast" {
				if members[1].fixtures.downloads.Load() <= 0 || members[2].fixtures.downloads.Load() != 0 {
					t.Fatal("fresh slow challenger B was not tested before untried C")
				}
				t.Log("status exposes the completed slow pair and retry time, not attempted identities or next challenger; full rotation is covered by TestSmartDegradationRotationAcrossCooldown")
			}
		})
	}
}
