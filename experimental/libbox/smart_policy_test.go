package libbox

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/daemon"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/group"
	"github.com/sagernet/sing/service/filemanager"
)

func TestSmartProfiles(t *testing.T) {
	content, err := SmartProfiles()
	if err != nil {
		t.Fatal(err)
	}
	var actual []map[string]any
	if err = json.Unmarshal([]byte(content), &actual); err != nil {
		t.Fatal(err)
	}
	profiles, err := group.SmartProfiles()
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(profiles)
	var expected []map[string]any
	_ = json.Unmarshal(encoded, &expected)
	if !reflect.DeepEqual(actual, expected) || len(actual) != 16 {
		t.Fatalf("libbox catalog differs from core: %s", content)
	}
	for _, profile := range actual {
		if len(profile) != 18 {
			t.Fatalf("incomplete catalog profile: %#v", profile)
		}
	}
	if err = CheckSmartPolicy(`{"preference":{"profile":"balance-speed-4k"}}`); err != nil {
		t.Fatal(err)
	}
	if err = CheckSmartPolicy(`{"preference":{"profile":"missing"}}`); err == nil || !strings.HasPrefix(err.Error(), "smart policy override: ") {
		t.Fatalf("invalid policy error = %v", err)
	}
}

func TestSmartPolicyServiceOverride(t *testing.T) {
	directory := t.TempDir()
	ctx := filemanager.WithDefault(baseContext(nil), directory, directory, os.Getuid(), os.Getgid())
	server := daemon.NewStartedService(daemon.ServiceOptions{Context: ctx, LogMaxLines: 10})
	t.Cleanup(server.Close)
	t.Cleanup(func() { _ = server.CloseService() })
	policy := "{ \"preference\": {\"profile\": \"balance-speed-4k\"} }"
	config := `{
		"log":{"disabled":true},
		"log":{"level":"debug"},
		"outbounds":[
			{"type":"direct","tag":"direct"},
			{"type":"smart","tag":"smart","outbounds":["direct"],"throughput":{"target_mbps":8,"wifi_daily_budget_mb":0,"cellular_daily_budget_mb":0,"other_daily_budget_mb":0},"probe":{"disable_direct":true}},
			{"type":"smart","tag":"smart-two","outbounds":["direct"],"throughput":{"target_mbps":8,"enabled":false,"wifi_daily_budget_mb":0,"cellular_daily_budget_mb":0,"other_daily_budget_mb":0},"probe":{"heartbeat_interval":"40s","standby_interval":"3m","background_interval":"9m","disable_direct":true},"interrupt_on_failover":false,"udp":{"quic_policy":"off"}}
		]
	}`
	if err := server.StartOrReloadService(ctx, config, &daemon.OverrideOptions{SmartPolicy: policy}); err != nil {
		t.Fatal(err)
	}
	response, err := server.GetSmartStatus(context.Background(), &daemon.GetSmartStatusRequest{Tag: "smart"})
	if err != nil {
		t.Fatal(err)
	}
	var status struct {
		Preference struct {
			Engine       string          `json:"engine"`
			Base         string          `json:"base"`
			Custom       bool            `json:"custom"`
			Source       string          `json:"source"`
			OverrideHash string          `json:"override_hash"`
			TargetMbps   float64         `json:"target_mbps"`
			Resolved     json.RawMessage `json:"resolved"`
		} `json:"preference"`
	}
	if err = json.Unmarshal([]byte(response.GetJson()), &status); err != nil {
		t.Fatal(err)
	}
	preference := status.Preference
	wantHash := fmt.Sprintf("%x", sha256.Sum256([]byte(policy)))
	if preference.Engine != "profile" || preference.Base != "balance-speed-4k" || preference.Custom || preference.Source != "override" || preference.OverrideHash != wantHash || preference.TargetMbps != 25 {
		t.Fatalf("applied override status = %s", response.GetJson())
	}
	if err = CheckSmartPolicy(string(preference.Resolved)); err != nil {
		t.Fatalf("resolved policy rejected: %v", err)
	}
	response, err = server.GetSmartStatus(ctx, &daemon.GetSmartStatusRequest{Tag: "smart-two"})
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal([]byte(response.GetJson()), &status); err != nil {
		t.Fatal(err)
	}
	if status.Preference.Base != "balance-speed-4k" || status.Preference.Custom || status.Preference.OverrideHash != wantHash || status.Preference.TargetMbps != 25 {
		t.Fatalf("second Smart outbound was not overridden: %s", response.GetJson())
	}
	if err = server.StartOrReloadService(ctx, config, &daemon.OverrideOptions{}); err != nil {
		t.Fatal(err)
	}
	response, err = server.GetSmartStatus(context.Background(), &daemon.GetSmartStatusRequest{Tag: "smart"})
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal([]byte(response.GetJson()), &status); err != nil {
		t.Fatal(err)
	}
	if status.Preference.Source != "config" || status.Preference.OverrideHash != "" || status.Preference.Engine != "legacy" || status.Preference.TargetMbps != 8 {
		t.Fatalf("override leaked across reload: %s", response.GetJson())
	}
	if err = server.StartOrReloadService(ctx, config, &daemon.OverrideOptions{SmartPolicy: `{"throughput":{"target_mbps":0}}`}); err == nil || !strings.HasPrefix(err.Error(), "smart policy override: ") {
		t.Fatalf("service accepted invalid policy: %v", err)
	}
}

func TestSmartPolicyServiceMixedCaseOverride(t *testing.T) {
	for _, test := range []struct {
		name, config string
	}{
		{"mixed case", `{
			"log":{"disabled":true},
			"Outbounds":[
				{"type":"direct","tag":"direct"},
				{"type":"smart","tag":"smart","outbounds":["direct"],
				 "Preference":{"profile":"data-saver","rank":["latency"]},
				 "Throughput":{"Target_Mbps":8,"Enabled":false,"other_daily_budget_mb":0,"wifi_daily_budget_mb":0,"cellular_daily_budget_mb":0},
				 "Probe":{"Heartbeat_Interval":"40s","Standby_Interval":"3m","Background_Interval":"9m","disable_direct":true},
				 "Interrupt_On_Failover":false,"UDP":{"Quic_Policy":"off","enabled":false}}
			]
		}`},
		{"case aliases", `{
			"log":{"disabled":true},
			"outbounds":[{"type":"direct","tag":"shadowed"}],
			"OUTBOUNDS":[
				{"type":"direct","tag":"direct"},
				{"type":"smart","tag":"smart","outbounds":["direct"],
				 "preference":{"profile":"phone"},"PREFERENCE":{"profile":"data-saver"},
				 "throughput":{"target_mbps":4},"THROUGHPUT":{"target_mbps":8,"Target_Mbps":8,"enabled":false,"ENABLED":false,"other_daily_budget_mb":0,"wifi_daily_budget_mb":0,"cellular_daily_budget_mb":0},
				 "probe":{"heartbeat_interval":"50s"},"PROBE":{"heartbeat_interval":"40s","HEARTBEAT_INTERVAL":"40s","standby_interval":"3m","STANDBY_INTERVAL":"3m","background_interval":"9m","BACKGROUND_INTERVAL":"9m","disable_direct":true},
				 "interrupt_on_failover":false,"INTERRUPT_ON_FAILOVER":false,
				 "udp":{"quic_policy":"route_only"},"UDP":{"quic_policy":"off","QUIC_POLICY":"off","enabled":false}}
			]
		}`},
		{"unicode folding", `{
			"log":{"disabled":true},
			"Outboundſ":[
				{"type":"direct","tag":"direct"},
				{"type":"smart","tag":"smart","outbounds":["direct"],
				 "preference":{"profile":"data-saver"},
				 "throughput":{"target_mbpſ":8,"enabled":false,"other_daily_budget_mb":0,"wifi_daily_budget_mb":0,"cellular_daily_budget_mb":0},
				 "probe":{"heartbeat_interval":"40s","ſtandby_interval":"3m","background_interval":"9m","disable_direct":true},
				 "interrupt_on_failover":false,"udp":{"quic_policy":"off","enabled":false}}
			]
		}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			ctx := filemanager.WithDefault(baseContext(nil), directory, directory, os.Getuid(), os.Getgid())
			server := daemon.NewStartedService(daemon.ServiceOptions{Context: ctx, LogMaxLines: 10})
			t.Cleanup(server.Close)
			t.Cleanup(func() { _ = server.CloseService() })
			policy := "{ \"preference\": {\"profile\": \"balance-speed-4k\"} }"
			if err := server.StartOrReloadService(ctx, test.config, &daemon.OverrideOptions{SmartPolicy: policy}); err != nil {
				t.Fatal(err)
			}
			response, err := server.GetSmartStatus(ctx, &daemon.GetSmartStatusRequest{Tag: "smart"})
			if err != nil {
				t.Fatal(err)
			}
			var status struct {
				Preference struct {
					Base, Source string
					Custom       bool
					OverrideHash string `json:"override_hash"`
					Resolved     map[string]any
				}
			}
			if err = json.Unmarshal([]byte(response.GetJson()), &status); err != nil {
				t.Fatal(err)
			}
			preference := status.Preference
			wantHash := fmt.Sprintf("%x", sha256.Sum256([]byte(policy)))
			if preference.Base != "balance-speed-4k" || preference.Custom || preference.Source != "override" || preference.OverrideHash != wantHash {
				t.Fatalf("override metadata = %s", response.GetJson())
			}
			resolved := preference.Resolved
			if !reflect.DeepEqual(resolved["throughput"], map[string]any{"target_mbps": float64(25), "enabled": true, "streams": float64(1)}) ||
				!reflect.DeepEqual(resolved["probe"], map[string]any{"heartbeat_interval": "15s", "standby_interval": "1m0s", "background_interval": "5m0s"}) ||
				resolved["interrupt_on_failover"] != true ||
				!reflect.DeepEqual(resolved["udp"], map[string]any{"quic_policy": "auto"}) {
				t.Fatalf("case variant survived override: %s", response.GetJson())
			}
		})
	}
}

func TestSmartPolicyPreservesNonPolicyAliasOrder(t *testing.T) {
	config := `{
		// The normal decoder merges these exact duplicate objects.
		"log":{"disabled":true},
		"log":{"level":"debug"},
		"outbounds":[
			{"type":"direct","tag":"one"},
			{"type":"direct","tag":"two"},
			{"type":"smart","tag":"smart","outbounds":["one"],"OUTBOUNDS":["two"],
			 "probe":{"timeout":"7s","TIMEOUT":"1s"},
			 "throughput":{"other_daily_budget_mb":7,"OTHER_DAILY_BUDGET_MB":1,
			               "download_url":"https://first.example/{bytes}","DOWNLOAD_URL":"https://last.example/{bytes}"},
			 "udp":{"enabled":true,"ENABLED":false}}
		]
	}`
	ctx := baseContext(nil)
	before, err := parseConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	after, err := parseConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := group.ParseSmartPolicy(`{"preference":{"profile":"balance-speed-4k"}}`)
	if err != nil {
		t.Fatal(err)
	}
	after.Outbounds[2].Options.(*option.SmartOutboundOptions).ApplySmartPolicy(policy)
	if !reflect.DeepEqual(before.Log, after.Log) || after.Log == nil || !after.Log.Disabled || after.Log.Level != "debug" {
		t.Fatalf("duplicate log objects no longer merge: before=%#v after=%#v", before.Log, after.Log)
	}
	if !reflect.DeepEqual(before.Outbounds[:2], after.Outbounds[:2]) {
		t.Fatal("non-smart outbounds changed")
	}
	for _, parsed := range []option.Options{before, after} {
		smart := parsed.Outbounds[2].Options.(*option.SmartOutboundOptions)
		if smart.Probe.Timeout.Build().String() != "1s" ||
			smart.Throughput.OtherDailyBudgetMB == nil || *smart.Throughput.OtherDailyBudgetMB != 1 ||
			smart.Throughput.DownloadURL != "https://last.example/{bytes}" ||
			smart.UDP.Enabled == nil || *smart.UDP.Enabled ||
			!reflect.DeepEqual([]string(smart.Outbounds), []string{"two"}) {
			t.Fatalf("non-policy alias precedence changed: %#v", smart)
		}
	}
}
