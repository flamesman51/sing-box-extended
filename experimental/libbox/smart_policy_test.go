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
	if !reflect.DeepEqual(actual, expected) || len(actual) != 15 {
		t.Fatalf("libbox catalog differs from core: %s", content)
	}
	for _, profile := range actual {
		if len(profile) != 17 {
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
		"outbounds":[
			{"type":"direct","tag":"direct"},
			{"type":"smart","tag":"smart","outbounds":["direct"],"throughput":{"target_mbps":8,"wifi_daily_budget_mb":0,"cellular_daily_budget_mb":0,"other_daily_budget_mb":0},"probe":{"disable_direct":true}}
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
