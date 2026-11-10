package group

import (
	"crypto/sha256"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
)

func TestSmartPolicyOverride(t *testing.T) {
	config := `{
		// Subscription values must not shadow the selected policy.
		"outbounds": [
			{"type":"smart","tag":"one","outbounds":["direct"],"preference":{"profile":"phone","rank":["latency"]},"throughput":{"target_mbps":8,"enabled":false,"wifi_daily_budget_mb":123},"probe":{"heartbeat_interval":"40s","standby_interval":"3m","background_interval":"9m","timeout":"7s","disable_direct":true},"interrupt_on_failover":false,"udp":{"quic_policy":"off","enabled":false},"notify_all_dead":false},
			{"type":"smart","tag":"two","throughput":{"target_mbps":8}},
			{"type":"direct","tag":"direct","domain_strategy":"prefer_ipv4"}
		]
	}`
	override := `{"preference":{"profile":"balance-speed-4k"},"probe":{"standby_interval":"45s"}}`
	merged, err := ApplySmartPolicyOverride(config, override)
	if err != nil {
		t.Fatal(err)
	}
	var before, after map[string]json.RawMessage
	if err = json.Unmarshal([]byte(config), &before); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal([]byte(merged), &after); err != nil {
		t.Fatal(err)
	}
	var original, outbounds []map[string]json.RawMessage
	_ = json.Unmarshal(before["outbounds"], &original)
	_ = json.Unmarshal(after["outbounds"], &outbounds)
	if !reflect.DeepEqual(original[2], outbounds[2]) {
		t.Fatal("non-smart outbound changed")
	}
	for i := range 2 {
		delete(outbounds[i], "type")
		delete(outbounds[i], "tag")
		content, _ := json.Marshal(outbounds[i])
		var options option.SmartOutboundOptions
		if err = json.Unmarshal(content, &options); err != nil {
			t.Fatal(err)
		}
		s := &Smart{options: options}
		if err = s.configure(); err != nil {
			t.Fatal(err)
		}
		if s.targetMbps != 25 || !s.throughputEnabled || !s.interrupt || s.quicPolicy != "auto" || s.heartbeat.String() != "15s" || s.standbyInterval.String() != "45s" || s.backgroundInterval.String() != "5m0s" {
			t.Fatalf("profile-owned values survived removal: %#v", s.preferenceStatusLocked())
		}
		if i == 0 && (s.options.Throughput.WIFIDailyBudgetMB == nil || *s.options.Throughput.WIFIDailyBudgetMB != 123 || s.options.Probe.Timeout.Build().String() != "7s" || !s.options.Probe.DisableDirect || s.udpEnabled || s.notify) {
			t.Fatal("non-policy fields changed")
		}
	}
}

func TestCheckSmartPolicyInvalid(t *testing.T) {
	for _, input := range []string{
		``, `null`, `[]`, `{`, `{} {}`, `{"tag":"bad"}`, `{"SmartPolicyOverrideHash":"spoof"}`,
		`{"throughput":{"wifi_daily_budget_mb":1}}`, `{"probe":{"timeout":"1s"}}`, `{"udp":{"enabled":true}}`,
		`{"preference":{"unknown":1}}`, `{"preference":{"switch":{"unknown":1}}}`,
		`{"preference":{"profile":"missing"}}`, `{"preference":{"rank":[]}}`, `{"preference":{"rank":["latency","latency"]}}`,
		`{"preference":{"min_stability":"E"}}`, `{"preference":{"speed_floor_hysteresis":0.6}}`,
		`{"preference":{"switch":{"dwell":"0s"}}}`, `{"preference":{"switch":{"confirmations":0}}}`, `{"preference":{"switch":{"speed_margin":0.5}}}`,
		`{"throughput":{"target_mbps":0}}`, `{"probe":{"heartbeat_interval":"0s"}}`, `{"udp":{"quic_policy":""}}`,
		`{"preference":null}`, `{"preference":{"profile":null}}`, `{"preference":{"rank":[null]}}`, `{"interrupt_on_failover":null}`,
	} {
		t.Run(input, func(t *testing.T) {
			if err := CheckSmartPolicy(input); err == nil || !strings.HasPrefix(err.Error(), "smart policy override: ") {
				t.Fatalf("invalid policy accepted or missing error prefix: %v", err)
			}
			if _, err := ApplySmartPolicyOverride(`{"outbounds":[]}`, input); err == nil || !strings.HasPrefix(err.Error(), "smart policy override: ") {
				t.Fatalf("start validation differs: %v", err)
			}
		})
	}
}

func TestSmartProfilesCatalog(t *testing.T) {
	profiles, err := SmartProfiles()
	if err != nil {
		t.Fatal(err)
	}
	wantNames := strings.Fields("default stability-downtime stability-connections balance-speed-720p balance-speed-1080p balance-speed-1440p balance-speed-4k speed-max responsive realtime-calls realtime-gaming data-saver phone desktop server")
	if len(profiles) != len(wantNames) {
		t.Fatalf("profile count = %d", len(profiles))
	}
	for i, name := range wantNames {
		if profiles[i]["name"] != name {
			t.Fatalf("profile %d = %v, want %s", i, profiles[i]["name"], name)
		}
		s := &Smart{options: option.SmartOutboundOptions{Preference: &option.SmartPreferenceOptions{Profile: &name}}}
		if err = s.configure(); err != nil {
			t.Fatal(err)
		}
		resolved := s.resolvedSmartPolicy()
		content, _ := json.Marshal(resolved)
		if err = CheckSmartPolicy(string(content)); err != nil {
			t.Fatalf("%s resolved round trip: %v", name, err)
		}
		if profiles[i]["target_mbps"] != s.targetMbps || profiles[i]["heartbeat_interval"] != s.heartbeat.String() || !reflect.DeepEqual(profiles[i]["rank"], s.preference.rank) {
			t.Fatalf("catalog differs from effective %s: %#v", name, profiles[i])
		}
	}
}

func TestSmartPolicyPreferenceStatus(t *testing.T) {
	for _, test := range []struct {
		input, engine, base string
		custom              bool
	}{
		{`{}`, "legacy", "default", false},
		{`{"throughput":{"target_mbps":8}}`, "legacy", "default", false},
		{`{"preference":{"profile":"default"},"interrupt_on_failover":false}`, "legacy", "default", false},
		{`{"preference":{"profile":"phone"}}`, "profile", "phone", false},
		{`{"preference":{"profile":"phone","switch":{}}}`, "profile", "phone", false},
		{`{"preference":{"profile":"phone","min_stability":"C"}}`, "profile", "phone", true},
		{`{"preference":{"profile":"phone"},"throughput":{"enabled":true}}`, "profile", "phone", true},
		{`{"preference":{"profile":"phone"},"probe":{"heartbeat_interval":"15s"}}`, "profile", "phone", true},
		{`{"preference":{"profile":"phone"},"udp":{"quic_policy":"auto"}}`, "profile", "phone", true},
		{`{"preference":{"profile":"default","switch":{"improvement":true}}}`, "profile", "default", true},
		{`{"preference":{"rank":["stability","throughput","latency"]}}`, "profile", "default", true},
	} {
		t.Run(test.input, func(t *testing.T) {
			var options option.SmartOutboundOptions
			if err := json.Unmarshal([]byte(test.input), &options); err != nil {
				t.Fatal(err)
			}
			s := &Smart{options: options}
			if err := s.configure(); err != nil {
				t.Fatal(err)
			}
			status := s.preferenceStatusLocked()
			if status["engine"] != test.engine || status["base"] != test.base || status["custom"] != test.custom || status["source"] != "config" || status["override_hash"] != "" {
				t.Fatalf("status = %#v", status)
			}
			content, _ := json.Marshal(status["resolved"])
			if err := CheckSmartPolicy(string(content)); err != nil {
				t.Fatalf("resolved does not round trip: %v", err)
			}
			hash := fmt.Sprintf("%x", sha256.Sum256([]byte(test.input)))
			s.options.SmartPolicyOverrideHash = hash
			status = s.preferenceStatusLocked()
			if status["source"] != "override" || status["override_hash"] != hash {
				t.Fatalf("override status = %#v", status)
			}
		})
	}
}
