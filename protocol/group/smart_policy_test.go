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
	policy, err := ParseSmartPolicy(`{"preference":{"profile":"balance-speed-4k"},"probe":{"standby_interval":"45s"}}`)
	if err != nil {
		t.Fatal(err)
	}
	for i, input := range []string{
		`{"outbounds":["direct"],"preference":{"profile":"phone","rank":["latency"]},"throughput":{"target_mbps":8,"enabled":false,"wifi_daily_budget_mb":123},"probe":{"heartbeat_interval":"40s","standby_interval":"3m","background_interval":"9m","timeout":"7s","disable_direct":true},"interrupt_on_failover":false,"udp":{"quic_policy":"off","enabled":false},"notify_all_dead":false}`,
		`{"throughput":{"target_mbps":0},"probe":{"heartbeat_interval":"0s","standby_interval":"0s","background_interval":"0s"},"udp":{"quic_policy":""}}`,
	} {
		var options option.SmartOutboundOptions
		if err = json.Unmarshal([]byte(input), &options); err != nil {
			t.Fatal(err)
		}
		options.ApplySmartPolicy(policy)
		s := &Smart{options: options}
		if err = s.configure(); err != nil {
			t.Fatal(err)
		}
		if s.targetMbps != 25 || !s.throughputEnabled || !s.interrupt || s.quicPolicy != "auto" || s.heartbeat.String() != "15s" || s.standbyInterval.String() != "45s" || s.backgroundInterval.String() != "5m0s" {
			t.Fatalf("profile-owned values survived replacement: %#v", s.preferenceStatusLocked())
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
			if _, err := ParseSmartPolicy(input); err == nil || !strings.HasPrefix(err.Error(), "smart policy override: ") {
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

func TestSmartPolicyStrictJSON(t *testing.T) {
	for _, test := range []struct {
		name, input, path string
	}{
		{"line comment", "{\n// comment\n\"preference\":{\"profile\":\"phone\"}}", "policy"},
		{"block comment", `{"preference":/* comment */{"profile":"phone"}}`, "preference"},
		{"trailing comma", `{"preference":{"profile":"phone",}}`, "preference"},
		{"top level case", `{"Preference":{"profile":"phone"}}`, "Preference"},
		{"nested case", `{"preference":{"Profile":"phone"}}`, "preference.Profile"},
		{"switch case", `{"preference":{"switch":{"Dwell":"1m"}}}`, "preference.switch.Dwell"},
		{"top level duplicate", `{"preference":{},"preference":{"profile":"phone"}}`, "preference"},
		{"nested duplicate", `{"preference":{"switch":{"dwell":"1m","dwell":"2m"}}}`, "preference.switch.dwell"},
		{"escaped duplicate", `{"preference":{"profile":"phone","\u0070rofile":"server"}}`, "preference.profile"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := CheckSmartPolicy(test.input)
			if err == nil || !strings.HasPrefix(err.Error(), "smart policy override: ") || !strings.Contains(err.Error(), test.path) {
				t.Fatalf("strict JSON error does not name %s: %v", test.path, err)
			}
			_, appliedErr := ParseSmartPolicy(test.input)
			if appliedErr == nil || appliedErr.Error() != err.Error() {
				t.Fatalf("application validation differs: check=%v apply=%v", err, appliedErr)
			}
		})
	}
}
