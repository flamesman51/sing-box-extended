package option

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestSmartPreferencePresenceRoundTrip(t *testing.T) {
	const document = `{"preference":{"profile":"balance-speed-1080p","rank":["latency"],"min_stability":"none","speed_floor_hysteresis":0,"switch":{"improvement":false,"dwell":"10m","confirmations":1,"confirmation_interval":"30s","tier_restore":"0s","speed_margin":1}},"throughput":{"enabled":false,"target_mbps":7,"wifi_daily_budget_mb":0},"interrupt_on_failover":false}`
	var options SmartOutboundOptions
	if err := json.Unmarshal([]byte(document), &options); err != nil {
		t.Fatal(err)
	}
	preference := options.Preference
	if preference == nil || preference.Profile == nil || preference.Rank == nil || preference.MinStability == nil || preference.SpeedFloorHysteresis == nil || preference.Switch == nil {
		t.Fatal("present preference leaves were lost")
	}
	switches := preference.Switch
	if switches.Improvement == nil || *switches.Improvement || switches.TierRestore == nil || *switches.TierRestore != 0 || *preference.SpeedFloorHysteresis != 0 || *preference.MinStability != "none" {
		t.Fatal("explicit false, zero, zero duration or none was lost")
	}
	encoded, err := json.Marshal(options)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{`"improvement":false`, `"speed_floor_hysteresis":0`, `"tier_restore":"0s"`, `"min_stability":"none"`, `"enabled":false`, `"wifi_daily_budget_mb":0`, `"interrupt_on_failover":false`} {
		if !strings.Contains(string(encoded), expected) {
			t.Fatalf("missing %s in %s", expected, encoded)
		}
	}
	var roundTrip SmartOutboundOptions
	if err := json.Unmarshal(encoded, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(options, roundTrip) {
		t.Fatalf("round trip changed options: %#v != %#v", options, roundTrip)
	}
}

func TestSmartPreferenceAbsentLeavesStayAbsent(t *testing.T) {
	for _, document := range []string{`{}`, `{"preference":{}}`, `{"preference":{"switch":{}}}`} {
		t.Run(document, func(t *testing.T) {
			var options SmartOutboundOptions
			if err := json.Unmarshal([]byte(document), &options); err != nil {
				t.Fatal(err)
			}
			if preference := options.Preference; preference != nil {
				if preference.Profile != nil || preference.Rank != nil || preference.MinStability != nil || preference.SpeedFloorHysteresis != nil {
					t.Fatal("absent preference leaves gained values")
				}
				if switches := preference.Switch; switches != nil && (switches.Improvement != nil || switches.Dwell != nil || switches.Confirmations != nil || switches.ConfirmationInterval != nil || switches.TierRestore != nil || switches.SpeedMargin != nil) {
					t.Fatal("absent switch leaves gained values")
				}
			}
			encoded, err := json.Marshal(options)
			if err != nil {
				t.Fatal(err)
			}
			for _, field := range []string{"profile", "rank", "min_stability", "speed_floor_hysteresis", "improvement", "dwell", "confirmations", "confirmation_interval", "tier_restore", "speed_margin"} {
				if strings.Contains(string(encoded), `"`+field+`"`) {
					t.Fatalf("absent %s was serialized: %s", field, encoded)
				}
			}
		})
	}
}

func TestSmartPreferenceZeroAndEmptyLeavesRemainPresent(t *testing.T) {
	var options SmartOutboundOptions
	if err := json.Unmarshal([]byte(`{"preference":{"profile":"","rank":[],"min_stability":"","speed_floor_hysteresis":0,"switch":{"improvement":false,"dwell":"0s","confirmations":0,"confirmation_interval":"0s","tier_restore":"0s","speed_margin":0}}}`), &options); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(options)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{`"profile":""`, `"rank":[]`, `"min_stability":""`, `"dwell":"0s"`, `"confirmations":0`, `"confirmation_interval":"0s"`, `"speed_margin":0`} {
		if !strings.Contains(string(encoded), expected) {
			t.Fatalf("explicit leaf %s disappeared: %s", expected, encoded)
		}
	}
}

func TestSmartExistingFieldsRejectExplicitInvalidDefaults(t *testing.T) {
	for _, document := range []string{
		`{"probe":{"heartbeat_interval":"0s"}}`,
		`{"probe":{"standby_interval":"0s"}}`,
		`{"probe":{"background_interval":"0s"}}`,
		`{"probe":{"timeout":"0s"}}`,
		`{"probe":{"heartbeat_interval":"-1s"}}`,
		`{"throughput":{"target_mbps":0}}`,
		`{"throughput":{"target_mbps":-1}}`,
		`{"udp":{"quic_policy":""}}`,
		`{"udp":{"quic_policy":"invalid"}}`,
		`{"probe":{"unknown_interval":"1s"}}`,
		`{"throughput":{"unknown_target":1}}`,
		`{"udp":{"unknown_policy":"auto"}}`,
	} {
		t.Run(document, func(t *testing.T) {
			var options SmartOutboundOptions
			if err := json.Unmarshal([]byte(document), &options); err == nil {
				t.Fatal("explicit invalid value was treated as an omitted default")
			}
		})
	}
}

func TestSmartExistingPlainFieldsRemainSourceCompatible(t *testing.T) {
	options := SmartOutboundOptions{
		Probe:      SmartProbeOptions{HeartbeatInterval: 1, StandbyInterval: 2, BackgroundInterval: 3},
		Throughput: SmartThroughputOptions{TargetMbps: 4},
		UDP:        SmartUDPOptions{QUICPolicy: "off"},
	}
	encoded, err := json.Marshal(options)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip SmartOutboundOptions
	if err := json.Unmarshal(encoded, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(options, roundTrip) {
		t.Fatalf("existing plain fields changed: %#v != %#v", options, roundTrip)
	}
}
