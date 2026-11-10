package option

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	singjson "github.com/sagernet/sing/common/json"
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

func TestSmartExistingFieldsDecodeBeforePreferenceResolution(t *testing.T) {
	for _, decode := range []struct {
		name string
		run  func([]byte, any) error
	}{
		{"standard", json.Unmarshal},
		{"strict", singjson.UnmarshalDisallowUnknownFields},
		{"context", func(content []byte, value any) error {
			return singjson.UnmarshalContextDisallowUnknownFields(context.Background(), content, value)
		}},
	} {
		for _, preference := range []string{"", `,"preference":{}`, `,"preference":{"profile":"default"}`, `,"preference":{"profile":"speed-max"}`, `,"preference":{"switch":{"improvement":false}}`} {
			document := `{"probe":{"heartbeat_interval":"0s","standby_interval":"0s","background_interval":"0s","timeout":"0s"},"throughput":{"target_mbps":0},"udp":{"quic_policy":""}` + preference + `}`
			t.Run(decode.name+"/"+document, func(t *testing.T) {
				var options SmartOutboundOptions
				if err := decode.run([]byte(document), &options); err != nil {
					t.Fatal(err)
				}
				if options.Probe.HeartbeatInterval != 0 || options.Probe.StandbyInterval != 0 || options.Probe.BackgroundInterval != 0 || options.Probe.Timeout != 0 || options.Throughput.TargetMbps != 0 || options.UDP.QUICPolicy != "" {
					t.Fatalf("decoding changed legacy zero values: %#v", options)
				}
				encoded, err := json.Marshal(options)
				if err != nil {
					t.Fatal(err)
				}
				for _, metadata := range []string{"heartbeatIntervalSet", "standbyIntervalSet", "backgroundIntervalSet", "timeoutSet", "targetMbpsSet", "streamsSet", "windowSet", "warmupSet", "uploadWindowSet", "uploadWarmupSet", "quicPolicySet"} {
					if strings.Contains(string(encoded), metadata) {
						t.Fatalf("presence metadata was serialized: %s", encoded)
					}
				}
			})
		}
	}
}

func TestSmartExistingFieldsRejectUnknownFields(t *testing.T) {
	for _, document := range []string{
		`{"probe":{"unknown_interval":"1s"}}`,
		`{"throughput":{"unknown_target":1}}`,
		`{"udp":{"unknown_policy":"auto"}}`,
	} {
		t.Run(document, func(t *testing.T) {
			var options SmartOutboundOptions
			if err := json.Unmarshal([]byte(document), &options); err == nil {
				t.Fatal("unknown field was accepted")
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
	if roundTrip.Probe.HeartbeatInterval != 1 || roundTrip.Probe.StandbyInterval != 2 || roundTrip.Probe.BackgroundInterval != 3 || roundTrip.Throughput.TargetMbps != 4 || roundTrip.UDP.QUICPolicy != "off" {
		t.Fatalf("existing plain fields changed: %#v", roundTrip)
	}
}

func TestSmartMeasurementFieldPresence(t *testing.T) {
	for _, decode := range []struct {
		name string
		run  func([]byte, any) error
	}{
		{"standard", json.Unmarshal},
		{"strict", singjson.UnmarshalDisallowUnknownFields},
		{"context", func(content []byte, value any) error {
			return singjson.UnmarshalContextDisallowUnknownFields(context.Background(), content, value)
		}},
	} {
		t.Run(decode.name, func(t *testing.T) {
			var options SmartOutboundOptions
			if err := decode.run([]byte(`{"throughput":{"streams":0,"window":"0s","warmup":"0s","upload":false,"upload_window":"0s","upload_warmup":"0s"}}`), &options); err != nil {
				t.Fatal(err)
			}
			throughput := options.Throughput
			if !throughput.streamsSet || !throughput.windowSet || !throughput.warmupSet || !throughput.uploadWindowSet || !throughput.uploadWarmupSet || throughput.Upload == nil || *throughput.Upload {
				t.Fatalf("explicit zero or false lost: %#v", throughput)
			}
			if !options.HasProfileOwnedFields() {
				t.Fatal("explicit streams did not mark the policy customized")
			}
			if err := throughput.ValidateMeasurementFields(false); err != nil {
				t.Fatalf("legacy zero defaults rejected: %v", err)
			}
			if err := throughput.ValidateMeasurementFields(true); err == nil {
				t.Fatal("profile accepted explicit zero")
			}
			if err := decode.run([]byte(`{"throughput":{"streams":2,"window":"5s","warmup":"1s","upload":false,"upload_window":"5s","upload_warmup":"1s"}}`), &options); err != nil {
				t.Fatal(err)
			}
			if err := options.ValidateProfileFields(); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(options)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(encoded), `"upload":false`) {
				t.Fatalf("upload false lost: %s", encoded)
			}
			var roundTrip SmartOutboundOptions
			if err := decode.run(encoded, &roundTrip); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(options, roundTrip) {
				t.Fatalf("measurement round trip changed options: %#v != %#v", options, roundTrip)
			}
			options = SmartOutboundOptions{}
			if err := decode.run([]byte(`{}`), &options); err != nil {
				t.Fatal(err)
			}
			if options.HasProfileOwnedFields() || !reflect.DeepEqual(options.Throughput, SmartThroughputOptions{}) {
				t.Fatalf("absent fields gained presence: %#v", options.Throughput)
			}
		})
	}
}

func TestSmartMeasurementFieldBounds(t *testing.T) {
	for _, field := range []string{
		`"streams":-1`, `"window":"-1s"`, `"window":"4.999s"`, `"warmup":"999ms"`,
		`"upload_window":"4.999s"`, `"upload_warmup":"999ms"`,
	} {
		t.Run(field, func(t *testing.T) {
			var throughput SmartThroughputOptions
			if err := json.Unmarshal([]byte(`{`+field+`}`), &throughput); err != nil {
				t.Fatal(err)
			}
			for _, profile := range []bool{false, true} {
				if err := throughput.ValidateMeasurementFields(profile); err == nil {
					t.Fatalf("accepted %s with profile=%v", field, profile)
				}
			}
		})
	}
	for _, field := range []string{`"streams":0`, `"window":"0s"`, `"warmup":"0s"`, `"upload_window":"0s"`, `"upload_warmup":"0s"`} {
		var throughput SmartThroughputOptions
		if err := json.Unmarshal([]byte(`{`+field+`}`), &throughput); err != nil {
			t.Fatal(err)
		}
		if err := throughput.ValidateMeasurementFields(false); err != nil {
			t.Fatalf("legacy zero rejected: %s: %v", field, err)
		}
		if err := throughput.ValidateMeasurementFields(true); err == nil {
			t.Fatalf("explicit zero accepted: %s", field)
		}
	}
}

func TestSmartPolicyReplacesStreamsOnly(t *testing.T) {
	var options, policy SmartOutboundOptions
	if err := json.Unmarshal([]byte(`{"throughput":{"streams":0,"window":"5s","warmup":"1s","upload":false,"upload_window":"6s","upload_warmup":"2s"}}`), &options); err != nil {
		t.Fatal(err)
	}
	before := options.Throughput
	options.ApplySmartPolicy(&policy)
	if options.Throughput.Streams != 0 || options.Throughput.streamsSet || options.HasProfileOwnedFields() {
		t.Fatal("omitted policy streams did not reset value and presence")
	}
	before.Streams, before.streamsSet = 0, false
	if !reflect.DeepEqual(options.Throughput, before) {
		t.Fatal("policy replacement changed non-policy measurement settings")
	}
	if err := json.Unmarshal([]byte(`{"throughput":{"streams":3}}`), &policy); err != nil {
		t.Fatal(err)
	}
	options.ApplySmartPolicy(&policy)
	if options.Throughput.Streams != 3 || !options.Throughput.streamsSet || !options.HasProfileOwnedFields() {
		t.Fatal("policy streams value or presence lost")
	}
}
