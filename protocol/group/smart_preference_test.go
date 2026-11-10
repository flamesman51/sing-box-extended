package group

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/option"
)

func TestSmartPreferenceProfileTable(t *testing.T) {
	for _, test := range []struct {
		profile       string
		rank          string
		minimum       int
		target        float64
		heartbeat     time.Duration
		standby       time.Duration
		background    time.Duration
		interrupt     bool
		throughput    bool
		dwell         time.Duration
		confirmations int
	}{
		{"stability-downtime", "stability outages latency", -1, 4, 10 * time.Second, 30 * time.Second, 5 * time.Minute, true, true, 10 * time.Minute, 2},
		{"stability-connections", "stability outages latency", -1, 4, 15 * time.Second, time.Minute, 5 * time.Minute, false, true, 30 * time.Minute, 3},
		{"balance-speed-720p", "speed_floor stability download latency", 2, 4, 15 * time.Second, time.Minute, 5 * time.Minute, true, true, 10 * time.Minute, 2},
		{"balance-speed-1080p", "speed_floor stability download latency", 2, 8, 15 * time.Second, time.Minute, 5 * time.Minute, true, true, 10 * time.Minute, 2},
		{"balance-speed-1440p", "speed_floor stability download latency", 2, 15, 15 * time.Second, time.Minute, 5 * time.Minute, true, true, 10 * time.Minute, 2},
		{"balance-speed-4k", "speed_floor stability download latency", 2, 25, 15 * time.Second, time.Minute, 5 * time.Minute, true, true, 10 * time.Minute, 2},
		{"speed-max", "download stability latency", 2, 25, 15 * time.Second, time.Minute, 5 * time.Minute, true, true, 5 * time.Minute, 2},
		{"responsive", "latency stability download", 2, 4, 15 * time.Second, time.Minute, 5 * time.Minute, true, true, 10 * time.Minute, 2},
		{"realtime-calls", "udp stability rtt_spread latency speed_floor", 2, 4, 10 * time.Second, 30 * time.Second, 5 * time.Minute, false, true, 10 * time.Minute, 2},
		{"realtime-gaming", "udp latency rtt_spread stability", 2, 2, 10 * time.Second, 30 * time.Second, 5 * time.Minute, false, true, 10 * time.Minute, 2},
		{"data-saver", "stability latency", -1, 4, 30 * time.Second, 2 * time.Minute, 15 * time.Minute, true, false, 10 * time.Minute, 2},
		{"phone", "speed_floor stability download latency", 2, 8, 15 * time.Second, time.Minute, 5 * time.Minute, true, true, 10 * time.Minute, 2},
		{"desktop", "speed_floor stability download latency", 2, 25, 15 * time.Second, time.Minute, 5 * time.Minute, true, true, 10 * time.Minute, 2},
		{"server", "speed_floor stability download latency", 2, 25, 10 * time.Second, 30 * time.Second, 5 * time.Minute, true, true, 10 * time.Minute, 2},
	} {
		t.Run(test.profile, func(t *testing.T) {
			options := option.SmartOutboundOptions{Preference: &option.SmartPreferenceOptions{Profile: &test.profile}}
			actual, err := resolveSmartPreference(&options)
			if err != nil {
				t.Fatal(err)
			}
			expected := smartPreference{
				profile: test.profile, rank: strings.Fields(test.rank), minStability: test.minimum,
				improvement: true, dwell: test.dwell, confirmations: test.confirmations,
				confirmationInterval: 30 * time.Second, tierRestore: time.Minute, speedMargin: 1.25, hysteresis: 0.2,
			}
			if !reflect.DeepEqual(actual, expected) {
				t.Fatalf("resolved policy = %#v, want %#v", actual, expected)
			}
			if options.Throughput.TargetMbps != test.target || options.Throughput.Enabled == nil || *options.Throughput.Enabled != test.throughput {
				t.Fatalf("throughput defaults = %#v", options.Throughput)
			}
			if time.Duration(options.Probe.HeartbeatInterval) != test.heartbeat || time.Duration(options.Probe.StandbyInterval) != test.standby || time.Duration(options.Probe.BackgroundInterval) != test.background {
				t.Fatalf("probe defaults = %#v", options.Probe)
			}
			if options.InterruptOnFailover == nil || *options.InterruptOnFailover != test.interrupt || options.UDP.QUICPolicy != "auto" {
				t.Fatalf("interrupt or QUIC defaults = %#v", options)
			}
			if options.Throughput.WIFIDailyBudgetMB != nil || options.Throughput.CellularDailyBudgetMB != nil || options.Throughput.OtherDailyBudgetMB != nil {
				t.Fatal("profile supplied budget defaults")
			}
			if options.UDP.Enabled != nil || options.NotifyAllDead != nil || options.Probe.Timeout != 0 {
				t.Fatal("profile changed fields it does not own")
			}
		})
	}
}

func TestSmartPreferenceLegacyResolution(t *testing.T) {
	for _, document := range []string{
		`{}`,
		`{"throughput":{"target_mbps":8}}`,
		`{"preference":{}}`,
		`{"preference":{"profile":"default"}}`,
		`{"preference":{"switch":{}}}`,
		`{"preference":{"profile":"default","switch":{}}}`,
		`{"preference":{"profile":"default"},"probe":{"heartbeat_interval":"7s","standby_interval":"11s","background_interval":"13s"},"throughput":{"target_mbps":7,"enabled":false},"interrupt_on_failover":false,"udp":{"quic_policy":"off"}}`,
	} {
		t.Run(document, func(t *testing.T) {
			var options option.SmartOutboundOptions
			if err := json.Unmarshal([]byte(document), &options); err != nil {
				t.Fatal(err)
			}
			before, err := json.Marshal(options)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := resolveSmartPreference(&options)
			if err != nil {
				t.Fatal(err)
			}
			expected := smartPreference{
				legacy: true, profile: "default", rank: []string{"stability", "throughput", "latency"},
				minStability: -1, improvement: true, dwell: 10 * time.Minute, confirmations: 2,
				confirmationInterval: 30 * time.Second, tierRestore: time.Minute, speedMargin: 1.25, hysteresis: 0.2,
			}
			if !reflect.DeepEqual(actual, expected) {
				t.Fatalf("legacy policy changed: %#v", actual)
			}
			after, err := json.Marshal(options)
			if err != nil {
				t.Fatal(err)
			}
			if string(before) != string(after) {
				t.Fatalf("legacy options changed: %s != %s", before, after)
			}
		})
	}
}

func TestSmartPreferenceLegacyZeroDefaults(t *testing.T) {
	for _, preference := range []string{
		"",
		`,"preference":{}`,
		`,"preference":{"profile":"default"}`,
		`,"preference":{"switch":{}}`,
		`,"preference":{"profile":"default","switch":{}}`,
	} {
		for _, fields := range []string{
			`"probe":{"heartbeat_interval":"0s"}`,
			`"probe":{"standby_interval":"0s"}`,
			`"probe":{"background_interval":"0s"}`,
			`"probe":{"timeout":"0s"}`,
			`"throughput":{"target_mbps":0}`,
			`"udp":{"quic_policy":""}`,
			`"probe":{"heartbeat_interval":"0s","standby_interval":"0s","background_interval":"0s","timeout":"0s"},"throughput":{"target_mbps":0},"udp":{"quic_policy":""}`,
		} {
			document := `{` + fields + preference + `}`
			t.Run(document, func(t *testing.T) {
				s, _ := smartTestGroup()
				if err := json.Unmarshal([]byte(document), &s.options); err != nil {
					t.Fatal(err)
				}
				before := s.options
				preference, err := resolveSmartPreference(&s.options)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(before, s.options) {
					t.Fatalf("legacy options changed: %#v != %#v", before, s.options)
				}
				if err := s.configure(); err != nil {
					t.Fatal(err)
				}
				if !preference.legacy || !s.preference.legacy {
					t.Fatal("legacy zero values selected the profile engine")
				}
				if s.heartbeat != 15*time.Second || s.standbyInterval != time.Minute || s.backgroundInterval != 5*time.Minute || s.timeout != 0 || s.targetMbps != 25 || s.quicPolicy != "auto" {
					t.Fatalf("legacy zero defaults changed: heartbeat=%s standby=%s background=%s timeout=%s target=%g quic=%q", s.heartbeat, s.standbyInterval, s.backgroundInterval, s.timeout, s.targetMbps, s.quicPolicy)
				}
			})
		}
	}
}

func TestSmartPreferenceExistingFieldsRequirePositiveOverrides(t *testing.T) {
	for _, preference := range []string{
		`{"profile":"speed-max"}`,
		`{"profile":"data-saver"}`,
		`{"switch":{"improvement":false}}`,
		`{"profile":"default","rank":["latency"]}`,
	} {
		for _, test := range []struct {
			fields string
			field  string
		}{
			{`"probe":{"heartbeat_interval":"0s"}`, "probe.heartbeat_interval"},
			{`"probe":{"standby_interval":"0s"}`, "probe.standby_interval"},
			{`"probe":{"background_interval":"0s"}`, "probe.background_interval"},
			{`"probe":{"timeout":"0s"}`, "probe.timeout"},
			{`"throughput":{"target_mbps":0}`, "throughput.target_mbps"},
			{`"udp":{"quic_policy":""}`, "udp.quic_policy"},
		} {
			document := `{"preference":` + preference + `,` + test.fields + `}`
			t.Run(document, func(t *testing.T) {
				var options option.SmartOutboundOptions
				if err := json.Unmarshal([]byte(document), &options); err != nil {
					t.Fatal(err)
				}
				before := options
				if _, err := resolveSmartPreference(&options); err == nil || !strings.Contains(err.Error(), test.field) {
					t.Fatalf("explicit invalid %s was not rejected during resolution: %v", test.field, err)
				}
				if !reflect.DeepEqual(before, options) {
					t.Fatal("invalid explicit fields were replaced by profile defaults")
				}
			})
		}
	}
}

func TestSmartPreferenceExistingFieldsRejectInvalidValues(t *testing.T) {
	for _, preference := range []string{"", `,"preference":{}`, `,"preference":{"profile":"default"}`, `,"preference":{"profile":"speed-max"}`, `,"preference":{"switch":{"improvement":false}}`} {
		for _, fields := range []string{
			`"probe":{"heartbeat_interval":"-1s"}`,
			`"probe":{"standby_interval":"-1s"}`,
			`"probe":{"background_interval":"-1s"}`,
			`"probe":{"timeout":"-1s"}`,
			`"throughput":{"target_mbps":-1}`,
			`"udp":{"quic_policy":"invalid"}`,
		} {
			document := `{` + fields + preference + `}`
			t.Run(document, func(t *testing.T) {
				s, _ := smartTestGroup()
				if err := json.Unmarshal([]byte(document), &s.options); err != nil {
					t.Fatal(err)
				}
				if err := s.configure(); err == nil {
					t.Fatal("invalid existing field was accepted")
				}
			})
		}
	}
}

func TestSmartPreferenceExplicitLeavesWin(t *testing.T) {
	var options option.SmartOutboundOptions
	if err := json.Unmarshal([]byte(`{"preference":{"profile":"speed-max","rank":["quic","outages"],"min_stability":"none","speed_floor_hysteresis":0,"switch":{"improvement":false,"dwell":"1m","confirmations":1,"confirmation_interval":"2s","tier_restore":"0s","speed_margin":1}},"probe":{"heartbeat_interval":"7s","standby_interval":"11s","background_interval":"13s","timeout":"3s"},"throughput":{"enabled":false,"target_mbps":9,"wifi_daily_budget_mb":0,"cellular_daily_budget_mb":1,"other_daily_budget_mb":2},"interrupt_on_failover":false,"udp":{"enabled":false,"quic_policy":"off"},"notify_all_dead":false}`), &options); err != nil {
		t.Fatal(err)
	}
	before, err := json.Marshal(options)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := resolveSmartPreference(&options)
	if err != nil {
		t.Fatal(err)
	}
	expected := smartPreference{
		profile: "speed-max", rank: []string{"quic", "outages"}, minStability: -1,
		improvement: false, dwell: time.Minute, confirmations: 1, confirmationInterval: 2 * time.Second,
		tierRestore: 0, speedMargin: 1, hysteresis: 0,
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("explicit preference leaves lost: %#v", actual)
	}
	after, err := json.Marshal(options)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatalf("explicit existing fields changed: %s != %s", before, after)
	}
}

func TestSmartPreferenceExplicitTrueOverridesFalseProfileDefaults(t *testing.T) {
	for _, document := range []string{
		`{"preference":{"profile":"data-saver"},"throughput":{"enabled":true}}`,
		`{"preference":{"profile":"stability-connections"},"interrupt_on_failover":true}`,
	} {
		var options option.SmartOutboundOptions
		if err := json.Unmarshal([]byte(document), &options); err != nil {
			t.Fatal(err)
		}
		if _, err := resolveSmartPreference(&options); err != nil {
			t.Fatal(err)
		}
		if options.Throughput.Enabled == nil || !*options.Throughput.Enabled || options.InterruptOnFailover == nil || !*options.InterruptOnFailover {
			t.Fatal("explicit true was replaced by a profile's false default")
		}
	}
}

func TestSmartPreferenceDefaultOverridesUseCustomEngine(t *testing.T) {
	for _, leaf := range []string{
		`"rank":["latency"]`,
		`"min_stability":"none"`,
		`"speed_floor_hysteresis":0`,
		`"switch":{"improvement":false}`,
		`"switch":{"dwell":"1m"}`,
		`"switch":{"confirmations":1}`,
		`"switch":{"confirmation_interval":"1s"}`,
		`"switch":{"tier_restore":"0s"}`,
		`"switch":{"speed_margin":1}`,
	} {
		for _, profile := range []string{"", `"profile":"default",`} {
			document := `{"preference":{` + profile + leaf + `}}`
			t.Run(document, func(t *testing.T) {
				var options option.SmartOutboundOptions
				if err := json.Unmarshal([]byte(document), &options); err != nil {
					t.Fatal(err)
				}
				actual, err := resolveSmartPreference(&options)
				if err != nil {
					t.Fatal(err)
				}
				if actual.legacy || actual.profile != "custom" || options.Throughput.TargetMbps != 25 {
					t.Fatalf("new preference leaf did not select custom engine: %#v", actual)
				}
				if strings.Contains(leaf, `"improvement":false`) && actual.improvement {
					t.Fatal("improvement false was not honored")
				}
				if strings.Contains(leaf, `"rank"`) && !reflect.DeepEqual(actual.rank, []string{"latency"}) {
					t.Fatal("explicit rank did not replace the base rank")
				}
			})
		}
	}
}

func TestSmartPreferenceMinimumStabilityValues(t *testing.T) {
	for expected, minimum := range []string{"A", "B", "C", "D", "none"} {
		options := option.SmartOutboundOptions{Preference: &option.SmartPreferenceOptions{MinStability: &minimum}}
		actual, err := resolveSmartPreference(&options)
		if err != nil {
			t.Fatal(err)
		}
		if minimum == "none" {
			expected = -1
		}
		if actual.minStability != expected {
			t.Fatalf("minimum %s = %d, want %d", minimum, actual.minStability, expected)
		}
	}
}

func TestSmartPreferenceValidation(t *testing.T) {
	for _, preference := range []string{
		`{"profile":"unknown"}`, `{"profile":""}`, `{"profile":"custom"}`,
		`{"rank":[]}`, `{"rank":["unknown"]}`, `{"rank":["latency","latency"]}`,
		`{"min_stability":"E"}`, `{"min_stability":"a"}`, `{"min_stability":""}`,
		`{"speed_floor_hysteresis":-0.01}`, `{"speed_floor_hysteresis":0.51}`,
		`{"switch":{"dwell":"0s"}}`, `{"switch":{"dwell":"-1s"}}`,
		`{"switch":{"confirmation_interval":"0s"}}`, `{"switch":{"confirmation_interval":"-1s"}}`,
		`{"switch":{"tier_restore":"-1s"}}`, `{"switch":{"confirmations":0}}`, `{"switch":{"confirmations":-1}}`,
		`{"switch":{"speed_margin":0}}`, `{"switch":{"speed_margin":0.99}}`,
	} {
		t.Run(preference, func(t *testing.T) {
			var options option.SmartOutboundOptions
			if err := json.Unmarshal([]byte(`{"preference":`+preference+`}`), &options); err != nil {
				t.Fatal(err)
			}
			if _, err := resolveSmartPreference(&options); err == nil {
				t.Fatal("invalid preference was accepted")
			}
		})
	}
}

func TestSmartPreferenceFiniteNumbersAndBoundaryValues(t *testing.T) {
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		for _, preference := range []*option.SmartPreferenceOptions{
			{SpeedFloorHysteresis: &value},
			{Switch: &option.SmartSwitchOptions{SpeedMargin: &value}},
		} {
			options := option.SmartOutboundOptions{Preference: preference}
			if _, err := resolveSmartPreference(&options); err == nil {
				t.Fatal("non-finite preference value was accepted")
			}
		}
	}
	for _, value := range []float64{0, 0.5} {
		options := option.SmartOutboundOptions{Preference: &option.SmartPreferenceOptions{SpeedFloorHysteresis: &value}}
		if _, err := resolveSmartPreference(&options); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSmartPreferenceCustomBaseDefaults(t *testing.T) {
	var options option.SmartOutboundOptions
	if err := json.Unmarshal([]byte(`{"preference":{"switch":{"improvement":false}}}`), &options); err != nil {
		t.Fatal(err)
	}
	actual, err := resolveSmartPreference(&options)
	if err != nil {
		t.Fatal(err)
	}
	expected := smartPreference{
		profile: "custom", rank: []string{"stability", "throughput", "latency"}, minStability: -1,
		improvement: false, dwell: 10 * time.Minute, confirmations: 2,
		confirmationInterval: 30 * time.Second, tierRestore: time.Minute, speedMargin: 1.25, hysteresis: 0.2,
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("custom base = %#v, want %#v", actual, expected)
	}
	if options.Throughput.TargetMbps != 25 || options.Throughput.Enabled == nil || !*options.Throughput.Enabled ||
		time.Duration(options.Probe.HeartbeatInterval) != 15*time.Second ||
		time.Duration(options.Probe.StandbyInterval) != time.Minute ||
		time.Duration(options.Probe.BackgroundInterval) != 5*time.Minute ||
		options.InterruptOnFailover == nil || !*options.InterruptOnFailover || options.UDP.QUICPolicy != "auto" {
		t.Fatalf("custom existing-field defaults = %#v", options)
	}
}

func TestSmartPreferenceAcceptsEveryRankCriterion(t *testing.T) {
	rank := []string{"stability", "speed_floor", "download", "throughput", "latency", "rtt_spread", "udp", "quic", "outages"}
	options := option.SmartOutboundOptions{Preference: &option.SmartPreferenceOptions{Rank: &rank}}
	actual, err := resolveSmartPreference(&options)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual.rank, rank) {
		t.Fatalf("explicit rank changed: %#v", actual.rank)
	}
}
