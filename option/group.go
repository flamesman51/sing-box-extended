package option

import (
	"fmt"
	"math"
	"time"

	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/common/json/badoption"
)

type SelectorOutboundOptions struct {
	GroupCommonOption
	Default                   string `json:"default,omitempty" reference:"outbound"`
	InterruptExistConnections bool   `json:"interrupt_exist_connections,omitempty"`
}

type URLTestOutboundOptions struct {
	GroupCommonOption
	URL                       string             `json:"url,omitempty"`
	Interval                  badoption.Duration `json:"interval,omitempty"`
	Tolerance                 uint16             `json:"tolerance,omitempty"`
	IdleTimeout               badoption.Duration `json:"idle_timeout,omitempty"`
	InterruptExistConnections bool               `json:"interrupt_exist_connections,omitempty"`
}

type SmartOutboundOptions struct {
	GroupCommonOption
	MemberOptions           map[string]SmartMemberOptions `json:"member_options,omitempty"`
	Probe                   SmartProbeOptions             `json:"probe,omitempty"`
	UDP                     SmartUDPOptions               `json:"udp,omitempty"`
	Throughput              SmartThroughputOptions        `json:"throughput,omitempty"`
	Preference              *SmartPreferenceOptions       `json:"preference,omitempty"`
	InterruptOnFailover     *bool                         `json:"interrupt_on_failover,omitempty"`
	NotifyAllDead           *bool                         `json:"notify_all_dead,omitempty"`
	SmartPolicyOverrideHash string                        `json:"-"`
}

type SmartPreferenceOptions struct {
	Profile              *string             `json:"profile,omitempty"`
	Rank                 *[]string           `json:"rank,omitempty"`
	MinStability         *string             `json:"min_stability,omitempty"`
	SpeedFloorHysteresis *float64            `json:"speed_floor_hysteresis,omitempty"`
	Switch               *SmartSwitchOptions `json:"switch,omitempty"`
}

type SmartSwitchOptions struct {
	Improvement          *bool               `json:"improvement,omitempty"`
	Dwell                *badoption.Duration `json:"dwell,omitempty"`
	Confirmations        *int                `json:"confirmations,omitempty"`
	ConfirmationInterval *badoption.Duration `json:"confirmation_interval,omitempty"`
	TierRestore          *badoption.Duration `json:"tier_restore,omitempty"`
	SpeedMargin          *float64            `json:"speed_margin,omitempty"`
}

type SmartMemberOptions struct {
	Tier    int    `json:"tier,omitempty"`
	Country string `json:"country,omitempty"`
}

type SmartProbeOptions struct {
	HeartbeatInterval  badoption.Duration `json:"heartbeat_interval,omitempty"`
	StandbyInterval    badoption.Duration `json:"standby_interval,omitempty"`
	BackgroundInterval badoption.Duration `json:"background_interval,omitempty"`
	Timeout            badoption.Duration `json:"timeout,omitempty"`
	DisableDirect      bool               `json:"disable_direct,omitempty"`

	heartbeatIntervalSet  bool
	standbyIntervalSet    bool
	backgroundIntervalSet bool
	timeoutSet            bool
}

type SmartUDPOptions struct {
	Enabled    *bool  `json:"enabled,omitempty"`
	QUICPolicy string `json:"quic_policy,omitempty"`

	quicPolicySet bool
}

type SmartThroughputOptions struct {
	Enabled               *bool              `json:"enabled,omitempty"`
	TargetMbps            float64            `json:"target_mbps,omitempty"`
	Streams               int                `json:"streams,omitempty"`
	Window                badoption.Duration `json:"window,omitempty"`
	Warmup                badoption.Duration `json:"warmup,omitempty"`
	Upload                *bool              `json:"upload,omitempty"`
	UploadWindow          badoption.Duration `json:"upload_window,omitempty"`
	UploadWarmup          badoption.Duration `json:"upload_warmup,omitempty"`
	DownloadURL           string             `json:"download_url,omitempty"`
	UploadURL             string             `json:"upload_url,omitempty"`
	WIFIDailyBudgetMB     *float64           `json:"wifi_daily_budget_mb,omitempty"`
	CellularDailyBudgetMB *float64           `json:"cellular_daily_budget_mb,omitempty"`
	OtherDailyBudgetMB    *float64           `json:"other_daily_budget_mb,omitempty"`

	targetMbpsSet   bool
	streamsSet      bool
	windowSet       bool
	warmupSet       bool
	uploadWindowSet bool
	uploadWarmupSet bool
}

func (o *SmartProbeOptions) UnmarshalJSON(content []byte) error {
	type probeOptions SmartProbeOptions
	var decoded struct {
		probeOptions
		HeartbeatInterval  *badoption.Duration `json:"heartbeat_interval,omitempty"`
		StandbyInterval    *badoption.Duration `json:"standby_interval,omitempty"`
		BackgroundInterval *badoption.Duration `json:"background_interval,omitempty"`
		Timeout            *badoption.Duration `json:"timeout,omitempty"`
	}
	if err := json.UnmarshalDisallowUnknownFields(content, &decoded); err != nil {
		return err
	}
	for _, field := range []struct {
		value   *badoption.Duration
		dest    *badoption.Duration
		present *bool
	}{
		{decoded.HeartbeatInterval, &decoded.probeOptions.HeartbeatInterval, &decoded.probeOptions.heartbeatIntervalSet},
		{decoded.StandbyInterval, &decoded.probeOptions.StandbyInterval, &decoded.probeOptions.standbyIntervalSet},
		{decoded.BackgroundInterval, &decoded.probeOptions.BackgroundInterval, &decoded.probeOptions.backgroundIntervalSet},
		{decoded.Timeout, &decoded.probeOptions.Timeout, &decoded.probeOptions.timeoutSet},
	} {
		if field.value != nil {
			*field.dest = *field.value
			*field.present = true
		}
	}
	*o = SmartProbeOptions(decoded.probeOptions)
	return nil
}

func (o *SmartUDPOptions) UnmarshalJSON(content []byte) error {
	type udpOptions SmartUDPOptions
	var decoded struct {
		udpOptions
		QUICPolicy *string `json:"quic_policy,omitempty"`
	}
	if err := json.UnmarshalDisallowUnknownFields(content, &decoded); err != nil {
		return err
	}
	if decoded.QUICPolicy != nil {
		decoded.udpOptions.QUICPolicy = *decoded.QUICPolicy
		decoded.udpOptions.quicPolicySet = true
	}
	*o = SmartUDPOptions(decoded.udpOptions)
	return nil
}

func (o *SmartThroughputOptions) UnmarshalJSON(content []byte) error {
	type throughputOptions SmartThroughputOptions
	var decoded struct {
		throughputOptions
		TargetMbps   *float64            `json:"target_mbps,omitempty"`
		Streams      *int                `json:"streams,omitempty"`
		Window       *badoption.Duration `json:"window,omitempty"`
		Warmup       *badoption.Duration `json:"warmup,omitempty"`
		UploadWindow *badoption.Duration `json:"upload_window,omitempty"`
		UploadWarmup *badoption.Duration `json:"upload_warmup,omitempty"`
	}
	if err := json.UnmarshalDisallowUnknownFields(content, &decoded); err != nil {
		return err
	}
	if decoded.TargetMbps != nil {
		decoded.throughputOptions.TargetMbps = *decoded.TargetMbps
		decoded.throughputOptions.targetMbpsSet = true
	}
	if decoded.Streams != nil {
		decoded.throughputOptions.Streams = *decoded.Streams
		decoded.throughputOptions.streamsSet = true
	}
	for _, field := range []struct {
		value   *badoption.Duration
		dest    *badoption.Duration
		present *bool
	}{
		{decoded.Window, &decoded.throughputOptions.Window, &decoded.throughputOptions.windowSet},
		{decoded.Warmup, &decoded.throughputOptions.Warmup, &decoded.throughputOptions.warmupSet},
		{decoded.UploadWindow, &decoded.throughputOptions.UploadWindow, &decoded.throughputOptions.uploadWindowSet},
		{decoded.UploadWarmup, &decoded.throughputOptions.UploadWarmup, &decoded.throughputOptions.uploadWarmupSet},
	} {
		if field.value != nil {
			*field.dest = *field.value
			*field.present = true
		}
	}
	*o = SmartThroughputOptions(decoded.throughputOptions)
	return nil
}

// HasProfileOwnedFields reports explicit profile-owned fields before defaults are applied.
func (o *SmartOutboundOptions) HasProfileOwnedFields() bool {
	return o.InterruptOnFailover != nil || o.Throughput.Enabled != nil ||
		o.Throughput.targetMbpsSet || o.Throughput.TargetMbps != 0 ||
		o.Throughput.streamsSet || o.Throughput.Streams != 0 ||
		o.Probe.heartbeatIntervalSet || o.Probe.HeartbeatInterval != 0 ||
		o.Probe.standbyIntervalSet || o.Probe.StandbyInterval != 0 ||
		o.Probe.backgroundIntervalSet || o.Probe.BackgroundInterval != 0 ||
		o.UDP.quicPolicySet || o.UDP.QUICPolicy != ""
}

// ApplySmartPolicy replaces only profile-owned values and their presence metadata.
func (o *SmartOutboundOptions) ApplySmartPolicy(policy *SmartOutboundOptions) {
	o.Preference = policy.Preference
	o.InterruptOnFailover = policy.InterruptOnFailover
	o.Throughput.Enabled = policy.Throughput.Enabled
	o.Throughput.TargetMbps = policy.Throughput.TargetMbps
	o.Throughput.targetMbpsSet = policy.Throughput.targetMbpsSet
	o.Throughput.Streams = policy.Throughput.Streams
	o.Throughput.streamsSet = policy.Throughput.streamsSet
	o.Probe.HeartbeatInterval = policy.Probe.HeartbeatInterval
	o.Probe.heartbeatIntervalSet = policy.Probe.heartbeatIntervalSet
	o.Probe.StandbyInterval = policy.Probe.StandbyInterval
	o.Probe.standbyIntervalSet = policy.Probe.standbyIntervalSet
	o.Probe.BackgroundInterval = policy.Probe.BackgroundInterval
	o.Probe.backgroundIntervalSet = policy.Probe.backgroundIntervalSet
	o.UDP.QUICPolicy = policy.UDP.QUICPolicy
	o.UDP.quicPolicySet = policy.UDP.quicPolicySet
}

// ValidateProfileFields applies presence-aware validation only for the profile engine.
func (o *SmartOutboundOptions) ValidateProfileFields() error {
	if err := o.Throughput.ValidateMeasurementFields(true); err != nil {
		return err
	}
	for _, field := range []struct {
		name    string
		value   badoption.Duration
		present bool
	}{
		{"heartbeat_interval", o.Probe.HeartbeatInterval, o.Probe.heartbeatIntervalSet},
		{"standby_interval", o.Probe.StandbyInterval, o.Probe.standbyIntervalSet},
		{"background_interval", o.Probe.BackgroundInterval, o.Probe.backgroundIntervalSet},
		{"timeout", o.Probe.Timeout, o.Probe.timeoutSet},
	} {
		if field.value < 0 || (field.present && field.value == 0) {
			return fmt.Errorf("smart probe.%s must be positive", field.name)
		}
	}
	target := o.Throughput.TargetMbps
	if target < 0 || math.IsNaN(target) || math.IsInf(target, 0) || (o.Throughput.targetMbpsSet && target == 0) {
		return fmt.Errorf("smart throughput.target_mbps must be positive and finite")
	}
	switch o.UDP.QUICPolicy {
	case "":
		if o.UDP.quicPolicySet {
			return fmt.Errorf("invalid smart udp.quic_policy: %q", o.UDP.QUICPolicy)
		}
	case "auto", "route_only", "off":
	default:
		return fmt.Errorf("invalid smart udp.quic_policy: %q", o.UDP.QUICPolicy)
	}
	return nil
}

// ValidateMeasurementFields preserves legacy zero defaults while checking measurement bounds.
func (o *SmartThroughputOptions) ValidateMeasurementFields(profile bool) error {
	if o.Streams < 0 || (profile && o.streamsSet && o.Streams == 0) {
		return fmt.Errorf("smart throughput.streams must be at least 1")
	}
	for _, field := range []struct {
		name    string
		value   badoption.Duration
		present bool
		minimum time.Duration
	}{
		{"window", o.Window, o.windowSet, 5 * time.Second},
		{"warmup", o.Warmup, o.warmupSet, time.Second},
		{"upload_window", o.UploadWindow, o.uploadWindowSet, 5 * time.Second},
		{"upload_warmup", o.UploadWarmup, o.uploadWarmupSet, time.Second},
	} {
		if (field.value != 0 || (profile && field.present)) && time.Duration(field.value) < field.minimum {
			return fmt.Errorf("smart throughput.%s must be at least %s", field.name, field.minimum)
		}
	}
	return nil
}

type FallbackOutboundOptions struct {
	Outbounds        []string           `json:"outbounds"`
	BlacklistTimeout badoption.Duration `json:"blacklist_timeout,omitempty"`
}

type GroupCommonOption struct {
	Outbounds       []string                   `json:"outbounds" reference:"outbound"`
	Providers       badoption.Listable[string] `json:"providers,omitempty"`
	Exclude         *badoption.Regexp          `json:"exclude,omitempty"`
	Include         *badoption.Regexp          `json:"include,omitempty"`
	UseAllProviders bool                       `json:"use_all_providers,omitempty"`
}
