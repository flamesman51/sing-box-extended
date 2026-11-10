package option

import (
	"fmt"
	"math"

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
	MemberOptions       map[string]SmartMemberOptions `json:"member_options,omitempty"`
	Probe               SmartProbeOptions             `json:"probe,omitempty"`
	UDP                 SmartUDPOptions               `json:"udp,omitempty"`
	Throughput          SmartThroughputOptions        `json:"throughput,omitempty"`
	Preference          *SmartPreferenceOptions       `json:"preference,omitempty"`
	InterruptOnFailover *bool                         `json:"interrupt_on_failover,omitempty"`
	NotifyAllDead       *bool                         `json:"notify_all_dead,omitempty"`
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
}

type SmartUDPOptions struct {
	Enabled    *bool  `json:"enabled,omitempty"`
	QUICPolicy string `json:"quic_policy,omitempty"`
}

type SmartThroughputOptions struct {
	Enabled               *bool    `json:"enabled,omitempty"`
	TargetMbps            float64  `json:"target_mbps,omitempty"`
	DownloadURL           string   `json:"download_url,omitempty"`
	UploadURL             string   `json:"upload_url,omitempty"`
	WIFIDailyBudgetMB     *float64 `json:"wifi_daily_budget_mb,omitempty"`
	CellularDailyBudgetMB *float64 `json:"cellular_daily_budget_mb,omitempty"`
	OtherDailyBudgetMB    *float64 `json:"other_daily_budget_mb,omitempty"`
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
		name  string
		value *badoption.Duration
		dest  *badoption.Duration
	}{
		{"heartbeat_interval", decoded.HeartbeatInterval, &decoded.probeOptions.HeartbeatInterval},
		{"standby_interval", decoded.StandbyInterval, &decoded.probeOptions.StandbyInterval},
		{"background_interval", decoded.BackgroundInterval, &decoded.probeOptions.BackgroundInterval},
		{"timeout", decoded.Timeout, &decoded.probeOptions.Timeout},
	} {
		if field.value != nil {
			if *field.value <= 0 {
				return fmt.Errorf("smart probe.%s must be positive", field.name)
			}
			*field.dest = *field.value
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
		switch *decoded.QUICPolicy {
		case "auto", "route_only", "off":
			decoded.udpOptions.QUICPolicy = *decoded.QUICPolicy
		default:
			return fmt.Errorf("invalid smart udp.quic_policy: %q", *decoded.QUICPolicy)
		}
	}
	*o = SmartUDPOptions(decoded.udpOptions)
	return nil
}

func (o *SmartThroughputOptions) UnmarshalJSON(content []byte) error {
	type throughputOptions SmartThroughputOptions
	var decoded struct {
		throughputOptions
		TargetMbps *float64 `json:"target_mbps,omitempty"`
	}
	if err := json.UnmarshalDisallowUnknownFields(content, &decoded); err != nil {
		return err
	}
	if decoded.TargetMbps != nil {
		target := *decoded.TargetMbps
		if target <= 0 || math.IsNaN(target) || math.IsInf(target, 0) {
			return fmt.Errorf("smart throughput.target_mbps must be positive and finite")
		}
		decoded.throughputOptions.TargetMbps = target
	}
	*o = SmartThroughputOptions(decoded.throughputOptions)
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
