package group

import (
	"fmt"
	"math"
	"time"

	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
)

type smartPreference struct {
	legacy               bool
	profile              string
	rank                 []string
	minStability         int
	improvement          bool
	dwell                time.Duration
	confirmations        int
	confirmationInterval time.Duration
	tierRestore          time.Duration
	speedMargin          float64
	hysteresis           float64
}

func resolveSmartPreference(options *option.SmartOutboundOptions) (smartPreference, error) {
	preference := smartPreference{
		legacy: true, profile: "default", rank: []string{"stability", "throughput", "latency"},
		minStability: -1, improvement: true, dwell: 10 * time.Minute, confirmations: 2,
		confirmationInterval: 30 * time.Second, tierRestore: time.Minute, speedMargin: 1.25, hysteresis: 0.2,
	}
	configured := options.Preference
	if configured == nil {
		return preference, nil
	}
	if configured.Profile != nil {
		preference.profile = *configured.Profile
	}
	if preference.profile == "default" && configured.Rank == nil && configured.MinStability == nil && configured.SpeedFloorHysteresis == nil {
		switches := configured.Switch
		if switches == nil || (switches.Improvement == nil && switches.Dwell == nil && switches.Confirmations == nil && switches.ConfirmationInterval == nil && switches.TierRestore == nil && switches.SpeedMargin == nil) {
			return preference, nil
		}
	}
	preference.legacy = false
	if err := options.ValidateProfileFields(); err != nil {
		return smartPreference{}, err
	}
	target := 25.0
	heartbeat, standby, background := 15*time.Second, time.Minute, 5*time.Minute
	interrupt, throughputEnabled := true, true
	switch preference.profile {
	case "default":
		preference.profile = "custom"
	case "stability-downtime":
		preference.rank = []string{"stability", "outages", "latency"}
		target, heartbeat, standby = 4, 10*time.Second, 30*time.Second
	case "stability-connections":
		preference.rank = []string{"stability", "outages", "latency"}
		target, interrupt = 4, false
		preference.dwell, preference.confirmations = 30*time.Minute, 3
	case "balance-speed-720p", "balance-speed-1080p", "balance-speed-1440p", "balance-speed-4k", "phone", "desktop", "server":
		preference.rank = []string{"speed_floor", "stability", "download", "latency"}
		preference.minStability = 2
		switch preference.profile {
		case "balance-speed-720p":
			target = 4
		case "balance-speed-1080p", "phone":
			target = 8
		case "balance-speed-1440p":
			target = 15
		case "server":
			heartbeat, standby = 10*time.Second, 30*time.Second
		}
	case "speed-max":
		preference.rank = []string{"download", "stability", "latency"}
		preference.minStability, preference.dwell = 2, 5*time.Minute
	case "responsive":
		preference.rank = []string{"latency", "stability", "download"}
		preference.minStability, target = 2, 4
	case "realtime-calls":
		preference.rank = []string{"udp", "stability", "rtt_spread", "latency", "speed_floor"}
		preference.minStability, target = 2, 4
		heartbeat, standby, interrupt = 10*time.Second, 30*time.Second, false
	case "realtime-gaming":
		preference.rank = []string{"udp", "latency", "rtt_spread", "stability"}
		preference.minStability, target = 2, 2
		heartbeat, standby, interrupt = 10*time.Second, 30*time.Second, false
	case "data-saver":
		preference.rank = []string{"stability", "latency"}
		target, throughputEnabled = 4, false
		heartbeat, standby, background = 30*time.Second, 2*time.Minute, 15*time.Minute
	default:
		return smartPreference{}, fmt.Errorf("unknown smart preference profile: %q", preference.profile)
	}
	if configured.Rank != nil {
		preference.rank = *configured.Rank
		if len(preference.rank) == 0 {
			return smartPreference{}, fmt.Errorf("smart preference.rank must not be empty")
		}
		for index, criterion := range preference.rank {
			switch criterion {
			case "stability", "speed_floor", "download", "throughput", "latency", "rtt_spread", "udp", "quic", "outages":
			default:
				return smartPreference{}, fmt.Errorf("unknown smart preference rank criterion: %q", criterion)
			}
			for _, previous := range preference.rank[:index] {
				if previous == criterion {
					return smartPreference{}, fmt.Errorf("duplicate smart preference rank criterion: %q", criterion)
				}
			}
		}
	}
	if configured.MinStability != nil {
		switch *configured.MinStability {
		case "none":
			preference.minStability = -1
		case "A", "B", "C", "D":
			preference.minStability = int((*configured.MinStability)[0] - 'A')
		default:
			return smartPreference{}, fmt.Errorf("invalid smart preference.min_stability: %q", *configured.MinStability)
		}
	}
	if configured.SpeedFloorHysteresis != nil {
		preference.hysteresis = *configured.SpeedFloorHysteresis
	}
	if switches := configured.Switch; switches != nil {
		if switches.Improvement != nil {
			preference.improvement = *switches.Improvement
		}
		if switches.Dwell != nil {
			preference.dwell = time.Duration(*switches.Dwell)
		}
		if switches.Confirmations != nil {
			preference.confirmations = *switches.Confirmations
		}
		if switches.ConfirmationInterval != nil {
			preference.confirmationInterval = time.Duration(*switches.ConfirmationInterval)
		}
		if switches.TierRestore != nil {
			preference.tierRestore = time.Duration(*switches.TierRestore)
		}
		if switches.SpeedMargin != nil {
			preference.speedMargin = *switches.SpeedMargin
		}
	}
	if preference.dwell <= 0 || preference.confirmationInterval <= 0 || preference.tierRestore < 0 {
		return smartPreference{}, fmt.Errorf("smart preference switch durations must be positive; tier_restore may be zero to disable restoration")
	}
	if preference.confirmations < 1 {
		return smartPreference{}, fmt.Errorf("smart preference.switch.confirmations must be at least 1")
	}
	if math.IsNaN(preference.speedMargin) || math.IsInf(preference.speedMargin, 0) || preference.speedMargin < 1 {
		return smartPreference{}, fmt.Errorf("smart preference.switch.speed_margin must be finite and at least 1")
	}
	if math.IsNaN(preference.hysteresis) || math.IsInf(preference.hysteresis, 0) || preference.hysteresis < 0 || preference.hysteresis > 0.5 {
		return smartPreference{}, fmt.Errorf("smart preference.speed_floor_hysteresis must be finite and between 0 and 0.5")
	}
	if options.Throughput.TargetMbps == 0 {
		options.Throughput.TargetMbps = target
	}
	if options.Throughput.Enabled == nil {
		options.Throughput.Enabled = &throughputEnabled
	}
	if options.Probe.HeartbeatInterval == 0 {
		options.Probe.HeartbeatInterval = badoption.Duration(heartbeat)
	}
	if options.Probe.StandbyInterval == 0 {
		options.Probe.StandbyInterval = badoption.Duration(standby)
	}
	if options.Probe.BackgroundInterval == 0 {
		options.Probe.BackgroundInterval = badoption.Duration(background)
	}
	if options.InterruptOnFailover == nil {
		options.InterruptOnFailover = &interrupt
	}
	if options.UDP.QUICPolicy == "" {
		options.UDP.QUICPolicy = "auto"
	}
	return preference, nil
}
