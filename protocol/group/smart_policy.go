package group

import (
	stdjson "encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
)

func smartPreferenceHasLeaves(preference *option.SmartPreferenceOptions) bool {
	if preference == nil {
		return false
	}
	if preference.Rank != nil || preference.MinStability != nil || preference.SpeedFloorHysteresis != nil {
		return true
	}
	switches := preference.Switch
	return switches != nil && (switches.Improvement != nil || switches.Dwell != nil || switches.Confirmations != nil || switches.ConfirmationInterval != nil || switches.TierRestore != nil || switches.SpeedMargin != nil)
}

// CheckSmartPolicy validates the app policy using the outbound's preference resolver.
func CheckSmartPolicy(policy string) error {
	_, err := ParseSmartPolicy(policy)
	return err
}

// ParseSmartPolicy validates an override and preserves its explicit field presence.
func ParseSmartPolicy(policy string) (*option.SmartOutboundOptions, error) {
	decoder := stdjson.NewDecoder(strings.NewReader(policy))
	decoder.UseNumber()
	if err := checkSmartPolicyValue(decoder, ""); err != nil {
		return nil, fmt.Errorf("smart policy override: %w", err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("unexpected value after policy object")
		}
		return nil, fmt.Errorf("smart policy override: policy: %w", err)
	}
	var options option.SmartOutboundOptions
	if err := json.UnmarshalDisallowUnknownFields([]byte(policy), &options); err != nil {
		return nil, fmt.Errorf("smart policy override: policy: %w", err)
	}
	// Override fields must be valid even when the selected base uses the legacy engine.
	if err := options.ValidateProfileFields(); err != nil {
		return nil, fmt.Errorf("smart policy override: %w", err)
	}
	resolved := options
	if _, err := resolveSmartPreference(&resolved); err != nil {
		return nil, fmt.Errorf("smart policy override: %w", err)
	}
	return &options, nil
}

func checkSmartPolicyValue(decoder *stdjson.Decoder, path string) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("%s: %w", pathOrPolicy(path), err)
	}
	var allowed []string
	switch path {
	case "":
		allowed = []string{"preference", "throughput", "probe", "interrupt_on_failover", "udp"}
	case "preference":
		allowed = []string{"profile", "rank", "min_stability", "speed_floor_hysteresis", "switch"}
	case "preference.switch":
		allowed = []string{"improvement", "dwell", "confirmations", "confirmation_interval", "tier_restore", "speed_margin"}
	case "throughput":
		allowed = []string{"target_mbps", "enabled"}
	case "probe":
		allowed = []string{"heartbeat_interval", "standby_interval", "background_interval"}
	case "udp":
		allowed = []string{"quic_policy"}
	}
	if allowed != nil && token != stdjson.Delim('{') {
		return fmt.Errorf("%s: expected an object", pathOrPolicy(path))
	}
	switch token {
	case nil:
		return fmt.Errorf("%s must not be null", pathOrPolicy(path))
	case stdjson.Delim('{'):
		seen := make(map[string]bool)
		for decoder.More() {
			token, err = decoder.Token()
			if err != nil {
				return fmt.Errorf("%s: %w", pathOrPolicy(path), err)
			}
			key := token.(string)
			field := key
			if path != "" {
				field = path + "." + key
			}
			if seen[key] {
				return fmt.Errorf("duplicate key %s", field)
			}
			seen[key] = true
			if !slices.Contains(allowed, key) {
				return fmt.Errorf("unknown key %s", field)
			}
			if err = checkSmartPolicyValue(decoder, field); err != nil {
				return err
			}
		}
		if _, err = decoder.Token(); err != nil {
			return fmt.Errorf("%s: %w", pathOrPolicy(path), err)
		}
	case stdjson.Delim('['):
		for index := 0; decoder.More(); index++ {
			if err = checkSmartPolicyValue(decoder, fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
		if _, err = decoder.Token(); err != nil {
			return fmt.Errorf("%s: %w", pathOrPolicy(path), err)
		}
	}
	return nil
}

func pathOrPolicy(path string) string {
	if path == "" {
		return "policy"
	}
	return path
}

func (s *Smart) resolvedSmartPolicy() map[string]any {
	base := s.preference.profile
	if base == "custom" {
		base = "default"
	}
	floor := "none"
	if s.preference.minStability >= 0 {
		floor = string(rune('A' + s.preference.minStability))
	}
	return map[string]any{
		"preference": map[string]any{
			"profile": base, "rank": slices.Clone(s.preference.rank), "min_stability": floor,
			"speed_floor_hysteresis": s.preference.hysteresis,
			"switch": map[string]any{
				"improvement": s.preference.improvement, "dwell": s.preference.dwell.String(),
				"confirmations": s.preference.confirmations, "confirmation_interval": s.preference.confirmationInterval.String(),
				"tier_restore": s.preference.tierRestore.String(), "speed_margin": s.preference.speedMargin,
			},
		},
		"throughput":            map[string]any{"target_mbps": s.targetMbps, "enabled": s.throughputEnabled},
		"probe":                 map[string]any{"heartbeat_interval": s.heartbeat.String(), "standby_interval": s.standbyInterval.String(), "background_interval": s.backgroundInterval.String()},
		"interrupt_on_failover": s.interrupt,
		"udp":                   map[string]any{"quic_policy": s.quicPolicy},
	}
}

// SmartProfiles returns every built-in profile in picker order, resolved by the core.
func SmartProfiles() ([]map[string]any, error) {
	profiles := make([]map[string]any, 0, len(smartProfiles))
	for _, definition := range smartProfiles {
		name := definition.name
		s := &Smart{options: option.SmartOutboundOptions{Preference: &option.SmartPreferenceOptions{Profile: &name}}}
		if err := s.configure(); err != nil {
			return nil, err
		}
		floor := "none"
		if s.preference.minStability >= 0 {
			floor = string(rune('A' + s.preference.minStability))
		}
		profiles = append(profiles, map[string]any{
			"name": name, "rank": slices.Clone(s.preference.rank), "min_stability": floor,
			"target_mbps": s.targetMbps, "throughput_enabled": s.throughputEnabled,
			"heartbeat_interval": s.heartbeat.String(), "standby_interval": s.standbyInterval.String(), "background_interval": s.backgroundInterval.String(),
			"interrupt_on_failover": s.interrupt, "quic_policy": s.quicPolicy,
			"improvement": s.preference.improvement, "dwell": s.preference.dwell.String(), "confirmations": s.preference.confirmations,
			"confirmation_interval": s.preference.confirmationInterval.String(), "tier_restore": s.preference.tierRestore.String(),
			"hysteresis": s.preference.hysteresis, "speed_margin": s.preference.speedMargin,
		})
	}
	return profiles, nil
}
