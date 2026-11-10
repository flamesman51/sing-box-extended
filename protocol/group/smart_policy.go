package group

import (
	"fmt"
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
	_, err := parseSmartPolicy(policy)
	return err
}

func parseSmartPolicy(policy string) (map[string]json.RawMessage, error) {
	var object map[string]any
	if err := json.Unmarshal([]byte(policy), &object); err != nil {
		return nil, fmt.Errorf("smart policy override: %w", err)
	}
	if object == nil {
		return nil, fmt.Errorf("smart policy override: expected an object")
	}
	if err := checkSmartPolicyKeys(object, ""); err != nil {
		return nil, fmt.Errorf("smart policy override: %w", err)
	}
	var options option.SmartOutboundOptions
	if err := json.UnmarshalDisallowUnknownFields([]byte(policy), &options); err != nil {
		return nil, fmt.Errorf("smart policy override: %w", err)
	}
	// Override fields must be valid even when the selected base uses the legacy engine.
	if err := options.ValidateProfileFields(); err != nil {
		return nil, fmt.Errorf("smart policy override: %w", err)
	}
	if _, err := resolveSmartPreference(&options); err != nil {
		return nil, fmt.Errorf("smart policy override: %w", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(policy), &raw); err != nil {
		return nil, fmt.Errorf("smart policy override: %w", err)
	}
	return raw, nil
}

func checkSmartPolicyKeys(object map[string]any, path string) error {
	var allowed []string
	switch path {
	case "":
		allowed = []string{"preference", "throughput", "probe", "interrupt_on_failover", "udp"}
	case "throughput":
		allowed = []string{"target_mbps", "enabled"}
	case "probe":
		allowed = []string{"heartbeat_interval", "standby_interval", "background_interval"}
	case "udp":
		allowed = []string{"quic_policy"}
	}
	for key, value := range object {
		if allowed != nil && !slices.Contains(allowed, key) {
			return fmt.Errorf("unknown field %q in %s", key, pathOrPolicy(path))
		}
		field := key
		if path != "" {
			field = path + "." + key
		}
		if value == nil {
			return fmt.Errorf("%s must not be null", field)
		}
		switch value := value.(type) {
		case map[string]any:
			if err := checkSmartPolicyKeys(value, field); err != nil {
				return err
			}
		case []any:
			for _, item := range value {
				if item == nil {
					return fmt.Errorf("%s must not contain null", field)
				}
			}
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

// ApplySmartPolicyOverride replaces only profile-owned fields on every smart outbound.
func ApplySmartPolicyOverride(config, policy string) (string, error) {
	override, err := parseSmartPolicy(policy)
	if err != nil {
		return "", err
	}
	var root map[string]json.RawMessage
	if err = json.Unmarshal([]byte(config), &root); err != nil {
		return "", fmt.Errorf("decode config: %w", err)
	}
	if root == nil {
		return "", fmt.Errorf("decode config: expected an object")
	}
	if !removeSmartPolicyField(root, "outbounds") {
		return config, nil
	}
	var envelope struct {
		Outbounds []json.RawMessage `json:"outbounds"`
	}
	if err = json.Unmarshal([]byte(config), &envelope); err != nil {
		return "", fmt.Errorf("decode config outbounds: %w", err)
	}
	for index, content := range envelope.Outbounds {
		var fields struct {
			Type       string          `json:"type"`
			Throughput json.RawMessage `json:"throughput"`
			Probe      json.RawMessage `json:"probe"`
			UDP        json.RawMessage `json:"udp"`
		}
		if err = json.Unmarshal(content, &fields); err != nil {
			return "", fmt.Errorf("decode config outbound: %w", err)
		}
		if fields.Type != "smart" {
			continue
		}
		var outbound map[string]json.RawMessage
		if err = json.Unmarshal(content, &outbound); err != nil {
			return "", fmt.Errorf("decode config smart outbound: %w", err)
		}
		removeSmartPolicyField(outbound, "type")
		outbound["type"] = json.RawMessage(`"smart"`)
		removeSmartPolicyField(outbound, "preference")
		removeSmartPolicyField(outbound, "interrupt_on_failover")
		for _, field := range []struct {
			name    string
			content json.RawMessage
			keys    []string
		}{
			{"throughput", fields.Throughput, []string{"target_mbps", "enabled"}},
			{"probe", fields.Probe, []string{"heartbeat_interval", "standby_interval", "background_interval"}},
			{"udp", fields.UDP, []string{"quic_policy"}},
		} {
			if !removeSmartPolicyField(outbound, field.name) {
				continue
			}
			var nested map[string]json.RawMessage
			if err = json.Unmarshal(field.content, &nested); err != nil {
				return "", fmt.Errorf("decode config smart %s: %w", field.name, err)
			}
			if nested == nil {
				outbound[field.name] = field.content
				continue
			}
			for _, key := range field.keys {
				removeSmartPolicyField(nested, key)
			}
			outbound[field.name], err = json.Marshal(nested)
			if err != nil {
				return "", err
			}
		}
		if err = mergeSmartPolicy(outbound, override); err != nil {
			return "", err
		}
		envelope.Outbounds[index], err = json.Marshal(outbound)
		if err != nil {
			return "", err
		}
	}
	root["outbounds"], err = json.Marshal(envelope.Outbounds)
	if err != nil {
		return "", err
	}
	content, err := json.Marshal(root)
	return string(content), err
}

func removeSmartPolicyField(object map[string]json.RawMessage, name string) bool {
	found := false
	for key := range object {
		if strings.EqualFold(key, name) {
			delete(object, key)
			found = true
		}
	}
	return found
}

func mergeSmartPolicy(destination, override map[string]json.RawMessage) error {
	for key, value := range override {
		var nested map[string]json.RawMessage
		if json.Unmarshal(value, &nested) == nil && nested != nil {
			var existing map[string]json.RawMessage
			if content, exists := destination[key]; exists {
				if err := json.Unmarshal(content, &existing); err != nil {
					return fmt.Errorf("decode config smart %s: %w", key, err)
				}
			}
			if existing == nil {
				existing = make(map[string]json.RawMessage)
			}
			if err := mergeSmartPolicy(existing, nested); err != nil {
				return err
			}
			content, err := json.Marshal(existing)
			if err != nil {
				return err
			}
			destination[key] = content
		} else {
			destination[key] = value
		}
	}
	return nil
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
