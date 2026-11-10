package group

import (
	"bytes"
	stdjson "encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/common/json/badjson"
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

func parseSmartPolicy(policy string) (*badjson.JSONObject, error) {
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
	if _, err := resolveSmartPreference(&options); err != nil {
		return nil, fmt.Errorf("smart policy override: %w", err)
	}
	raw, err := decodeSmartPolicyObject([]byte(policy))
	if err != nil {
		return nil, fmt.Errorf("smart policy override: policy: %w", err)
	}
	return raw, nil
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

// ApplySmartPolicyOverride replaces only profile-owned fields on every smart outbound.
func ApplySmartPolicyOverride(config, policy string) (string, error) {
	override, err := parseSmartPolicy(policy)
	if err != nil {
		return "", err
	}
	root, err := decodeSmartPolicyObject([]byte(config))
	if err != nil {
		return "", fmt.Errorf("decode config: %w", err)
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
		outbound, err := decodeSmartPolicyObject(content)
		if err != nil {
			return "", fmt.Errorf("decode config smart outbound: %w", err)
		}
		removeSmartPolicyField(outbound, "type")
		outbound.Put("type", json.RawMessage(`"smart"`))
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
			if bytes.Equal(bytes.TrimSpace(field.content), []byte("null")) {
				outbound.Put(field.name, field.content)
				continue
			}
			nested, err := decodeSmartPolicyObject(field.content)
			if err != nil {
				return "", fmt.Errorf("decode config smart %s: %w", field.name, err)
			}
			for _, key := range field.keys {
				removeSmartPolicyField(nested, key)
			}
			content, err := json.Marshal(nested)
			if err != nil {
				return "", err
			}
			outbound.Put(field.name, json.RawMessage(content))
		}
		if err = mergeSmartPolicy(outbound, override); err != nil {
			return "", err
		}
		envelope.Outbounds[index], err = json.Marshal(outbound)
		if err != nil {
			return "", err
		}
	}
	content, err := json.Marshal(envelope.Outbounds)
	if err != nil {
		return "", err
	}
	root.Put("outbounds", json.RawMessage(content))
	content, err = json.Marshal(root)
	return string(content), err
}

func removeSmartPolicyField(object *badjson.JSONObject, name string) bool {
	found := false
	for _, key := range object.Keys() {
		if strings.EqualFold(key, name) {
			object.Remove(key)
			found = true
		}
	}
	return found
}

func mergeSmartPolicy(destination, override *badjson.JSONObject) error {
	for _, entry := range override.Entries() {
		value := entry.Value.(json.RawMessage)
		if bytes.HasPrefix(bytes.TrimSpace(value), []byte("{")) {
			nested, err := decodeSmartPolicyObject(value)
			if err != nil {
				return err
			}
			existing := new(badjson.JSONObject)
			if content, exists := destination.Get(entry.Key); exists && !bytes.Equal(bytes.TrimSpace(content.(json.RawMessage)), []byte("null")) {
				existing, err = decodeSmartPolicyObject(content.(json.RawMessage))
				if err != nil {
					return fmt.Errorf("decode config smart %s: %w", entry.Key, err)
				}
			}
			if err = mergeSmartPolicy(existing, nested); err != nil {
				return err
			}
			content, err := json.Marshal(existing)
			if err != nil {
				return err
			}
			destination.Put(entry.Key, json.RawMessage(content))
		} else {
			destination.Put(entry.Key, value)
		}
	}
	return nil
}

// Raw values preserve untouched ordering, number precision, and empty objects.
func decodeSmartPolicyObject(content []byte) (*badjson.JSONObject, error) {
	decoder := json.NewDecoder(bytes.NewReader(content))
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if token != json.Delim('{') {
		return nil, fmt.Errorf("expected an object")
	}
	object := new(badjson.JSONObject)
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return nil, err
		}
		key := token.(string)
		var value json.RawMessage
		if err = decoder.Decode(&value); err != nil {
			return nil, err
		}
		object.Remove(key)
		object.Put(key, value)
	}
	if _, err = decoder.Token(); err != nil {
		return nil, err
	}
	if _, err = decoder.Token(); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("unexpected value after object")
		}
		return nil, err
	}
	return object, nil
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
