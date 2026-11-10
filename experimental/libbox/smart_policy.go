package libbox

import (
	"github.com/sagernet/sing-box/protocol/group"
	"github.com/sagernet/sing/common/json"
)

// SmartProfiles returns the built-in selection profiles as a fully resolved JSON array.
func SmartProfiles() (string, error) {
	profiles, err := group.SmartProfiles()
	if err != nil {
		return "", err
	}
	content, err := json.Marshal(profiles)
	return string(content), err
}

// CheckSmartPolicy validates a selection policy accepted by OverrideOptions.SmartPolicy.
func CheckSmartPolicy(json string) error {
	return group.CheckSmartPolicy(json)
}
