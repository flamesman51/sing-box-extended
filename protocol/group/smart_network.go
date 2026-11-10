package group

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing/service"
)

type smartNetwork struct {
	Key, Type, Label, Carrier, Radio, Country, Fingerprint string
	Path                                                   []string
	Labels                                                 map[string]string
	Link                                                   bool
	Offset                                                 int
	PowerSave                                              bool
}

type smartNetworkService struct {
	mu       sync.Mutex
	at       time.Time
	expires  time.Time
	snapshot smartNetwork
}

var smartNetworkServiceMu sync.Mutex

func smartNetworkServiceFromContext(ctx context.Context) *smartNetworkService {
	smartNetworkServiceMu.Lock()
	defer smartNetworkServiceMu.Unlock()
	if cached := service.FromContext[*smartNetworkService](ctx); cached != nil {
		return cached
	}
	cached := new(smartNetworkService)
	if service.RegistryFromContext(ctx) != nil {
		service.MustRegister(ctx, cached)
	}
	return cached
}

// smartForceNetwork invalidates the shared snapshot on interface changes and wake.
func smartForceNetwork(ctx context.Context) {
	cached := smartNetworkServiceFromContext(ctx)
	cached.mu.Lock()
	cached.at = time.Time{}
	cached.mu.Unlock()
}

func smartReadNetwork(ctx context.Context, seed []byte, now time.Time) smartNetwork {
	cached := smartNetworkServiceFromContext(ctx)
	cached.mu.Lock()
	defer cached.mu.Unlock()
	if !cached.at.IsZero() && !now.Before(cached.at) && now.Before(cached.expires) {
		result := cached.snapshot
		result.Path = slices.Clone(result.Path)
		result.Labels = maps.Clone(result.Labels)
		return result
	}
	result := smartNetwork{Key: "other", Type: "other", Path: []string{"other"}, Link: true}
	_, result.Offset = now.In(time.Local).Zone()
	platform := service.FromContext[adapter.PlatformInterface](ctx)
	var wifi adapter.WIFIState
	var cell *adapter.CellularState
	if platform != nil {
		wifi = platform.ReadWIFIState(ctx)
		cell = platform.ReadCellularState()
		result.Country = strings.ToUpper(platform.SystemCountryCode())
		result.PowerSave = platform.IsPowerSaveMode()
		result.Offset = int(platform.SystemTimezoneOffsetSeconds())
	}
	manager := service.FromContext[adapter.NetworkManager](ctx)
	var iface *adapter.NetworkInterface
	if manager != nil {
		iface = manager.DefaultNetworkInterface()
		result.Link = iface != nil
	}
	var ifaceName string
	var ifaceIndex int
	if iface != nil {
		ifaceName, ifaceIndex = iface.Name, iface.Index
	}
	if iface != nil && platform != nil {
		result.Type = iface.Type.String()
		switch iface.Type {
		case C.InterfaceTypeWIFI:
			result.Key, result.Path = "wifi", []string{"wifi"}
			ssid := smartWIFIValue(wifi.SSID)
			bssid := smartWIFIValue(adapter.NormalizeWIFIBSSID(wifi.BSSID))
			result.Labels = make(map[string]string)
			if ssid != "" {
				result.Key = "wifi/ssid:" + smartNetworkHash(seed, ssid)
				result.Label = ssid
				result.Path = append([]string{result.Key}, result.Path...)
				result.Labels[result.Key] = ssid
			}
			if bssid != "" {
				result.Key = "wifi/bssid:" + smartNetworkHash(seed, bssid)
				result.Label = strings.TrimSpace(ssid + " " + bssid)
				result.Path = append([]string{result.Key}, result.Path...)
				result.Labels[result.Key] = bssid
			}
		case C.InterfaceTypeCellular:
			result.Key, result.Path = "cell", []string{"cell"}
			if cell != nil {
				result.Carrier, result.Radio = cell.Carrier, cell.RadioType
				if smartDigits(cell.MCC) && smartDigits(cell.MNC) {
					result.Key += "/" + cell.MCC + cell.MNC
					result.Path = append([]string{result.Key}, result.Path...)
					if cell.RadioType != "" && !strings.ContainsAny(cell.RadioType, "/:\x00") {
						result.Key += "/" + cell.RadioType
						result.Path = append([]string{result.Key}, result.Path...)
					}
				}
				result.Label = strings.TrimSpace(cell.Carrier + " " + cell.RadioType)
			}
		case C.InterfaceTypeEthernet:
			result.Key = "eth/" + iface.Name
			result.Label = iface.Name
			result.Path = []string{result.Key}
		default:
			result.Type = "other"
		}
	}
	// Tower data is deliberately absent: only path/carrier/BSSID changes revoke evidence.
	var bssid string
	if result.Type == "wifi" {
		bssid = smartWIFIValue(adapter.NormalizeWIFIBSSID(wifi.BSSID))
	}
	fingerprint := fmt.Sprintf("%s\x00%d\x00%s\x00%s\x00%s", ifaceName, ifaceIndex, result.Type, result.Carrier, bssid)
	if cell != nil && result.Type == "cellular" {
		fingerprint += "\x00" + cell.MCC + cell.MNC
	}
	result.Fingerprint = smartNetworkHash(seed, fingerprint)
	result = smartNetworkWithTower(result, seed, cell, now)
	cached.at, cached.snapshot = now, result
	cached.expires = now.Add(30 * time.Second)
	if cell != nil && strings.Contains(result.Key, "/tower:") {
		if expiry := cell.ObservedAt.Add(120 * time.Second); expiry.Before(cached.expires) {
			cached.expires = expiry
		}
	}
	result.Path = slices.Clone(result.Path)
	result.Labels = maps.Clone(result.Labels)
	return result
}

func smartNetworkHash(seed []byte, value string) string {
	h := hmac.New(sha256.New, seed)
	_, _ = h.Write([]byte(value))
	return hex.EncodeToString(h.Sum(nil)[:8])
}

func smartWIFIValue(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "<unknown ssid>", "02:00:00:00:00:00", "00:00:00:00:00:00":
		return ""
	default:
		return value
	}
}

func smartDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func smartNetworkWithTower(network smartNetwork, seed []byte, cell *adapter.CellularState, now time.Time) smartNetwork {
	if network.Type != "cellular" || cell == nil || cell.AreaCode == "" || cell.CellID == "" || cell.ObservedAt.IsZero() || now.Before(cell.ObservedAt) || now.Sub(cell.ObservedAt) >= 120*time.Second {
		return network
	}
	key := network.Key + "/tower:" + smartNetworkHash(seed, cell.AreaCode+"\x00"+cell.CellID)
	network.Labels = maps.Clone(network.Labels)
	if network.Labels == nil {
		network.Labels = make(map[string]string)
	}
	network.Labels[key] = cell.AreaCode + "/" + cell.CellID
	network.Path = append([]string{key}, network.Path...)
	network.Key = key
	network.Label = strings.TrimSpace(network.Label + " " + cell.AreaCode + "/" + cell.CellID)
	return network
}
