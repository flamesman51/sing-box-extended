package group

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing/common/control"
	"github.com/sagernet/sing/service"
)

type smartNetworkTestPlatform struct {
	adapter.PlatformInterface
	wifi  adapter.WIFIState
	cell  *adapter.CellularState
	reads int
}

func (p *smartNetworkTestPlatform) ReadWIFIState(context.Context) adapter.WIFIState {
	p.reads++
	return p.wifi
}
func (p *smartNetworkTestPlatform) ReadCellularState() *adapter.CellularState { return p.cell }
func (p *smartNetworkTestPlatform) SystemCountryCode() string                 { return "RU" }
func (p *smartNetworkTestPlatform) IsPowerSaveMode() bool                     { return false }
func (p *smartNetworkTestPlatform) SystemTimezoneOffsetSeconds() int32        { return 10800 }

type smartNetworkTestManager struct {
	adapter.NetworkManager
	iface *adapter.NetworkInterface
}

func (m *smartNetworkTestManager) DefaultNetworkInterface() *adapter.NetworkInterface { return m.iface }

func TestSmartNetworkPermissionFallbackAndSharedCache(t *testing.T) {
	p := &smartNetworkTestPlatform{wifi: adapter.WIFIState{SSID: "<unknown ssid>", BSSID: "02:00:00:00:00:00"}}
	m := &smartNetworkTestManager{iface: &adapter.NetworkInterface{Interface: control.Interface{Name: "wlan0", Index: 2}, Type: C.InterfaceTypeWIFI}}
	ctx := service.ContextWith[adapter.PlatformInterface](context.Background(), p)
	ctx = service.ContextWith[adapter.NetworkManager](ctx, m)
	now := time.Unix(1800000000, 0)
	first := smartReadNetwork(ctx, []byte("seed"), now)
	if first.Key != "wifi" || first.Country != "RU" || !first.Link {
		t.Fatalf("unexpected snapshot: %+v", first)
	}
	first.Path[0] = "mutated"
	second := smartReadNetwork(ctx, []byte("seed"), now.Add(time.Second))
	if p.reads != 1 || second.Path[0] != "wifi" {
		t.Fatal("snapshot is not shared or immutable")
	}
	p.wifi = adapter.WIFIState{SSID: "home", BSSID: "aa:bb:cc:dd:ee:ff"}
	smartForceNetwork(ctx)
	third := smartReadNetwork(ctx, []byte("seed"), now.Add(2*time.Second))
	if p.reads != 2 || !strings.HasPrefix(third.Key, "wifi/bssid:") || strings.Contains(third.Key, "aa:bb") || third.Fingerprint == second.Fingerprint {
		t.Fatalf("invalid precise snapshot: %+v", third)
	}
	if len(third.Path) != 3 || third.Path[0] != third.Key || !strings.HasPrefix(third.Path[1], "wifi/ssid:") || third.Path[2] != "wifi" {
		t.Fatalf("wrong hierarchy: %v", third.Path)
	}
	if third.Labels[third.Path[0]] != "aa:bb:cc:dd:ee:ff" || third.Labels[third.Path[1]] != "home" {
		t.Fatalf("raw labels missing: %v", third.Labels)
	}
	third.Labels[third.Key] = "mutated"
	if cached := smartReadNetwork(ctx, []byte("seed"), now.Add(3*time.Second)); cached.Labels[cached.Key] != "aa:bb:cc:dd:ee:ff" {
		t.Fatal("label map mutation escaped snapshot")
	}
}

func TestSmartNetworkTowerOnlyChangesAttribution(t *testing.T) {
	now := time.Unix(1800000000, 0)
	p := &smartNetworkTestPlatform{cell: &adapter.CellularState{MCC: "250", MNC: "01", RadioType: "LTE", Carrier: "carrier", AreaCode: "10", CellID: "20", ObservedAt: now}}
	m := &smartNetworkTestManager{iface: &adapter.NetworkInterface{Interface: control.Interface{Name: "rmnet0"}, Type: C.InterfaceTypeCellular}}
	ctx := service.ContextWith[adapter.PlatformInterface](context.Background(), p)
	ctx = service.ContextWith[adapter.NetworkManager](ctx, m)
	base := smartReadNetwork(ctx, []byte("seed"), now)
	if !strings.HasPrefix(base.Key, "cell/25001/LTE/tower:") {
		t.Fatalf("tower attribution missing: %s", base.Key)
	}
	base.Key, base.Path = "cell/25001/LTE", base.Path[1:]
	first := smartNetworkWithTower(base, []byte("seed"), p.cell, now)
	p.cell.CellID = "21"
	second := smartNetworkWithTower(base, []byte("seed"), p.cell, now)
	if first.Key == second.Key || first.Fingerprint != second.Fingerprint {
		t.Fatal("tower changed generation or failed attribution")
	}
	if first.Labels[first.Key] != "10/20" || second.Labels[second.Key] != "10/21" {
		t.Fatal("raw tower labels missing")
	}
	stale := smartNetworkWithTower(base, []byte("seed"), p.cell, now.Add(121*time.Second))
	if stale.Key != base.Key {
		t.Fatal("stale tower retained")
	}
	p.cell.AreaCode = ""
	if got := smartNetworkWithTower(base, []byte("seed"), p.cell, now); got.Key != base.Key {
		t.Fatal("permission denial retained tower")
	}
}

func TestSmartNetworkNilPlatform(t *testing.T) {
	n := smartReadNetwork(context.Background(), nil, time.Now())
	if n.Key != "other" || n.Type != "other" || len(n.Path) != 1 {
		t.Fatalf("invalid coarse network: %+v", n)
	}
}

func TestSmartNetworkWithoutPlatformUsesOtherBudget(t *testing.T) {
	for _, interfaceType := range []C.InterfaceType{C.InterfaceTypeWIFI, C.InterfaceTypeEthernet, C.InterfaceTypeCellular} {
		t.Run(interfaceType.String(), func(t *testing.T) {
			manager := &smartNetworkTestManager{iface: &adapter.NetworkInterface{Interface: control.Interface{Name: "link", Index: 1}, Type: interfaceType}}
			ctx := service.ContextWith[adapter.NetworkManager](context.Background(), manager)
			now := time.Unix(1800000000, 0)
			network := smartReadNetwork(ctx, nil, now)
			if network.Type != "other" || network.Key != "other" || len(network.Path) != 1 || network.Path[0] != "other" || !network.Link {
				t.Fatalf("network without platform was classified by interface type: %+v", network)
			}
			ledger, err := getSmartLedger(&smartLedgerTestCache{})
			if err != nil {
				t.Fatal(err)
			}
			ledger.Register("test", map[string]float64{"wifi": 300, "cellular": 50, "other": 7})
			if budget := ledger.Snapshot(network.Type, now, network.Offset); budget.LimitMB != 7 {
				t.Fatalf("wrong network budget: %+v", budget)
			}
		})
	}
}
