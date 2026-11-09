package libbox

import (
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
)

type cellularStatePlatform struct {
	PlatformInterface
	state *CellularState
}

func (p *cellularStatePlatform) ReadCellularState() *CellularState {
	return p.state
}

func TestReadCellularState(t *testing.T) {
	for _, test := range []struct {
		name  string
		state *CellularState
	}{
		{name: "not cellular"},
		{name: "unknown timestamp", state: &CellularState{}},
		{
			name: "observed cell",
			state: &CellularState{
				MCC:                  "250",
				MNC:                  "01",
				Carrier:              "carrier",
				RadioType:            "NR_NSA",
				AreaCode:             "123",
				CellID:               "456",
				ObservedAtUnixMillis: 1700000000123,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			wrapper := &platformInterfaceWrapper{iif: &cellularStatePlatform{state: test.state}}
			got := wrapper.ReadCellularState()
			if test.state == nil {
				if got != nil {
					t.Fatalf("got %v, want nil", got)
				}
				return
			}
			want := adapter.CellularState{
				MCC:       test.state.MCC,
				MNC:       test.state.MNC,
				Carrier:   test.state.Carrier,
				RadioType: test.state.RadioType,
				AreaCode:  test.state.AreaCode,
				CellID:    test.state.CellID,
			}
			if test.state.ObservedAtUnixMillis != 0 {
				want.ObservedAt = time.Date(2023, time.November, 14, 22, 13, 20, 123000000, time.UTC)
			}
			if got == nil || got.MCC != want.MCC || got.MNC != want.MNC || got.Carrier != want.Carrier || got.RadioType != want.RadioType || got.AreaCode != want.AreaCode || got.CellID != want.CellID || !got.ObservedAt.Equal(want.ObservedAt) {
				t.Fatalf("got %+v, want %+v", got, want)
			}
		})
	}
}

func TestSmartPlatformWithoutImplementation(t *testing.T) {
	wrapper := &platformInterfaceWrapper{}
	if wrapper.ReadCellularState() != nil || wrapper.SystemCountryCode() != "" || wrapper.IsPowerSaveMode() || wrapper.SystemTimezoneOffsetSeconds() != 0 {
		t.Fatal("missing platform must return zero values")
	}
}

func TestSmartProxyDisplayType(t *testing.T) {
	if got := ProxyDisplayType("smart"); got != "Smart" {
		t.Fatalf("got %q, want Smart", got)
	}
}
