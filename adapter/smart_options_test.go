package adapter

import (
	"encoding/json"
	"testing"

	"github.com/sagernet/sing-box/option"
)

func TestSmartOptionExplicitZeroBudgetSurvivesJSON(t *testing.T) {
	var options option.SmartOutboundOptions
	if err := json.Unmarshal([]byte(`{"outbounds":["member"],"udp":{"enabled":false},"throughput":{"enabled":false,"wifi_daily_budget_mb":0},"interrupt_on_failover":false,"notify_all_dead":false}`), &options); err != nil {
		t.Fatal(err)
	}
	if options.Throughput.WIFIDailyBudgetMB == nil || *options.Throughput.WIFIDailyBudgetMB != 0 || options.Throughput.CellularDailyBudgetMB != nil {
		t.Fatal("explicit zero budget was not distinguished from an omitted budget")
	}
	for _, value := range []*bool{options.UDP.Enabled, options.Throughput.Enabled, options.InterruptOnFailover, options.NotifyAllDead} {
		if value == nil || *value {
			t.Fatal("explicit false was not distinguished from a default")
		}
	}
	encoded, err := json.Marshal(options)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip option.SmartOutboundOptions
	if err = json.Unmarshal(encoded, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if roundTrip.Throughput.WIFIDailyBudgetMB == nil || *roundTrip.Throughput.WIFIDailyBudgetMB != 0 {
		t.Fatal("explicit zero budget was omitted when marshaling")
	}
}
