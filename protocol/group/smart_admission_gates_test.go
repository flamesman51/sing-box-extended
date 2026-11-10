package group

import (
	"context"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/service"
)

func TestSmartDelayedAdmissionRechecksActiveTestGates(t *testing.T) {
	for _, gate := range []string{"power-save", "snapshot-power-save", "pause", "disabled", "budget", "link", "traffic", "health"} {
		t.Run(gate, func(t *testing.T) {
			s, clock := smartTestGroup()
			cache := new(smartLedgerBlockingCache)
			ledger, err := getSmartLedger(cache)
			if err != nil {
				t.Fatal(err)
			}
			ledger.Register(s.Tag(), map[string]float64{"wifi": 100})
			platform := new(smartAdmissionCountingPlatform)
			s.ctx = service.ContextWith[adapter.PlatformInterface](s.ctx, platform)
			s.ledger, s.throughputEnabled, s.generationSucceeded = ledger, true, true
			blocked := cache.block(t)
			s.startThroughputAdmissionLocked(s.primary, clock.Now())
			smartLedgerWait(t, blocked.entered)
			switch gate {
			case "power-save":
				platform.power.Store(true)
			case "snapshot-power-save":
				s.network.PowerSave = true
			case "pause":
				s.paused = true
			case "disabled":
				s.throughputEnabled = false
			case "budget":
				ledger.Register(s.Tag(), map[string]float64{"wifi": 0})
			case "link":
				s.network.Link = false
			case "traffic":
				lease := &smartLease{member: s.primary, group: s, lastRead: clock.Now(), lastWrite: clock.Now()}
				s.leases[s.primary] = map[*smartLease]struct{}{lease: {}}
			case "health":
				s.primary.state = "suspect"
			}
			blocked.unblock()
			result := smartLedgerWait(t, s.results)
			s.applyResultLocked(result, clock.Now())
			s.admissionWorkers.Wait()
			s.accountingWorkers.Wait()
			if len(s.pending) != 0 || s.primary.queued["throughput"] {
				t.Fatalf("delayed admission ignored the new %s gate", gate)
			}
			if snapshot := ledger.Snapshot("wifi", clock.Now(), 0); snapshot.UsedMB != 0 || snapshot.ReservedMB != 0 {
				t.Fatalf("rejected admission was not refunded at zero bytes: %+v", snapshot)
			}
			platform.power.Store(false)
			ledger.Register(s.Tag(), map[string]float64{"wifi": 100})
			next, _, err := ledger.Reserve(context.Background(), "wifi", clock.Now().Add(time.Minute), 0)
			if err != nil {
				t.Fatal(err)
			}
			if err = ledger.Reconcile(context.Background(), next, 0, false); err != nil {
				t.Fatal(err)
			}
			// No fake lease closer is registered with shutdown.
			s.leases = make(map[*smartMember]map[*smartLease]struct{})
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
