package route

import (
	"context"
	"errors"
	"io"
	"net/netip"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-tun"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func TestPreMatchSmartHealthRejectContinues(t *testing.T) {
	group := &smartRouteTestGroup{tag: "smart", err: errors.New("UDP/443 unavailable")}
	router := &Router{outbound: &smartRouteTestManager{defaultOutbound: group}}
	metadata := smartRouteTestMetadata()
	result := router.preMatchFlow(context.Background(), &metadata, metadata.Destination, nil, "")
	if result.Action != adapter.PreMatchContinue || result.NewTracker != nil || group.attaches != 0 {
		t.Fatal("health rejection created a durable verdict or attached an uncommitted flow")
	}
}

func TestPreMatchSmartAttachesOnlyCommittedFlow(t *testing.T) {
	leaf := &smartRouteTestOutbound{tag: "leaf"}
	inner := &smartRouteTestGroup{tag: "inner", next: leaf, generation: 2}
	outer := &smartRouteTestGroup{tag: "outer", next: inner, generation: 1}
	selector := &smartRouteTestSelector{selected: outer.Tag()}
	original := &smartRouteTestTracker{}
	display := &smartRouteTestConnectionTracker{tracker: original}
	router := &Router{
		logger: log.NewNOPFactory().Logger(),
		outbound: &smartRouteTestManager{
			defaultOutbound: selector,
			members:         map[string]adapter.Outbound{outer.Tag(): outer},
		},
		trackers: []adapter.ConnectionTracker{display},
	}
	metadata := smartRouteTestMetadata()
	result := router.preMatchFlow(context.Background(), &metadata, metadata.Destination, nil, "")
	if result.Action != adapter.PreMatchFlow || result.Outbound != leaf {
		t.Fatal("nested groups did not select the smart leaf")
	}
	if outer.destination != metadata.Destination || inner.destination != metadata.Destination || outer.network != metadata.Network {
		t.Fatal("selection did not receive the actual network and destination")
	}
	tracker := result.NewTracker()
	if outer.attaches != 0 || inner.attaches != 0 {
		t.Fatal("tracker creation attached before the dispatcher committed")
	}
	handle := &smartRouteTestHandle{tracker: tracker}
	tracker.AttachFlow(handle)
	tracker.CountForward(21)
	tracker.CountReverse(34)
	tracker.FlowEstablished()
	tracker.CloseFlow(tun.FlowCloseFinished)
	tracker.CloseFlow(tun.FlowCloseFinished)
	if handle.closes != 0 || original.closes != 1 || original.attaches != 1 || original.forward != 21 || original.reverse != 34 || original.established != 1 || display.outbound != leaf {
		t.Fatal("existing tracker behavior or selected display snapshot changed")
	}
	for _, group := range []*smartRouteTestGroup{outer, inner} {
		if group.attaches != 1 || group.lease.closes != 1 || group.lease.read != 34 || group.lease.write != 21 {
			t.Fatal("incorrect nested smart attachment lifecycle or byte counters")
		}
		chain := group.decision.Selections()
		if len(chain) != 2 || chain[0].Generation != 1 || chain[1].Generation != 2 || group.decision.Outbound() != leaf {
			t.Fatal("attachment lost the immutable decision chain")
		}
	}
}

func TestSmartFlowTrackerHandlesLateRevocation(t *testing.T) {
	group := &smartRouteTestGroup{tag: "smart", revoke: true}
	original := &smartRouteTestTracker{}
	tracker := &smartFlowTracker{
		FlowTracker: original,
		decision:    adapter.NewSmartDecision(nil, []adapter.SmartSelection{{Group: group, Generation: 1}}),
	}
	handle := &smartRouteTestHandle{tracker: tracker}
	tracker.AttachFlow(handle)
	tracker.CloseFlow(tun.FlowCloseTimeout)
	if group.attaches != 1 || group.lease.closes != 1 || handle.closes != 1 || original.closes != 1 {
		t.Fatal("synchronous late revocation did not close and unregister exactly once")
	}
}

func smartRouteTestMetadata() adapter.InboundContext {
	return adapter.InboundContext{
		Network:     N.NetworkUDP,
		Source:      M.SocksaddrFrom(netip.MustParseAddr("192.0.2.1"), 12345),
		Destination: M.SocksaddrFrom(netip.MustParseAddr("198.51.100.1"), 443),
	}
}

type smartRouteTestGroup struct {
	adapter.OutboundGroup
	tag         string
	next        adapter.Outbound
	generation  uint64
	err         error
	revoke      bool
	attaches    int
	network     string
	destination M.Socksaddr
	decision    adapter.SmartDecision
	lease       *smartRouteTestLease
}

func (g *smartRouteTestGroup) Tag() string { return g.tag }
func (g *smartRouteTestGroup) Now() string {
	panic("smart must be selected before generic group recursion")
}

func (g *smartRouteTestGroup) SelectMember(_ context.Context, network string, destination M.Socksaddr) (adapter.SmartDecision, error) {
	g.network, g.destination = network, destination
	return adapter.NewSmartDecision(g.next, []adapter.SmartSelection{{Group: g, Member: "member", Generation: g.generation}}), g.err
}

func (g *smartRouteTestGroup) Attach(decision adapter.SmartDecision, closer io.Closer) adapter.SmartLease {
	g.attaches++
	g.decision = decision
	g.lease = &smartRouteTestLease{}
	if g.revoke {
		_ = closer.Close()
	}
	return g.lease
}

type smartRouteTestSelector struct {
	adapter.OutboundGroup
	selected string
}

func (g *smartRouteTestSelector) Now() string { return g.selected }

type smartRouteTestOutbound struct {
	adapter.FlowOutbound
	tag string
}

func (o *smartRouteTestOutbound) Tag() string       { return o.tag }
func (o *smartRouteTestOutbound) Type() string      { return "test" }
func (o *smartRouteTestOutbound) Network() []string { return []string{N.NetworkTCP, N.NetworkUDP} }
func (o *smartRouteTestOutbound) PreMatchFlow(string, netip.Addr) adapter.PreMatchAction {
	return adapter.PreMatchFlow
}

type smartRouteTestManager struct {
	adapter.OutboundManager
	defaultOutbound adapter.Outbound
	members         map[string]adapter.Outbound
}

func (m *smartRouteTestManager) Default() adapter.Outbound { return m.defaultOutbound }
func (m *smartRouteTestManager) Outbound(tag string) (adapter.Outbound, bool) {
	outbound, loaded := m.members[tag]
	return outbound, loaded
}

type smartRouteTestLease struct {
	read, write, closes int
}

func (l *smartRouteTestLease) CountRead(n int)  { l.read += n }
func (l *smartRouteTestLease) CountWrite(n int) { l.write += n }
func (l *smartRouteTestLease) Close() error {
	l.closes++
	return nil
}

type smartRouteTestHandle struct {
	tracker tun.FlowTracker
	closes  int
}

func (h *smartRouteTestHandle) CloseFlow() {
	h.closes++
	h.tracker.CloseFlow(tun.FlowCloseReset)
}

type smartRouteTestTracker struct {
	attaches, forward, reverse, established, closes int
}

func (t *smartRouteTestTracker) AttachFlow(tun.FlowHandle)     { t.attaches++ }
func (t *smartRouteTestTracker) CountForward(n int)            { t.forward += n }
func (t *smartRouteTestTracker) CountReverse(n int)            { t.reverse += n }
func (t *smartRouteTestTracker) FlowEstablished()              { t.established++ }
func (t *smartRouteTestTracker) CloseFlow(tun.FlowCloseReason) { t.closes++ }

type smartRouteTestConnectionTracker struct {
	adapter.ConnectionTracker
	tracker  tun.FlowTracker
	outbound adapter.Outbound
}

func (t *smartRouteTestConnectionTracker) RoutedFlow(_ context.Context, _ adapter.InboundContext, _ adapter.Rule, outbound adapter.Outbound) tun.FlowTracker {
	t.outbound = outbound
	return t.tracker
}
