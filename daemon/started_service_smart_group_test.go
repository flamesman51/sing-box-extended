package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
)

type smartTestOutbound struct {
	adapter.Outbound
}

func (o *smartTestOutbound) Tag() string  { return "member" }
func (o *smartTestOutbound) Type() string { return "direct" }

type smartTestGroup struct {
	adapter.Outbound
	tested chan struct{}
}

func (g *smartTestGroup) Tag() string   { return "auto" }
func (g *smartTestGroup) Type() string  { return "smart" }
func (g *smartTestGroup) Now() string   { return "member" }
func (g *smartTestGroup) All() []string { return []string{"member"} }

func (g *smartTestGroup) URLTest(context.Context) (map[string]uint16, error) {
	close(g.tested)
	return nil, nil
}

func (g *smartTestGroup) PerformUpdateCheck() {}

type smartTestOutboundManager struct {
	adapter.OutboundManager
	outbounds []adapter.Outbound
}

func (m *smartTestOutboundManager) Outbounds() []adapter.Outbound { return m.outbounds }

func (m *smartTestOutboundManager) Outbound(tag string) (adapter.Outbound, bool) {
	for _, outbound := range m.outbounds {
		if outbound.Tag() == tag {
			return outbound, true
		}
	}
	return nil, false
}

func TestSmartGroupPresentationAndURLTest(t *testing.T) {
	smartGroup := &smartTestGroup{tested: make(chan struct{})}
	manager := &smartTestOutboundManager{outbounds: []adapter.Outbound{smartGroup, &smartTestOutbound{}}}
	history := urltest.NewHistoryStorage()
	t.Cleanup(func() { _ = history.Close() })
	checkedAt := time.Unix(1700000000, 0)
	history.StoreURLTestHistory("member", &adapter.URLTestHistory{Time: checkedAt, Delay: 42})
	s := &StartedService{
		serviceStatus: &ServiceStatus{Status: ServiceStatus_STARTED},
		instance: &Instance{
			ctx:                   context.Background(),
			outboundManager:       manager,
			urlTestHistoryStorage: history,
		},
	}
	groups := s.readGroups().Group
	if len(groups) != 1 {
		t.Fatalf("got %d groups, want one", len(groups))
	}
	got := groups[0]
	if got.Type != "smart" || got.Selectable || got.Selected != "member" || len(got.Items) != 1 {
		t.Fatalf("unexpected smart presentation: %v", got)
	}
	if item := got.Items[0]; item.UrlTestTime != checkedAt.Unix() || item.UrlTestDelay != 42 {
		t.Fatalf("unexpected RTT presentation: %v", item)
	}
	_, err := s.URLTest(context.Background(), &URLTestRequest{OutboundTag: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-smartGroup.tested:
	case <-time.After(time.Second):
		t.Fatal("smart URLTestGroup was not called")
	}
}
