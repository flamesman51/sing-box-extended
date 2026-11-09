package parser

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/parser/link"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

type smartIdentityTestOutbound struct {
	outbound.Adapter
	N.Dialer
}

func TestParserPreservesConcreteSmartIdentity(t *testing.T) {
	registry := outbound.NewRegistry()
	RegisterOutbound(registry)
	outbound.Register(registry, C.TypeTrojan, func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ option.TrojanOutboundOptions) (adapter.Outbound, error) {
		return &smartIdentityTestOutbound{Adapter: outbound.NewAdapter(C.TypeTrojan, tag, nil, nil)}, nil
	})
	ctx := service.ContextWith[adapter.OutboundRegistry](context.Background(), registry)
	var previousServer string
	for _, server := range []string{"first.example", "second.example"} {
		options := &option.ParserOutboundOptions{Link: "trojan://password@" + server + ":443"}
		member, err := registry.CreateOutbound(ctx, nil, nil, server, C.TypeParser, options)
		if err != nil {
			t.Fatal(err)
		}
		concrete, err := link.ParseSubscriptionLink(options.Link)
		if err != nil {
			t.Fatal(err)
		}
		expectedLocal, expectedServer, err := adapter.ComputeSmartIdentity(concrete.Type, concrete.Options)
		if err != nil {
			t.Fatal(err)
		}
		local, serverIdentity, err := registry.SmartIdentity(member)
		if err != nil || local != expectedLocal || serverIdentity != expectedServer {
			t.Fatalf("parser replaced concrete identity: local=%q server=%q err=%v", local, serverIdentity, err)
		}
		if serverIdentity == previousServer {
			t.Fatal("different parser servers share a server identity")
		}
		previousServer = serverIdentity
	}
}
