package parser

import (
	"context"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/parser/link"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/service"
)

func RegisterOutbound(registry *outbound.Registry) {
	outbound.Register[option.ParserOutboundOptions](registry, C.TypeParser, NewOutbound)
}

func NewOutbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.ParserOutboundOptions) (adapter.Outbound, error) {
	if options.Link == "" {
		return nil, E.New("missing link")
	}
	outboundOptions, err := link.ParseSubscriptionLink(options.Link)
	if err != nil {
		return nil, err
	}
	if dialerOptions, ok := outboundOptions.Options.(option.DialerOptionsWrapper); ok {
		dialerOptions.ReplaceDialerOptions(options.DialerOptions)
	}
	outboundRegistry := service.FromContext[adapter.OutboundRegistry](ctx)
	outbound, err := outboundRegistry.UnsafeCreateOutbound(ctx, router, logger, tag, outboundOptions.Type, outboundOptions.Options)
	if err != nil {
		return nil, err
	}
	return outbound, nil
}
