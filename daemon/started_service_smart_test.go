package daemon

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/sagernet/sing-box/protocol/group"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSmartRPCError(t *testing.T) {
	for _, test := range []struct {
		name    string
		err     error
		code    codes.Code
		message string
	}{
		{"success", nil, codes.OK, ""},
		{"not found", group.ErrSmartNotFound, codes.NotFound, "not_found"},
		{"no group", group.ErrSmartNoGroup, codes.NotFound, "no_smart_group"},
		{"invalid", group.ErrSmartInvalid, codes.InvalidArgument, "invalid"},
		{"too large", group.ErrSmartTooLarge, codes.ResourceExhausted, "too_large"},
		{"unsupported version", group.ErrSmartUnsupportedVersion, codes.InvalidArgument, "unsupported_version"},
		{"wrapped", fmt.Errorf("decode: %w", group.ErrSmartInvalid), codes.InvalidArgument, "invalid"},
		{"unexpected", errors.New("private details"), codes.Internal, "internal"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := status.Convert(smartRPCError(test.err))
			if got.Code() != test.code || got.Message() != test.message {
				t.Fatalf("got %s/%q, want %s/%q", got.Code(), got.Message(), test.code, test.message)
			}
		})
	}
}

func TestSmartCommandsNotRunning(t *testing.T) {
	s := &StartedService{serviceStatus: &ServiceStatus{Status: ServiceStatus_IDLE}}
	ctx := context.Background()
	checks := []struct {
		name string
		call func() error
	}{
		{"status", func() error { _, err := s.GetSmartStatus(ctx, &GetSmartStatusRequest{}); return err }},
		{"networks", func() error { _, err := s.GetSmartNetworks(ctx, &GetSmartNetworksRequest{}); return err }},
		{"export", func() error { _, err := s.ExportSmartHistory(ctx, &ExportSmartHistoryRequest{}); return err }},
		{"import", func() error { _, err := s.ImportSmartHistory(ctx, &ImportSmartHistoryRequest{}); return err }},
		{"reset", func() error { _, err := s.ResetSmartHistory(ctx, &ResetSmartHistoryRequest{}); return err }},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			got := status.Convert(check.call())
			if got.Code() != codes.FailedPrecondition || got.Message() != "not_running" {
				t.Fatalf("got %s/%q, want FailedPrecondition/not_running", got.Code(), got.Message())
			}
		})
	}
}
