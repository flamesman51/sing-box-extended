package daemon

import (
	"context"
	"errors"

	"github.com/sagernet/sing-box/protocol/group"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (s *StartedService) smartInstance() (*Instance, error) {
	s.serviceAccess.RLock()
	defer s.serviceAccess.RUnlock()
	if s.serviceStatus == nil || s.serviceStatus.Status != ServiceStatus_STARTED || s.instance == nil {
		return nil, status.Error(codes.FailedPrecondition, "not_running")
	}
	return s.instance, nil
}

// smartRPCError keeps the contract's stable error identifier in the status message.
func smartRPCError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, group.ErrSmartNotFound):
		return status.Error(codes.NotFound, "not_found")
	case errors.Is(err, group.ErrSmartNoGroup):
		return status.Error(codes.NotFound, "no_smart_group")
	case errors.Is(err, group.ErrSmartInvalid):
		return status.Error(codes.InvalidArgument, "invalid")
	case errors.Is(err, group.ErrSmartTooLarge):
		return status.Error(codes.ResourceExhausted, "too_large")
	case errors.Is(err, group.ErrSmartUnsupportedVersion):
		return status.Error(codes.InvalidArgument, "unsupported_version")
	default:
		return status.Error(codes.Internal, "internal")
	}
}

func (s *StartedService) GetSmartStatus(ctx context.Context, request *GetSmartStatusRequest) (*GetSmartStatusResponse, error) {
	instance, err := s.smartInstance()
	if err != nil {
		return nil, err
	}
	data, err := group.SmartStatus(instance.ctx, instance.outboundManager, request.GetTag())
	if err != nil {
		return nil, smartRPCError(err)
	}
	return &GetSmartStatusResponse{Json: data}, nil
}

func (s *StartedService) GetSmartNetworks(ctx context.Context, request *GetSmartNetworksRequest) (*GetSmartNetworksResponse, error) {
	instance, err := s.smartInstance()
	if err != nil {
		return nil, err
	}
	data, err := group.SmartNetworks(instance.ctx, instance.outboundManager, request.GetTag())
	if err != nil {
		return nil, smartRPCError(err)
	}
	return &GetSmartNetworksResponse{Json: data}, nil
}

func (s *StartedService) ExportSmartHistory(ctx context.Context, request *ExportSmartHistoryRequest) (*ExportSmartHistoryResponse, error) {
	instance, err := s.smartInstance()
	if err != nil {
		return nil, err
	}
	data, err := group.SmartExport(instance.ctx, instance.outboundManager, request.GetRaw())
	if err != nil {
		return nil, smartRPCError(err)
	}
	return &ExportSmartHistoryResponse{Json: data}, nil
}

func (s *StartedService) ImportSmartHistory(ctx context.Context, request *ImportSmartHistoryRequest) (*ImportSmartHistoryResponse, error) {
	instance, err := s.smartInstance()
	if err != nil {
		return nil, err
	}
	data, err := group.SmartImport(instance.ctx, instance.outboundManager, request.GetData(), request.GetTrust())
	if err != nil {
		return nil, smartRPCError(err)
	}
	return &ImportSmartHistoryResponse{Json: data}, nil
}

func (s *StartedService) ResetSmartHistory(ctx context.Context, request *ResetSmartHistoryRequest) (*emptypb.Empty, error) {
	instance, err := s.smartInstance()
	if err != nil {
		return nil, err
	}
	err = group.SmartReset(instance.ctx, instance.outboundManager, request.GetTag())
	if err != nil {
		return nil, smartRPCError(err)
	}
	return &emptypb.Empty{}, nil
}
