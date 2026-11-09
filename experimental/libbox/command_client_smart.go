package libbox

import (
	"context"

	"github.com/sagernet/sing-box/daemon"

	"google.golang.org/protobuf/types/known/emptypb"
)

// GetSmartStatus returns the versioned status JSON; an empty tag selects the first smart group.
func (c *CommandClient) GetSmartStatus(tag string) (string, error) {
	return callWithResult(c, func(ctx context.Context, client daemon.StartedServiceClient) (string, error) {
		response, err := client.GetSmartStatus(ctx, &daemon.GetSmartStatusRequest{Tag: tag})
		if err != nil {
			return "", err
		}
		return response.GetJson(), nil
	})
}

// GetSmartNetworks returns per-network history JSON; an empty tag selects the first smart group.
func (c *CommandClient) GetSmartNetworks(tag string) (string, error) {
	return callWithResult(c, func(ctx context.Context, client daemon.StartedServiceClient) (string, error) {
		response, err := client.GetSmartNetworks(ctx, &daemon.GetSmartNetworksRequest{Tag: tag})
		if err != nil {
			return "", err
		}
		return response.GetJson(), nil
	})
}

// ExportSmartHistory returns history JSON. Raw exports include private network labels.
func (c *CommandClient) ExportSmartHistory(raw bool) (string, error) {
	return callWithResult(c, func(ctx context.Context, client daemon.StartedServiceClient) (string, error) {
		response, err := client.ExportSmartHistory(ctx, &daemon.ExportSmartHistoryRequest{Raw: raw})
		if err != nil {
			return "", err
		}
		return response.GetJson(), nil
	})
}

// ImportSmartHistory imports history JSON with trust in [0, 0.5] and returns the import report JSON.
func (c *CommandClient) ImportSmartHistory(data string, trust float64) (string, error) {
	return callWithResult(c, func(ctx context.Context, client daemon.StartedServiceClient) (string, error) {
		response, err := client.ImportSmartHistory(ctx, &daemon.ImportSmartHistoryRequest{Data: data, Trust: trust})
		if err != nil {
			return "", err
		}
		return response.GetJson(), nil
	})
}

// ResetSmartHistory clears learned history, not live health or budgets; an empty tag resets all smart groups.
func (c *CommandClient) ResetSmartHistory(tag string) error {
	_, err := callWithResult(c, func(ctx context.Context, client daemon.StartedServiceClient) (*emptypb.Empty, error) {
		return client.ResetSmartHistory(ctx, &daemon.ResetSmartHistoryRequest{Tag: tag})
	})
	return err
}
