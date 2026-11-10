package group

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	N "github.com/sagernet/sing/common/network"
)

var errSmartDegradationNoData = errors.New("smart degradation measurement has insufficient sustained evidence")

func smartDegradationConfirmationCap(targetMbps float64) int64 {
	if targetMbps <= 0 || math.IsNaN(targetMbps) || math.IsInf(targetMbps, 0) {
		return 0
	}
	megabytes := math.Ceil((1_000_000 + targetMbps*1_000_000/8*2.5) * 1.12 / 1_000_000)
	return int64(min(megabytes, 25)) * 1_000_000
}

func smartDegradationQuota(targetMbps float64) (int64, error) {
	if targetMbps <= 0 || math.IsNaN(targetMbps) || math.IsInf(targetMbps, 0) {
		return 0, errSmartDegradationNoData
	}
	quota := math.Ceil(targetMbps * 1_000_000 / 8 * 0.625)
	if quota > (25_000_000-1_000_000-16_384)/4 {
		return 0, errSmartDegradationNoData
	}
	return int64(quota), nil
}

func smartMeasureDegradation(ctx context.Context, dialer N.Dialer, downloadURL string, capBytes int64, targetMbps float64, passive func() int64, clock smartClock) (download float64, bytes int64, err error) {
	var meter *smartMeasureMeter
	defer func() {
		if meter != nil {
			func() {
				meter.mu.Lock()
				defer meter.mu.Unlock()
				used := meter.used
				// A contained connection panic may leave its granted read allowance unsettled.
				if errors.Is(err, errSmartContained) {
					used = max(used, capBytes-meter.remaining)
				}
				bytes = (used*11 + 9) / 10
			}()
		}
		if ctx.Err() != nil && !errors.Is(err, errSmartContained) {
			err = ctx.Err()
		}
		if err != nil {
			download = 0
			if !errors.Is(err, errSmartDegradationNoData) {
				err = errors.Join(errSmartDegradationNoData, err)
			}
		}
	}()
	defer smartContextRecover(ctx, "degradation-measure", &err)
	if err = ctx.Err(); err != nil {
		return
	}
	if !strings.Contains(downloadURL, "{bytes}") {
		return 0, 0, errors.New("smart download URL lacks {bytes}")
	}
	quota, quotaErr := smartDegradationQuota(targetMbps)
	if quotaErr != nil {
		return 0, 0, quotaErr
	}
	payload := 1_000_000 + 4*quota
	capBytes = min(capBytes, 25_000_000)
	if capBytes < payload+16_384 {
		return 0, 0, errSmartDegradationNoData
	}
	meter = &smartMeasureMeter{remaining: capBytes, maxDials: 2}
	metered := &smartMeasureDialer{Dialer: dialer, meter: meter}
	resolved := &smartResolvingDialer{Dialer: metered, resolve: func(ctx context.Context, host string) ([]netip.Addr, error) {
		return smartResolveMember(ctx, metered, host)
	}}
	client := smartProbeHTTPClient(resolved)
	defer client.CloseIdleConnections()
	request, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, strings.ReplaceAll(downloadURL, "{bytes}", strconv.FormatInt(payload, 10)), nil)
	if requestErr != nil {
		return 0, 0, requestErr
	}
	response, requestErr := client.Do(request)
	if requestErr != nil {
		return 0, 0, requestErr
	}
	defer func() {
		if closeErr := smartDegradationCloseBody(ctx, response.Body); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return 0, 0, fmt.Errorf("smart download HTTP status %d", response.StatusCode)
	}
	download, err = smartMeasureDegradationBody(ctx, response.Body, targetMbps, passive, clock)
	return
}

func smartDegradationCloseBody(ctx context.Context, body io.Closer) (err error) {
	defer smartContextRecover(ctx, "degradation-close", &err)
	return body.Close()
}

func smartMeasureDegradationBody(ctx context.Context, body io.Reader, targetMbps float64, passive func() int64, clock smartClock) (download float64, err error) {
	defer func() {
		if err != nil {
			download = 0
			err = errors.Join(errSmartDegradationNoData, err)
		}
	}()
	defer smartContextRecover(ctx, "degradation-body", &err)
	quota, err := smartDegradationQuota(targetMbps)
	if err != nil {
		return 0, err
	}
	if clock == nil {
		clock = smartRealClock{}
	}
	if err = ctx.Err(); err != nil {
		return 0, err
	}
	if _, err = io.CopyN(io.Discard, body, 1_000_000); err != nil {
		return 0, err
	}
	download = math.Inf(1)
	for range 4 {
		if err = ctx.Err(); err != nil {
			return 0, err
		}
		start := clock.Now()
		var passiveStart int64
		if passive != nil {
			passiveStart = passive()
		}
		n, readErr := io.CopyN(io.Discard, body, quota)
		if readErr != nil || n != quota {
			if readErr == nil {
				readErr = io.ErrUnexpectedEOF
			}
			return 0, readErr
		}
		for elapsed := clock.Now().Sub(start); elapsed < 500*time.Millisecond; elapsed = clock.Now().Sub(start) {
			timer := clock.NewTimer(500*time.Millisecond - elapsed)
			select {
			case <-ctx.Done():
				timer.Stop()
				return 0, ctx.Err()
			case <-timer.C():
				timer.Stop()
			}
		}
		var passiveBytes int64
		if passive != nil {
			passiveBytes = max(0, passive()-passiveStart)
		}
		elapsed := clock.Now().Sub(start)
		if err = ctx.Err(); err != nil {
			return 0, err
		}
		rate := (float64(n) + float64(passiveBytes)) * 8 / elapsed.Seconds() / 1_000_000
		download = min(download, rate)
		if elapsed >= 2*time.Second && rate < targetMbps/2 {
			return download, nil
		}
	}
	return download, nil
}
