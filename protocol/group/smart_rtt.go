package group

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// smartMeasurementObservation belongs to one completed burst or scheduled cohort, not its rolling history.
type smartMeasurementObservation struct {
	Value   float64
	At      time.Time
	Samples int
}

type smartRTTProbeResult struct {
	Samples    []float64
	BurstSizes []int
}

// smartSTUNProbe keeps burst boundaries so partial replies cannot manufacture a jitter cohort.
func smartSTUNProbe(ctx context.Context, dialer N.Dialer, bursts int) (smartRTTProbeResult, error) {
	var result smartRTTProbeResult
	var failures []error
	for index := range bursts {
		if index > 0 {
			timer := time.NewTimer(time.Second)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return result, ctx.Err()
			}
		}
		samples, err := smartSTUNBurst(ctx, dialer)
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if errors.Is(err, errSmartContained) {
			return smartRTTProbeResult{}, err
		}
		result.Samples = append(result.Samples, samples...)
		result.BurstSizes = append(result.BurstSizes, len(samples))
		if err != nil {
			failures = append(failures, err)
		}
	}
	if len(result.Samples) == 0 {
		return result, errors.Join(failures...)
	}
	return result, nil
}

func smartSTUNBurst(ctx context.Context, dialer N.Dialer) ([]float64, error) {
	burst, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	hosts := [...]string{"stun.cloudflare.com", "stun.l.google.com"}
	ports := [...]uint16{3478, 19302}
	var destinations [2]M.Socksaddr
	count := 0
	var failures []error
	for index, host := range hosts {
		attempt, finish := smartProbeAttemptContext(burst, len(hosts)-index)
		addresses, err := smartResolveMember(attempt, dialer, host)
		finish()
		if err == nil && len(addresses) > 0 {
			destinations[count] = M.SocksaddrFrom(addresses[0], ports[index])
			count++
		} else if err != nil {
			failures = append(failures, err)
		}
	}
	if count == 0 {
		return nil, fmt.Errorf("%w: %w", errSmartResolve, errors.Join(failures...))
	}
	var replies [5]float64
	var valid [5]bool
	var probeErrors [5]error
	var workers sync.WaitGroup
	started := time.Now()
	for index := range replies {
		if delay := time.Until(started.Add(time.Duration(index) * 200 * time.Millisecond)); delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-burst.Done():
				timer.Stop()
			}
		}
		if burst.Err() != nil {
			break
		}
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			defer smartContextRecover(burst, "probe/udp-burst", &probeErrors[index])
			rtt, err := smartUDPExchange(burst, dialer, destinations[index%count], false)
			probeErrors[index] = err
			if err == nil {
				replies[index], valid[index] = rtt.Seconds()*1000, true
			}
		}(index)
	}
	workers.Wait()
	for _, err := range probeErrors {
		if errors.Is(err, errSmartContained) {
			return nil, err
		}
	}
	samples := make([]float64, 0, 5)
	for index, value := range replies {
		if valid[index] {
			samples = append(samples, value)
		}
	}
	if len(samples) > 0 {
		return samples, nil
	}
	if burst.Err() != nil {
		return nil, burst.Err()
	}
	return nil, errors.Join(probeErrors[:]...)
}

func smartAppendRTTs(previous, samples []float64) []float64 {
	previous = append(previous, samples...)
	if len(previous) > 64 {
		copy(previous, previous[len(previous)-64:])
		previous = previous[:64]
	}
	return previous
}

func (s *Smart) applyRTTMeasurementLocked(member *smartMember, samples []float64, burstSizes []int, now time.Time) {
	if len(member.udpRTTAt) < len(member.udpRTTs) {
		member.udpRTTAt = append(member.udpRTTAt, make([]time.Time, len(member.udpRTTs)-len(member.udpRTTAt))...)
	}
	for range samples {
		member.udpRTTAt = append(member.udpRTTAt, now)
	}
	if len(member.udpRTTAt) > 64 {
		copy(member.udpRTTAt, member.udpRTTAt[len(member.udpRTTAt)-64:])
		member.udpRTTAt = member.udpRTTAt[:64]
	}
	member.udpRTTs = smartAppendRTTs(member.udpRTTs, samples)
	at := 0
	for _, count := range burstSizes {
		if count < 0 || count > 5 || at+count > len(samples) {
			return
		}
		if count >= 3 {
			median, _ := smartProfileRTTQuantiles(samples[at : at+count])
			member.udpObservation = smartMeasurementObservation{Value: median, At: now, Samples: count}
		}
		at += count
	}
	if len(burstSizes) == 2 && at == len(samples) && at >= 10 {
		median, p90 := smartProfileRTTQuantiles(samples)
		member.jitterObservation = smartMeasurementObservation{Value: p90 - median, At: now, Samples: at}
	}
}

func smartFreshRTTSamples(member *smartMember, now time.Time, buffer []float64) []float64 {
	samples := buffer[:0]
	for index, value := range member.udpRTTs {
		if index >= len(member.udpRTTAt) {
			break
		}
		at := member.udpRTTAt[index]
		if !at.IsZero() && now.Sub(at) < 6*time.Hour {
			samples = append(samples, value)
		}
	}
	return samples
}

func smartSpeedBucket(value float64, fresh bool) int {
	if !fresh {
		return math.MaxInt
	}
	if value <= 0 {
		return 1 << 30
	}
	return -int(math.Floor(math.Log(value) / math.Log(1.25)))
}

func smartFreshUpload(member *smartMember, now time.Time) (float64, bool) {
	value := member.throughput.UploadResult
	at := value.At
	return value.Value, value.Valid && !at.IsZero() && now.Sub(at) < 24*time.Hour && smartFinite(value.Value) && value.Value >= 0
}

func smartRTTBucket(samples []float64, criterion string) int {
	minimum, width := 5, 10.0
	if criterion == "jitter" {
		minimum, width = 10, 5
	}
	if len(samples) < minimum {
		return math.MaxInt
	}
	median, p90 := smartProfileRTTQuantiles(samples)
	if criterion == "jitter" {
		median = p90 - median
	}
	return int(math.Floor(median / width))
}

func (s *Smart) profileCriterionFreshLocked(member *smartMember, criterion string, now time.Time) bool {
	switch criterion {
	case "download", "speed_floor", "throughput":
		_, fresh := s.effectiveDownloadLocked(member, now)
		return fresh
	case "upload":
		_, fresh := smartFreshUpload(member, now)
		return fresh
	case "rtt":
		return member.udpObservation.Samples >= 3 && !member.udpObservation.At.IsZero() && now.Sub(member.udpObservation.At) < 30*time.Minute && member.udp.value(now) == "ok"
	case "jitter":
		return member.jitterObservation.Samples >= 10 && !member.jitterObservation.At.IsZero() && now.Sub(member.jitterObservation.At) < 30*time.Minute && member.udp.value(now) == "ok"
	default:
		return !smartCriterionObservation(member, criterion).IsZero()
	}
}

func (s *Smart) profileObservationBetterLocked(candidate, primary *smartMember, criterion string, now time.Time) bool {
	if !s.profileCriterionFreshLocked(candidate, criterion, now) {
		return false
	}
	switch criterion {
	case "rtt", "jitter":
		observation, width := candidate.udpObservation, 10.0
		if criterion == "jitter" {
			observation, width = candidate.jitterObservation, 5
		}
		var buffer [64]float64
		return int(math.Floor(observation.Value/width)) < smartRTTBucket(smartFreshRTTSamples(primary, now, buffer[:]), criterion)
	case "speed_floor":
		value, fresh := s.effectiveDownloadLocked(candidate, now)
		if !fresh {
			return false
		}
		candidateFloor, primaryFloor := value >= s.targetMbps, s.meetsFloorLocked(primary, now)
		if candidateFloor != primaryFloor {
			return candidateFloor
		}
		if candidateFloor {
			return false
		}
		primaryValue, primaryFresh := s.effectiveDownloadLocked(primary, now)
		return smartSpeedBucket(value, true) < smartSpeedBucket(primaryValue, primaryFresh) && (!primaryFresh || value >= s.preference.speedMargin*primaryValue)
	case "download", "upload", "throughput":
		better, decisive := s.betterLocked(candidate, primary, now)
		return better && decisive == criterion
	default:
		// History-fed criteria use the newly recomputed estimate, not one probe's value.
		s.rankProfileMemberLocked(candidate, now)
		s.rankProfileMemberLocked(primary, now)
		return s.profileCriterionCompareLocked(candidate, primary, criterion, now) < 0
	}
}

func (s *Smart) smartConfirmationPlanLocked(member *smartMember) smartMeasurementPlan {
	if member == nil {
		return smartMeasurementPlan{}
	}
	switch member.improvementCriterion {
	case "download", "speed_floor", "throughput":
		phase := s.downloadPhaseLocked()
		return smartMeasurementPlan{Download: &phase}
	case "upload":
		if s.options.Throughput.Upload != nil && !*s.options.Throughput.Upload {
			return smartMeasurementPlan{}
		}
		phase := s.uploadPhaseLocked()
		return smartMeasurementPlan{Upload: &phase}
	case "rtt":
		return smartMeasurementPlan{STUNBursts: 1}
	case "jitter":
		return smartMeasurementPlan{STUNBursts: 2}
	default:
		return smartMeasurementPlan{}
	}
}
