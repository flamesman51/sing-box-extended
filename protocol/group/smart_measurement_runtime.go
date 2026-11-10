package group

import (
	"context"
	"math"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/json/badoption"
	"github.com/sagernet/sing/service"
)

type smartMeasurementBackoff struct {
	until                time.Time
	remaining            int64
	downloadAt, uploadAt time.Time
	network              string
}

type smartMeasurementKey struct {
	download, upload           smartMeasurementPhase
	downloadPhases, stunBursts int
}

func (s *Smart) normalizeMeasurementOptions() {
	t := &s.options.Throughput
	if t.Streams == 0 {
		t.Streams = 1
	}
	if t.Window == 0 {
		t.Window = badoption.Duration(7 * time.Second)
	}
	if t.Warmup == 0 {
		t.Warmup = badoption.Duration(time.Second)
	}
	if t.UploadWindow == 0 {
		t.UploadWindow = badoption.Duration(7 * time.Second)
	}
	if t.UploadWarmup == 0 {
		t.UploadWarmup = badoption.Duration(time.Second)
	}
	if t.Upload == nil {
		enabled := true
		t.Upload = &enabled
	}
}

func (s *Smart) downloadPhaseLocked() smartMeasurementPhase {
	streams := s.options.Throughput.Streams
	if streams == 0 {
		streams = 1
	}
	window, warmup := time.Duration(s.options.Throughput.Window), time.Duration(s.options.Throughput.Warmup)
	if window == 0 {
		window = 7 * time.Second
	}
	if warmup == 0 {
		warmup = time.Second
	}
	return smartMeasurementPhase{Streams: streams, Window: window, Warmup: warmup}
}

func (s *Smart) uploadPhaseLocked() smartMeasurementPhase {
	phase := s.downloadPhaseLocked()
	phase.Window, phase.Warmup = time.Duration(s.options.Throughput.UploadWindow), time.Duration(s.options.Throughput.UploadWarmup)
	if phase.Window == 0 {
		phase.Window = 7 * time.Second
	}
	if phase.Warmup == 0 {
		phase.Warmup = time.Second
	}
	return phase
}

func (s *Smart) downloadMeasurementPlanLocked() smartMeasurementPlan {
	phase := s.downloadPhaseLocked()
	return smartMeasurementPlan{Download: &phase}
}

func (s *Smart) regularMeasurementPlanLocked(member *smartMember, now time.Time) smartMeasurementPlan {
	var plan smartMeasurementPlan
	if _, fresh := smartFreshDownload(member, now); !fresh || s.downloadStaleAfterRevocationLocked(member) {
		phase := s.downloadPhaseLocked()
		plan.Download = &phase
	}
	if s.options.Throughput.Upload == nil || *s.options.Throughput.Upload {
		value := member.throughput.UploadResult
		if !value.Valid || value.At.IsZero() || now.Sub(value.At) >= 24*time.Hour || member.throughput.UploadInconclusive {
			phase := s.uploadPhaseLocked()
			plan.Upload = &phase
		}
	}
	return plan
}

func smartMeasurementPlanKey(plan smartMeasurementPlan) smartMeasurementKey {
	var down, up smartMeasurementPhase
	if plan.Download != nil {
		down = *plan.Download
	}
	if plan.Upload != nil {
		up = *plan.Upload
	}
	return smartMeasurementKey{download: down, upload: up, downloadPhases: plan.DownloadPhases, stunBursts: plan.STUNBursts}
}

func smartEstimatePlan(plan smartMeasurementPlan, downloadRate, uploadRate float64) int64 {
	bytes := 0.0
	if phase := plan.Download; phase != nil {
		bytes += downloadRate * 1e6 * (phase.Warmup + phase.Window).Seconds() / 8 * float64(max(1, plan.DownloadPhases))
	}
	if phase := plan.Upload; phase != nil {
		bytes += uploadRate * 1e6 * (phase.Warmup + phase.Window).Seconds() / 8
	}
	return int64(math.Ceil(bytes * 1.25 * 1.1))
}

func (s *Smart) measurementEstimateLocked(member *smartMember, plan smartMeasurementPlan, episode *smartDegradationEpisode) int64 {
	rate := func(member *smartMember) float64 {
		target := s.targetMbps
		if target <= 0 {
			target = 25
		}
		if member.throughput.Source == "active" && !member.throughput.At.IsZero() {
			return max(target, member.throughput.Download)
		}
		return max(target, 25)
	}
	upload := 1.0
	if member.throughput.UploadResult.Valid {
		upload = member.throughput.UploadResult.Value
	}
	if episode == nil {
		return smartEstimatePlan(plan, rate(member), upload)
	}
	return smartEstimatePlan(plan, (rate(episode.primary)+rate(episode.challenger))/2, 1)
}

func (s *Smart) measurementRemainingLocked(now time.Time) int64 {
	snapshot := s.ledger.Snapshot(s.network.Type, now, s.network.Offset)
	return max(0, int64(math.Floor(snapshot.LimitMB*1e6))-int64(math.Round(snapshot.UsedMB*1e6))-int64(math.Round(snapshot.ReservedMB*1e6)))
}

func (s *Smart) measurementPlanBackedOffLocked(member *smartMember, plan smartMeasurementPlan, now time.Time) bool {
	backoff, exists := member.measurementBackoff[smartMeasurementPlanKey(plan)]
	return exists && now.Before(backoff.until) && backoff.network == s.network.Key && backoff.remaining == s.measurementRemainingLocked(now) && backoff.downloadAt.Equal(member.throughput.At) && backoff.uploadAt.Equal(member.throughput.UploadAt)
}

func (s *Smart) backoffMeasurementPlanLocked(member *smartMember, plan smartMeasurementPlan, now time.Time) {
	local := now.In(time.FixedZone("smart-local", s.network.Offset))
	year, month, day := local.Date()
	if member.measurementBackoff == nil {
		member.measurementBackoff = make(map[smartMeasurementKey]smartMeasurementBackoff)
	}
	member.measurementBackoff[smartMeasurementPlanKey(plan)] = smartMeasurementBackoff{until: time.Date(year, month, day+1, 0, 0, 0, 0, local.Location()), remaining: s.measurementRemainingLocked(now), downloadAt: member.throughput.At, uploadAt: member.throughput.UploadAt, network: s.network.Key}
}

func (s *Smart) cancelMemberMeasurementLocked(member *smartMember) {
	if member.measurementCancel != nil {
		member.measurementCancel()
		member.measurementCancel = nil
	}
}

func (s *Smart) queueDegradationChallengerLocked(episode *smartDegradationEpisode, now time.Time) {
	member := episode.challenger
	if !s.degradationValidLocked(episode, now) || member == nil {
		s.finishDegradationLocked(now, 30*time.Minute, "inconclusive")
		return
	}
	s.queueLocked(member, "throughput", 0, false, now)
	for index := len(s.pending) - 1; index >= 0; index-- {
		job := &s.pending[index]
		if job.member == member && job.kind == "throughput" && job.reservation == "" {
			job.ctx, job.episode, job.purpose = episode.ctx, episode, smartDegradationChallenger
			job.reservation, job.capBytes, job.meter, job.plan = episode.reservation, episode.capBytes, episode.meter, episode.plan
			job.timeout = episode.plan.Deadline()
			episode.accountingWait.Add(1)
			member.degradationQueued = episode
			s.degradation.state = "checking"
			return
		}
	}
	s.finishDegradationLocked(now, 30*time.Minute, "inconclusive")
}

func (s *Smart) finishEpisodeAccountingLocked(episode *smartDegradationEpisode) {
	if episode == nil || episode.reservation == "" {
		return
	}
	episode.accountingOnce.Do(func() {
		reservation := episode.reservation
		s.accountingWorkers.Go(func() {
			episode.accountingWait.Wait()
			s.mu.Lock()
			uncertain := episode.generation != s.generation || episode.key != s.network.Key
			s.mu.Unlock()
			bytes := int64(0)
			if episode.meter != nil {
				bytes = episode.meter.chargedBytes()
			}
			if episode.accountingPanic.Load() {
				bytes = episode.capBytes
			}
			if err := s.reconcileReservation(reservation, bytes, uncertain); err != nil && s.logger != nil {
				s.logger.Error("reconcile smart degradation plan: ", err)
			}
		})
	})
}

func (s *Smart) measurementContextLocked(member *smartMember, plan smartMeasurementPlan) context.Context {
	ctx := s.workCtx
	if ctx == nil {
		ctx = s.ctx
	}
	s.cancelMemberMeasurementLocked(member)
	ctx, cancel := context.WithTimeout(ctx, plan.Deadline())
	member.measurementCancel = cancel
	return ctx
}

func (s *Smart) measurementPlanAllowedLocked(plan smartMeasurementPlan, now time.Time) bool {
	if plan.STUNBursts == 0 {
		return s.throughputAllowedLocked(now)
	}
	if s.closed || s.paused || !s.network.Link || s.network.PowerSave || s.primary == nil || !s.generationSucceeded {
		return false
	}
	if s.pauseManager != nil && s.pauseManager.IsPaused() {
		return false
	}
	if platform := service.FromContext[adapter.PlatformInterface](s.ctx); platform != nil && platform.IsPowerSaveMode() {
		return false
	}
	return true
}

func (s *Smart) measurementIdleLocked(now time.Time) bool {
	for _, entries := range s.leases {
		for lease := range entries {
			if now.Sub(lease.lastRead) < 2*time.Second || now.Sub(lease.lastWrite) < 2*time.Second {
				return false
			}
		}
	}
	return true
}

func (s *Smart) applyMeasurementResultLocked(result smartResult, now time.Time) {
	member := result.member
	if result.err != nil {
		return
	}
	measurement := result.measurement
	if measurement.Download.Valid {
		value := measurement.Download
		if value.At.IsZero() {
			value.At = now
		}
		value.Source = "active"
		member.throughput.DownloadResult = value
		member.throughput.Download, member.throughput.At, member.throughput.Source = value.Value, value.At, "active"
		s.clearRecoveredFloorLocked(member, now)
	}
	if measurement.Upload.Valid {
		value := measurement.Upload
		if value.At.IsZero() {
			value.At = now
		}
		value.Source = "active"
		member.throughput.UploadResult = value
		member.throughput.Upload, member.throughput.UploadAt = value.Value, value.At
		member.throughput.UploadUnverified, member.throughput.UploadInconclusive = false, false
	} else if result.plan.Upload != nil {
		member.throughput.UploadInconclusive = true
		member.throughput.UploadUnverified = measurement.UploadUnverified
	}
	if measurement.Download.Valid || measurement.Upload.Valid {
		s.eventLocked("speedtest", "", member.tag, s.network.Key, "active throughput measurement", now)
	}
	for _, key := range s.network.Path {
		stat := s.history.statistic(key, member.identity, now)
		if measurement.Download.Valid {
			stat.DownloadResult = member.throughput.DownloadResult
			stat.Download, stat.ThroughputAt = member.throughput.Download, member.throughput.At.Unix()
			points := s.history.Timeline[key][member.identity]
			if len(points) > 0 {
				points[len(points)-1].MBPS = member.throughput.Download
			}
		}
		if measurement.Upload.Valid {
			stat.UploadResult, stat.Upload = member.throughput.UploadResult, member.throughput.Upload
		}
		stat.UploadUnverified, stat.UploadInconclusive = member.throughput.UploadUnverified, member.throughput.UploadInconclusive
	}
	s.history.IdentityServers[member.identity] = member.serverIdentity
	s.dirty = true
}
