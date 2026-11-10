package group

import "time"

func smartDirectionStatus(value smartDirectionResult, download bool, now time.Time) map[string]any {
	result := map[string]any{
		"value": smartNumber(value.Value), "streams": value.Streams, "window": value.Window.Seconds(),
		"source": smartNullable(value.Source), "at": smartTime(value.At), "valid": value.Valid,
		"fresh": value.Valid && !value.At.IsZero() && now.Sub(value.At) < 24*time.Hour,
	}
	if download {
		result["p10"], result["mean"] = smartNumber(value.P10), smartNumber(value.Mean)
	} else {
		result["post_min"] = smartNumber(value.PostMin)
	}
	return result
}

func (s *Smart) measurementStatusLocked(member *smartMember, entry map[string]any, now time.Time) {
	value := member.throughput
	download := value.DownloadResult
	if !download.Valid && value.Source == "active" && !value.At.IsZero() {
		download = smartDirectionResult{Value: value.Download, At: value.At, Source: "active", Valid: true}
	}
	entry["download"] = smartDirectionStatus(download, true, now)
	entry["upload"] = smartDirectionStatus(value.UploadResult, false, now)
	entry["download_at"], entry["upload_at"] = smartTime(download.At), smartTime(value.UploadResult.At)
	entry["download_p10_mbps"], entry["download_mean_mbps"], entry["upload_post_min_mbps"] = nil, nil, nil
	if download.Valid && now.Sub(download.At) < 24*time.Hour {
		entry["download_p10_mbps"], entry["download_mean_mbps"] = smartNumber(download.P10), smartNumber(download.Mean)
	}
	if value.UploadResult.Valid && now.Sub(value.UploadResult.At) < 24*time.Hour {
		entry["upload_post_min_mbps"] = smartNumber(value.UploadResult.PostMin)
	}
	if value.UploadUnverified {
		entry["upload_status"] = "unverified"
	} else if value.UploadInconclusive {
		entry["upload_status"] = "inconclusive"
	} else if value.UploadResult.Valid {
		entry["upload_status"] = "verified"
	} else {
		entry["upload_status"] = "unknown"
	}
	if value.LegacyUpload > 0 {
		entry["legacy_upload"] = value.LegacyUpload
	}
	down, up := s.passiveDiagnosticsLocked(member, now)
	entry["observed_peak_down"], entry["offered_up"] = down, up
	if member.downloadUnderLoad.Valid {
		entry["download_under_load"] = smartDirectionStatus(member.downloadUnderLoad, true, now)
	}
	if state := member.floorState; state.Kind != "" {
		effective := state.Kind == "revoked"
		if state.Kind == "proven" {
			effective = now.Sub(state.At) < 24*time.Hour && state.Target >= s.targetMbps && !download.At.After(state.At)
		}
		entry["floor_state"] = map[string]any{"kind": state.Kind, "target": state.Target, "at": smartTime(state.At), "value": state.Value, "effective": effective}
	}
	if observation := member.legacyEpisodeObs; !observation.At.IsZero() {
		entry["legacy_episode_obs"] = map[string]any{"value": observation.Value, "at": smartTime(observation.At)}
		if !s.profileEngine() && observation.At.After(download.At) && now.Sub(observation.At) < 24*time.Hour {
			entry["throughput_source"] = "under_load"
		}
	}
	var buffer [64]float64
	samples := smartFreshRTTSamples(member, now, buffer[:0])
	if len(samples) >= 5 {
		median, _ := smartProfileRTTQuantiles(samples)
		entry["udp_rtt_ms"] = median
	}
	if len(samples) >= 10 {
		median, p90 := smartProfileRTTQuantiles(samples)
		entry["jitter_ms"] = p90 - median
	}
	if len(member.quicRTTs) > 0 {
		median, _ := smartProfileRTTQuantiles(member.quicRTTs)
		entry["quic_rtt_ms"] = median
	}
}
