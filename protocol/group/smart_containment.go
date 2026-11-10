package group

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	errSmartContained   = errors.New("smart: contained internal panic")
	smartPanicURL       = regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.-]*://[^\s<>"']+`)
	smartPanicIPv4      = regexp.MustCompile(`\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b`)
	smartPanicIPv6      = regexp.MustCompile(`(?:[0-9a-fA-F]{0,4}:){2,}[0-9a-fA-F:.]*(?:%[a-zA-Z0-9_.-]+)?`)
	smartPanicHost      = regexp.MustCompile(`(?i)\b(?:[a-z0-9_](?:[a-z0-9_-]*[a-z0-9_])?\.)+[a-z][a-z0-9_-]*\b`)
	smartPanicEndpoint  = regexp.MustCompile(`\b[a-zA-Z_][a-zA-Z0-9_.-]*:[0-9]+\b`)
	smartPanicNamedHost = regexp.MustCompile(`(?i)(\b(?:host|hostname|server|address)\s*[=:]\s*|\blookup\s+)[^\s,;<>]+`)
)

type smartPanicEvent struct {
	At      time.Time `json:"at"`
	Site    string    `json:"site"`
	Message string    `json:"message"`
	Stack   string    `json:"stack"`
}

type smartPanics struct {
	Total  uint64            `json:"total"`
	Recent []smartPanicEvent `json:"recent"`
}

func smartTruncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	value = value[:limit]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

func smartScrubMessage(value string) string {
	value = smartPanicURL.ReplaceAllLiteralString(value, "<url>")
	value = smartPanicIPv6.ReplaceAllStringFunc(value, func(candidate string) string {
		for address := candidate; address != ""; address = address[:len(address)-1] {
			if _, err := netip.ParseAddr(address); err == nil {
				return "<ip>" + candidate[len(address):]
			}
			if !strings.HasSuffix(address, ".") && !strings.HasSuffix(address, ":") {
				break
			}
		}
		return candidate
	})
	value = smartPanicIPv4.ReplaceAllLiteralString(value, "<ip>")
	value = smartPanicEndpoint.ReplaceAllLiteralString(value, "<host>")
	value = smartPanicHost.ReplaceAllLiteralString(value, "<host>")
	value = smartPanicNamedHost.ReplaceAllString(value, "${1}<host>")
	return smartTruncate(strings.ToValidUTF8(value, "�"), 512)
}

func smartScrubStack(stack []byte) string {
	var frames []string
	for line := range strings.SplitSeq(string(stack), "\n") {
		if strings.HasPrefix(line, "\t") {
			location := strings.Fields(line)
			if len(location) > 0 {
				frames = append(frames, filepath.Base(location[0]))
			}
			continue
		}
		if created, isCreated := strings.CutPrefix(line, "created by "); isCreated {
			line, _, _ = strings.Cut(created, " in goroutine ")
		} else if index := strings.LastIndex(line, "("); index >= 0 {
			line = line[:index]
		} else {
			continue
		}
		if index := strings.LastIndex(line, "/"); index >= 0 {
			line = line[index+1:]
		}
		frames = append(frames, line)
	}
	return smartTruncate(strings.Join(frames, "\n"), 4096)
}

func smartPanicMessage(value any) (message string) {
	defer func() {
		if recover() != nil {
			message = "panic value could not be formatted"
		}
	}()
	return smartScrubMessage(fmt.Sprint(value))
}

func smartCapturePanic(site string, value any) smartPanicEvent {
	return smartPanicEvent{At: time.Now().UTC(), Site: site, Message: smartPanicMessage(value), Stack: smartScrubStack(debug.Stack())}
}

func (s *Smart) recordPanic(event smartPanicEvent) {
	s.panicMu.Lock()
	s.panics.Total++
	if len(s.panics.Recent) == 20 {
		copy(s.panics.Recent, s.panics.Recent[1:])
		s.panics.Recent = s.panics.Recent[:19]
	}
	s.panics.Recent = append(s.panics.Recent, event)
	s.panicMu.Unlock()
	// Logging must not turn an already contained panic into a second process crash.
	func() {
		defer func() { _ = recover() }()
		if s.logger != nil {
			s.logger.Error("contained smart panic at ", event.Site, ": ", event.Message, "\n", event.Stack)
		}
	}()
}

func (s *Smart) panicSnapshot() smartPanics {
	s.panicMu.Lock()
	defer s.panicMu.Unlock()
	return smartPanics{Total: s.panics.Total, Recent: append([]smartPanicEvent{}, s.panics.Recent...)}
}

func (s *Smart) recoverError(site string, err *error) {
	if value := recover(); value != nil {
		s.recordPanic(smartCapturePanic(site, value))
		*err = errSmartContained
	}
}

func (s *Smart) contain(site string, callback func()) {
	defer func() {
		if value := recover(); value != nil {
			s.recordPanic(smartCapturePanic(site, value))
		}
	}()
	callback()
}

func (s *Smart) injectPanic(site string) {
	if s.testHook != nil {
		s.testHook(site)
	}
}

func (s *Smart) fallbackLocked() *smartMember {
	if s.primary != nil {
		return s.primary
	}
	var first *smartMember
	for _, member := range s.members {
		if first == nil || member.tier < first.tier || member.tier == first.tier && member.configOrder < first.configOrder {
			first = member
		}
	}
	return first
}

func (s *Smart) degradedLocked() bool {
	return s.syncDegraded || s.loopStopped || s.generationContained && !s.generationSucceeded
}

func (s *Smart) closeContained(closer io.Closer) { s.contain("close", func() { _ = closer.Close() }) }

func (s *Smart) loopAttempt(ctx context.Context) (event *smartPanicEvent) {
	defer func() {
		if value := recover(); value != nil {
			captured := smartCapturePanic("loop", value)
			event = &captured
		}
	}()
	s.runLoop(ctx)
	return nil
}

func (s *Smart) loop(ctx context.Context) {
	defer close(s.loopDone)
	defer func() {
		if value := recover(); value != nil {
			func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				s.observeAllOutagesLocked(s.clock.Now())
				s.loopStopped = true
				s.observeAllOutagesLocked(s.clock.Now())
			}()
			s.recordPanic(smartCapturePanic("supervisor", value))
		}
	}()
	var restarts []time.Time
	backoff := time.Second
	for {
		event := s.loopAttempt(ctx)
		if event == nil {
			return
		}
		s.contain("restart", func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.observeAllOutagesLocked(s.clock.Now())
			if s.profileEngine() {
				s.resetProfileImprovementLocked()
			}
			s.clearDegradationLocked()
			if s.workCancel != nil {
				s.workCancel()
			}
			s.clearPendingLocked()
			s.discardBufferedJobsLocked()
			for _, member := range s.members {
				s.memberGeneration++
				member.generation = s.memberGeneration
				member.inflight = false
				member.queued = make(map[string]bool)
				member.discoveryPreempted = false
				member.lastProbe = time.Time{}
			}
			s.observeAllOutagesLocked(s.clock.Now())
			s.directInflight = false
			s.cancelURLWaitersLocked(errSmartContained)
			if !s.closed && ctx.Err() == nil {
				s.workCtx, s.workCancel = context.WithCancel(s.ctx)
				s.backgroundCtx, s.backgroundCancel = context.WithCancel(s.workCtx)
			}
			s.generationContained = true
			s.injectPanic("restart")
		})
		s.recordPanic(*event)
		if ctx.Err() != nil {
			return
		}
		now := s.clock.Now()
		restarts = slices.DeleteFunc(restarts, func(at time.Time) bool { return now.Sub(at) >= time.Hour })
		restarts = append(restarts, now)
		if len(restarts) >= 10 {
			func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				s.observeAllOutagesLocked(s.clock.Now())
				s.loopStopped = true
				s.observeAllOutagesLocked(s.clock.Now())
				if s.primary == nil {
					for _, member := range s.members {
						if member.state == "healthy" && (s.primary == nil || member.tier < s.primary.tier || member.tier == s.primary.tier && member.configOrder < s.primary.configOrder) {
							s.primary = member
						}
					}
					if s.primary == nil {
						s.primary = s.fallbackLocked()
					}
				}
			}()
			return
		}
		timer := s.clock.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C():
		}
		backoff = min(backoff*2, 5*time.Minute)
	}
}

func (s *Smart) runJob(job smartJob) smartResult {
	if job.episode != nil {
		defer job.episode.accountingWait.Done()
	}
	result := s.runOperation(job)
	if job.episode != nil && result.contained {
		job.episode.accountingPanic.Store(true)
	}
	if job.kind == "throughput" && job.episode == nil && s.ledger != nil {
		uncertain := func() bool {
			s.mu.Lock()
			defer s.mu.Unlock()
			return job.generation != s.generation || job.networkType != s.network.Type
		}()
		if err := s.reconcileReservation(job.reservation, result.bytes, uncertain); err != nil {
			result.err = errors.Join(result.err, err)
			result.contained = result.contained || errors.Is(err, errSmartContained)
		}
	}
	return result
}

type smartContainmentKey struct{}

func (s *Smart) runOperation(job smartJob) (result smartResult) {
	result = smartResult{member: job.member, kind: job.kind, operator: job.operator, confirm: job.confirm, generation: job.generation, memberGeneration: job.memberGeneration, configGeneration: job.configGeneration, reservation: job.reservation, episode: job.episode, purpose: job.purpose}
	result.plan = job.plan
	result.primaryEpoch, result.improvementEpoch = job.primaryEpoch, job.improvementEpoch
	started := false
	defer func() {
		if value := recover(); value != nil {
			s.recordPanic(smartCapturePanic("probe/"+job.kind, value))
			result.contained, result.err = true, errSmartContained
			if job.kind == "throughput" && started {
				result.bytes = job.capBytes
			}
		}
		if errors.Is(result.err, errSmartContained) {
			result.contained = true
			if job.kind == "throughput" && started {
				result.bytes = job.capBytes
			}
		}
	}()
	workCtx, cancel := context.WithTimeout(context.WithValue(job.ctx, smartContainmentKey{}, s), job.timeout)
	defer cancel()
	if job.ctx.Err() != nil {
		result.err = job.ctx.Err()
		return
	}
	if job.episode != nil && !s.degradationJobValid(job) {
		result.err = context.Canceled
		return
	}
	if job.purpose == smartImprovementConfirmation {
		valid := func() bool {
			s.mu.Lock()
			defer s.mu.Unlock()
			return s.improvementConfirmationJobValidLocked(job.member, job.primaryEpoch, job.improvementEpoch, s.clock.Now())
		}()
		if !valid {
			result.err = context.Canceled
			return
		}
	}
	started = true
	s.injectPanic("probe/" + job.kind)
	switch job.kind {
	case "tcp":
		result.rtt, result.err = s.probe(workCtx, job.member.outbound, job.operator)
	case "udp":
		bursts := max(1, job.plan.STUNBursts)
		probe := s.udpBurstProbe
		if probe == nil {
			probe = smartSTUNProbe
		}
		var observation smartRTTProbeResult
		observation, result.err = probe(workCtx, job.member.outbound, bursts)
		result.udpRTTs, result.udpBurstSizes, result.udpBursts = observation.Samples, observation.BurstSizes, bursts
	case "udp443":
		result.rtt, result.err = s.udpProbe(workCtx, job.member.outbound, true)
	case "direct":
		result.rtt, result.err = s.directProbe(workCtx, job.country, s.directSeed)
	case "throughput":
		meter := job.meter
		if meter == nil {
			meter = newSmartMeasureMeter(job.capBytes)
		}
		result.measurement, result.err = smartMeasure(workCtx, job.member.outbound, s.options.Throughput.DownloadURL, s.options.Throughput.UploadURL, meter, job.plan, s.clock)
		result.download, result.upload = result.measurement.Download.Value, result.measurement.Upload.Value
		result.bytes = meter.chargedBytes()
		if errors.Is(result.err, errSmartMeasureCap) && s.logger != nil {
			s.logger.Warn("speedtest aborted: budget")
		}
	}
	return
}

func smartContextRecover(ctx context.Context, site string, err *error) {
	if value := recover(); value != nil {
		if ctx != nil {
			if s, ok := ctx.Value(smartContainmentKey{}).(*Smart); ok {
				s.recordPanic(smartCapturePanic(site, value))
			}
		}
		*err = errSmartContained
	}
}

func smartCloseCallback(ctx context.Context, closer io.Closer) {
	var err error
	defer smartContextRecover(ctx, "afterfunc", &err)
	if s, ok := ctx.Value(smartContainmentKey{}).(*Smart); ok {
		s.injectPanic("afterfunc")
	}
	_ = closer.Close()
}

type smartAPIGuard struct {
	group                     *Smart
	site                      string
	locked, persistenceLocked bool
}

func (g *smartAPIGuard) lock(s *Smart) {
	g.group = s
	s.mu.Lock()
	g.locked = true
	s.injectPanic(g.site)
}

func (g *smartAPIGuard) unlock() {
	g.group.mu.Unlock()
	g.locked = false
}

func (g *smartAPIGuard) lockPersistence(s *Smart) {
	g.group = s
	s.persistenceMu.Lock()
	g.persistenceLocked = true
}

func (g *smartAPIGuard) unlockPersistence() {
	g.group.persistenceMu.Unlock()
	g.persistenceLocked = false
}

func (g *smartAPIGuard) finish(err *error) {
	value := recover()
	if g.locked {
		g.unlock()
	}
	if g.persistenceLocked {
		g.unlockPersistence()
	}
	if value != nil {
		if g.group != nil {
			g.group.recordPanic(smartCapturePanic(g.site, value))
		}
		*err = errSmartContained
	}
}
