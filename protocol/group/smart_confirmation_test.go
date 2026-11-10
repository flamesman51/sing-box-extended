package group

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

func smartProfileThroughputTestGroup(t *testing.T) (*Smart, *smartFakeClock) {
	t.Helper()
	s, clock := smartDegradationTestGroup(t)
	s.preference.legacy = false
	s.preference.profile = "custom"
	s.preference.rank = []string{"speed_floor", "stability", "download", "latency"}
	s.preference.minStability = -1
	s.preference.improvement = true
	s.preference.dwell = 10 * time.Minute
	s.preference.confirmations = 2
	s.preference.confirmationInterval = 30 * time.Second
	s.preference.tierRestore = time.Minute
	s.preference.speedMargin = 1.25
	s.preference.hysteresis = .2
	return s, clock
}

func smartAwaitConfirmationAdmission(t *testing.T, s *Smart, clock *smartFakeClock) smartResult {
	t.Helper()
	select {
	case result := <-s.results:
		if result.kind != "admission" || result.admission.job.purpose != smartImprovementConfirmation || result.admission.job.episode != nil {
			t.Fatalf("unexpected confirmation admission: %+v", result)
		}
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("confirmation admission did not finish")
		return smartResult{}
	}
}

func smartPrepareConfirmation(t *testing.T) (*Smart, *smartFakeClock, *smartMember) {
	t.Helper()
	s, clock := smartProfileThroughputTestGroup(t)
	s.preference.rank = []string{"download"}
	s.primary.throughput.Download = 3
	candidate := s.standby
	candidate.throughput.Download = 20
	candidate.lastThroughput = clock.Now()
	s.selectedAt = clock.Now()
	s.chooseLocked(clock.Now())
	if s.improvementCandidate != candidate || candidate.improvementObservations != 1 || candidate.improvementCriterion != "download" {
		t.Fatal("initial measured candidate did not start download confirmations")
	}
	return s, clock, candidate
}

func TestSmartImprovementConfirmationScheduledWithinDwell(t *testing.T) {
	s, clock, candidate := smartPrepareConfirmation(t)
	s.preference.confirmations = 3
	primary := s.primary
	if s.scheduleImprovementConfirmationLocked(clock.Now()) || s.admission != nil {
		t.Fatal("confirmation was admitted before its interval")
	}
	for observations := 2; observations <= 3; observations++ {
		clock.Advance(s.preference.confirmationInterval)
		if !s.scheduleImprovementConfirmationLocked(clock.Now()) {
			t.Fatal("fresh regular-tested candidate did not receive a confirmation")
		}
		result := smartAwaitConfirmationAdmission(t, s, clock)
		if result.err != nil {
			t.Fatal(result.err)
		}
		s.ledger.mu.Lock()
		reservation := s.ledger.state.Reservations[result.admission.job.reservation]
		s.ledger.mu.Unlock()
		if reservation.Purpose != "" {
			t.Fatal("improvement confirmation used the degradation allowance")
		}
		s.applyAdmissionLocked(result, clock.Now())
		if len(s.pending) != 1 || s.pending[0].purpose != smartImprovementConfirmation || s.pending[0].episode != nil {
			t.Fatal("confirmation lost its purpose or acquired an episode")
		}
		job := s.pending[0]
		s.pending = nil
		if err := s.ledger.Reconcile(s.ctx, job.reservation, 1_000_000, false); err != nil {
			t.Fatal(err)
		}
		s.applyResultLocked(smartResult{member: candidate, kind: "throughput", purpose: job.purpose, download: 20, generation: job.generation, configGeneration: job.configGeneration, memberGeneration: job.memberGeneration, primaryEpoch: job.primaryEpoch, improvementEpoch: job.improvementEpoch}, clock.Now())
		if candidate.improvementObservations != observations || s.primary != primary {
			t.Fatalf("observations=%d primary=%s before dwell", candidate.improvementObservations, s.primary.tag)
		}
	}
	clock.Advance(s.preference.dwell)
	s.chooseLocked(clock.Now())
	if s.primary != candidate {
		t.Fatal("scheduled confirmations did not permit the improvement after dwell")
	}
}

func TestSmartImprovementConfirmationDenialPreservesObservations(t *testing.T) {
	for _, denial := range []string{"budget", "rate", "busy"} {
		t.Run(denial, func(t *testing.T) {
			s, clock, candidate := smartPrepareConfirmation(t)
			var expected error
			switch denial {
			case "budget":
				s.ledger.Register(s.Tag(), map[string]float64{"wifi": 1})
				expected = errSmartBudget
			case "rate":
				s.ledger.mu.Lock()
				s.ledger.state.Starts = []int64{clock.Now().Unix(), clock.Now().Unix(), clock.Now().Unix(), clock.Now().Unix()}
				s.ledger.mu.Unlock()
				expected = errSmartTestRate
			case "busy":
				id, _, err := s.ledger.Reserve(s.ctx, "wifi", clock.Now(), 0)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := s.ledger.Reconcile(s.ctx, id, 0, false); err != nil {
						t.Error(err)
					}
				})
				expected = errSmartTestBusy
			}
			clock.Advance(s.preference.confirmationInterval)
			if !s.scheduleImprovementConfirmationLocked(clock.Now()) {
				t.Fatal("confirmation did not enter ledger admission")
			}
			result := smartAwaitConfirmationAdmission(t, s, clock)
			if !errors.Is(result.err, expected) {
				t.Fatalf("admission error=%v, want %v", result.err, expected)
			}
			s.applyAdmissionLocked(result, clock.Now())
			s.chooseLocked(clock.Now())
			if s.improvementCandidate != candidate || candidate.improvementObservations != 1 || candidate.improvementCriterion != "download" {
				t.Fatal("ledger denial reset pending confirmations")
			}
			if s.scheduleImprovementConfirmationLocked(clock.Now()) || s.admission != nil {
				t.Fatal("denied confirmation ignored the admission retry deadline")
			}
		})
	}
}

func TestSmartImprovementConfirmationEligibility(t *testing.T) {
	for _, change := range []string{"disabled", "legacy", "probe_criterion", "complete", "active_expired", "passive_expired", "candidate_changed", "no_longer_better", "traffic", "queued", "episode"} {
		t.Run(change, func(t *testing.T) {
			s, clock, candidate := smartPrepareConfirmation(t)
			clock.Advance(s.preference.confirmationInterval)
			switch change {
			case "disabled":
				s.preference.improvement = false
			case "legacy":
				s.preference.legacy = true
			case "probe_criterion":
				candidate.improvementCriterion = "latency"
			case "complete":
				candidate.improvementObservations = s.preference.confirmations
			case "active_expired":
				candidate.throughput.At = clock.Now().Add(-24 * time.Hour)
			case "passive_expired":
				candidate.throughput.Source = "passive"
				candidate.throughput.At = clock.Now().Add(-6 * time.Hour)
			case "candidate_changed":
				s.improvementCandidate = nil
			case "no_longer_better":
				candidate.throughput.Download = 1
			case "traffic":
				lease := &smartLease{lastRead: clock.Now()}
				s.leases[s.primary] = map[*smartLease]struct{}{lease: {}}
			case "queued":
				candidate.queued["throughput"] = true
			case "episode":
				smartTestDegradationEpisode(s, clock.Now())
			}
			if s.scheduleImprovementConfirmationLocked(clock.Now()) || s.admission != nil {
				t.Fatal("ineligible confirmation was scheduled")
			}
		})
	}
}

func TestSmartImprovementConfirmationDelayedAdmissionRevalidated(t *testing.T) {
	for _, change := range []string{"candidate", "criterion", "expired", "disabled"} {
		t.Run(change, func(t *testing.T) {
			s, clock, candidate := smartPrepareConfirmation(t)
			clock.Advance(s.preference.confirmationInterval)
			if !s.scheduleImprovementConfirmationLocked(clock.Now()) {
				t.Fatal("confirmation was not scheduled")
			}
			result := smartAwaitConfirmationAdmission(t, s, clock)
			if result.err != nil {
				t.Fatal(result.err)
			}
			switch change {
			case "candidate":
				s.improvementCandidate = nil
			case "criterion":
				candidate.improvementCriterion = "stability"
				s.improvementEpoch++
			case "expired":
				clock.Advance(24 * time.Hour)
			case "disabled":
				s.preference.improvement = false
			}
			s.applyAdmissionLocked(result, clock.Now())
			if len(s.pending) != 0 || candidate.queued["throughput"] || candidate.improvementObservations != 1 {
				t.Fatal("stale admission queued work or reset observations")
			}
		})
	}
}

func TestSmartImprovementConfirmationAdmissionDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, clock, candidate := smartPrepareConfirmation(t)
		cache := new(smartLedgerBlockingCache)
		ledger, err := getSmartLedger(cache)
		if err != nil {
			t.Fatal(err)
		}
		s.ledger.Unregister(s.Tag())
		ledger.Register(s.Tag(), map[string]float64{"wifi": 5000})
		s.ledger = ledger
		// Force cancellation to win the result handoff while the owner is busy.
		s.results = make(chan smartResult)
		blocked := cache.block(t)
		clock.Advance(s.preference.confirmationInterval)
		began := time.Now()
		if !s.scheduleImprovementConfirmationLocked(clock.Now()) {
			t.Fatal("confirmation was not scheduled")
		}
		admission := s.admission
		deadline, ok := admission.job.ctx.Deadline()
		if !ok || deadline.Sub(began) != 30*time.Second {
			t.Fatalf("confirmation deadline = %v, present=%v; want 30 seconds from admission", deadline, ok)
		}
		smartLedgerWait(t, blocked.entered)
		time.Sleep(29 * time.Second)
		clock.Advance(29 * time.Second)
		synctest.Wait()
		if admission.job.ctx.Err() != nil {
			t.Fatal("confirmation expired before its admission deadline")
		}
		time.Sleep(time.Second)
		clock.Advance(time.Second)
		synctest.Wait()
		if !errors.Is(admission.job.ctx.Err(), context.DeadlineExceeded) {
			t.Fatalf("blocked admission context = %v; want deadline exceeded", admission.job.ctx.Err())
		}
		if len(s.pending) != 0 || candidate.queued["throughput"] || candidate.improvementObservations != 1 {
			t.Fatal("expired admission queued work or reset confirmations")
		}
		clock.Advance(time.Minute)
		if !s.scheduleImprovementConfirmationLocked(clock.Now()) || s.admission == admission {
			t.Fatal("expired admission prevented a later confirmation")
		}
		laterAdmission := s.admission
		synctest.Wait()
		cleanup := cache.block(t)
		blocked.unblock()
		smartLedgerWait(t, cleanup.entered)
		if len(cache.metadata(t).Ledger.Reservations) != 1 {
			t.Fatal("expired admission did not reconcile its late durable reservation")
		}
		next := cache.block(t)
		cleanup.unblock()
		smartLedgerWait(t, next.entered)
		if metadata := cache.metadata(t); len(metadata.Ledger.Reservations) != 0 || len(metadata.Ledger.Days) != 0 {
			t.Fatalf("expired admission left a reservation or charged unused bytes: %+v", metadata.Ledger)
		}
		time.Sleep(5 * time.Second)
		clock.Advance(5 * time.Second)
		next.unblock()
		result := smartAwaitConfirmationAdmission(t, s, clock)
		if result.err != nil {
			t.Fatal(result.err)
		}
		s.applyAdmissionLocked(result, clock.Now())
		synctest.Wait()
		if result.admission != laterAdmission {
			t.Fatal("late cleanup replaced the current admission")
		}
		if len(s.pending) != 1 || s.pending[0].ctx.Err() != nil || candidate.improvementObservations != 1 {
			t.Fatal("later confirmation did not retain its own live context and observations")
		}
		job := s.pending[0]
		if deadline, ok = job.ctx.Deadline(); !ok || time.Until(deadline) != 25*time.Second {
			t.Fatal("queued confirmation lost its original admission deadline")
		}
		time.Sleep(25 * time.Second)
		clock.Advance(25 * time.Second)
		synctest.Wait()
		if !errors.Is(job.ctx.Err(), context.DeadlineExceeded) {
			t.Fatal("queued confirmation outlived its admission deadline")
		}
		s.pending = nil
		delete(candidate.queued, "throughput")
		if err = ledger.Reconcile(s.ctx, job.reservation, 0, false); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSmartImprovementConfirmationSpeedFloor(t *testing.T) {
	s, clock := smartProfileThroughputTestGroup(t)
	primary, candidate := s.primary, s.standby
	primary.throughput.Download = 3
	candidate.throughput.Download = 30
	candidate.lastThroughput = clock.Now()
	s.selectedAt = clock.Now()
	s.chooseLocked(clock.Now())
	if s.improvementCandidate != candidate || candidate.improvementCriterion != "speed_floor" || candidate.improvementObservations != 1 {
		t.Fatal("floor winner did not start criterion-specific confirmations")
	}
	clock.Advance(s.preference.confirmationInterval)
	if !s.scheduleImprovementConfirmationLocked(clock.Now()) {
		t.Fatal("floor winner did not receive a scheduled confirmation")
	}
	result := smartAwaitConfirmationAdmission(t, s, clock)
	if result.err != nil {
		t.Fatal(result.err)
	}
	s.applyAdmissionLocked(result, clock.Now())
	if len(s.pending) != 1 || s.pending[0].member != candidate || candidate.improvementObservations != 1 || s.primary != primary {
		t.Fatal("admission counted an observation before new measurement evidence")
	}
}

func TestSmartImprovementConfirmationThroughputClass(t *testing.T) {
	s, clock := smartProfileThroughputTestGroup(t)
	s.preference.rank = []string{"throughput"}
	primary, candidate := s.primary, s.standby
	primary.throughput.Download = 3
	candidate.throughput.Download = 20
	s.selectedAt = clock.Now()
	s.chooseLocked(clock.Now())
	if s.improvementCandidate != candidate || candidate.improvementCriterion != "throughput" {
		t.Fatal("throughput-class winner did not start confirmations")
	}
	clock.Advance(s.preference.confirmationInterval)
	if !s.scheduleImprovementConfirmationLocked(clock.Now()) {
		t.Fatal("throughput-class improvement did not receive a scheduled confirmation")
	}
	result := smartAwaitConfirmationAdmission(t, s, clock)
	if result.err != nil {
		t.Fatal(result.err)
	}
	s.applyAdmissionLocked(result, clock.Now())
	if len(s.pending) != 1 || s.pending[0].member != candidate {
		t.Fatal("throughput-class confirmation was not queued")
	}
}

func TestSmartImprovementConfirmationTokenIndependentOfObservationSpacing(t *testing.T) {
	s, clock, candidate := smartPrepareConfirmation(t)
	primaryEpoch, improvementEpoch := s.primaryEpoch, s.improvementEpoch
	candidate.improvementObservations = s.preference.confirmations
	candidate.improvementAt = clock.Now()
	if !s.improvementConfirmationJobValidLocked(candidate, primaryEpoch, improvementEpoch, clock.Now()) {
		t.Fatal("valid in-flight job was invalidated by count or spacing")
	}
	s.improvementEpoch++
	if s.improvementConfirmationJobValidLocked(candidate, primaryEpoch, improvementEpoch, clock.Now()) {
		t.Fatal("candidate nonce change retained an old job")
	}
	s.improvementEpoch = improvementEpoch
	s.primaryEpoch++
	if s.improvementConfirmationJobValidLocked(candidate, primaryEpoch, improvementEpoch, clock.Now()) {
		t.Fatal("primary epoch change retained an old job")
	}
}

func TestSmartImprovementConfirmationLateResultScope(t *testing.T) {
	for _, change := range []string{"network", "membership", "primary_away_back", "candidate_away_back"} {
		t.Run(change, func(t *testing.T) {
			s, clock, candidate := smartPrepareConfirmation(t)
			primary := s.primary
			clock.Advance(s.preference.confirmationInterval)
			if !s.scheduleImprovementConfirmationLocked(clock.Now()) {
				t.Fatal("confirmation was not scheduled")
			}
			admission := smartAwaitConfirmationAdmission(t, s, clock)
			if admission.err != nil {
				t.Fatal(admission.err)
			}
			s.applyAdmissionLocked(admission, clock.Now())
			if len(s.pending) != 1 {
				t.Fatal("confirmation was not queued")
			}
			job := s.pending[0]
			s.pending = nil
			if err := s.ledger.Reconcile(s.ctx, job.reservation, 1_000_000, false); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "network":
				network := s.network
				network.Key = "other"
				network.Path = []string{"other"}
				s.newGenerationLocked(network, clock.Now())
			case "membership":
				s.configGeneration++
				s.newGenerationLocked(s.network, clock.Now())
			case "primary_away_back":
				s.switchLocked(candidate, "switch", "test", clock.Now())
				s.switchLocked(primary, "switch", "test", clock.Now())
			case "candidate_away_back":
				s.resetProfileImprovementLocked()
			}
			if s.improvementCandidate != nil || candidate.improvementObservations != 0 {
				t.Fatal("scope change retained pending confirmation state")
			}
			if change == "primary_away_back" || change == "candidate_away_back" {
				candidate.throughput.At = clock.Now()
				s.chooseLocked(clock.Now())
				if s.improvementCandidate != candidate || candidate.improvementObservations != 1 {
					t.Fatal("same candidate did not start a new pending scope")
				}
			}
			before := candidate.throughput
			observations := candidate.improvementObservations
			s.applyResultLocked(smartResult{member: candidate, kind: "throughput", purpose: job.purpose, download: 80, generation: job.generation, configGeneration: job.configGeneration, memberGeneration: job.memberGeneration, primaryEpoch: job.primaryEpoch, improvementEpoch: job.improvementEpoch}, clock.Now())
			if candidate.throughput != before || candidate.improvementObservations != observations {
				t.Fatal("late confirmation result published evidence into a different scope")
			}
		})
	}
}

func TestSmartFamilyBudgetDeniesPairButAllowsRegularConfirmations(t *testing.T) {
	for _, networkType := range []string{"wifi", "cellular"} {
		t.Run(networkType, func(t *testing.T) {
			s, clock := smartProfileThroughputTestGroup(t)
			s.ledger.Register(s.Tag(), map[string]float64{"wifi": 300, "cellular": 50})
			s.network.Type = networkType
			primary, candidate := s.primary, s.standby
			primary.throughput.Download = 3
			candidate.throughput = smartThroughput{}
			s.selectedAt = clock.Now()
			e := smartTestDegradationEpisode(s, clock.Now())
			s.startDegradationAdmissionLocked(primary, smartDegradationConfirmation, e, clock.Now())
			var admission smartResult
			select {
			case admission = <-s.results:
			case <-time.After(5 * time.Second):
				t.Fatal("degradation admission did not finish")
			}
			if !errors.Is(admission.err, errSmartDegradationBudget) {
				t.Fatalf("degradation admission=%v, want allowance denial", admission.err)
			}
			s.applyAdmissionLocked(admission, clock.Now())
			budget := s.ledger.Snapshot(networkType, clock.Now(), s.network.Offset)
			if s.degradation.episode != nil || len(s.pending) != 0 || budget.ReservedMB != 0 || budget.Degradation.UsedMB != 0 {
				t.Fatal("denied family-budget pair retained work or charges")
			}
			for _, purpose := range []string{"", smartImprovementConfirmation} {
				if purpose == "" {
					s.startThroughputAdmissionLocked(candidate, clock.Now())
					select {
					case admission = <-s.results:
					case <-time.After(5 * time.Second):
						t.Fatal("regular admission did not finish")
					}
				} else {
					clock.Advance(s.preference.confirmationInterval)
					if !s.scheduleImprovementConfirmationLocked(clock.Now()) {
						t.Fatal("family-budget candidate did not receive a confirmation")
					}
					admission = smartAwaitConfirmationAdmission(t, s, clock)
				}
				if admission.err != nil {
					t.Fatal(admission.err)
				}
				s.applyAdmissionLocked(admission, clock.Now())
				if len(s.pending) != 1 || s.pending[0].purpose != purpose {
					t.Fatal("ordinary-budget throughput work was not queued")
				}
				job := s.pending[0]
				s.pending = nil
				if err := s.ledger.Reconcile(s.ctx, job.reservation, 1_000_000, false); err != nil {
					t.Fatal(err)
				}
				s.applyResultLocked(smartResult{member: candidate, kind: "throughput", purpose: job.purpose, download: 30, generation: job.generation, configGeneration: job.configGeneration, memberGeneration: job.memberGeneration, primaryEpoch: job.primaryEpoch, improvementEpoch: job.improvementEpoch}, clock.Now())
			}
			if candidate.improvementObservations != s.preference.confirmations || s.primary != primary {
				t.Fatal("regular test and confirmation did not converge within dwell")
			}
			clock.Advance(s.preference.dwell)
			s.chooseLocked(clock.Now())
			budget = s.ledger.Snapshot(networkType, clock.Now(), s.network.Offset)
			if s.primary != candidate || budget.UsedMB != 2 || budget.ReservedMB != 0 || budget.Degradation.UsedMB != 0 {
				t.Fatal("family budgets did not permit ordinary confirmed improvement without pair spending")
			}
		})
	}
}

type smartConfirmationTestOutbound struct {
	*smartFakeOutbound
	dialer *smartProbeTestDialer
}

func (o *smartConfirmationTestOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return o.dialer.DialContext(ctx, network, destination)
}

func TestSmartImprovementConfirmationSurvivesHeartbeatAndSkipsUpload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, clock, candidate := smartPrepareConfirmation(t)
		const previousUpload = 7.5
		candidate.throughput.Upload = previousUpload
		for _, key := range s.network.Path {
			s.history.statistic(key, candidate.identity, clock.Now()).Upload = previousUpload + 1
		}
		s.workCtx, s.workCancel = context.WithCancel(s.ctx)
		s.backgroundCtx, s.backgroundCancel = context.WithCancel(s.workCtx)
		s.options.Throughput.DownloadURL = "http://192.0.2.1/down?bytes={bytes}"
		s.options.Throughput.UploadURL = "http://192.0.2.1/up"
		started := make(chan struct{})
		uploads := 0
		dialer := &smartProbeTestDialer{httpHandler: func(conn net.Conn) {
			defer conn.Close()
			reader := bufio.NewReader(conn)
			for {
				request, err := http.ReadRequest(reader)
				if err != nil {
					return
				}
				if request.Method == http.MethodPost {
					uploads++
					_, _ = io.Copy(io.Discard, request.Body)
					_ = request.Body.Close()
					_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
					continue
				}
				_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\n")
				_, _ = io.CopyN(conn, smartZeroReader{}, 100_000)
				close(started)
				time.Sleep(s.heartbeat + time.Second)
				if _, err = io.CopyN(conn, smartZeroReader{}, 900_000); err != nil {
					return
				}
			}
		}}
		candidate.outbound = &smartConfirmationTestOutbound{smartFakeOutbound: &smartFakeOutbound{tag: candidate.tag}, dialer: dialer}
		clock.Advance(s.preference.confirmationInterval)
		if !s.scheduleImprovementConfirmationLocked(clock.Now()) {
			t.Fatal("confirmation was not scheduled")
		}
		admission := smartAwaitConfirmationAdmission(t, s, clock)
		if admission.err != nil {
			t.Fatal(admission.err)
		}
		s.applyAdmissionLocked(admission, clock.Now())
		if len(s.pending) != 1 || s.pending[0].ctx != admission.admission.job.ctx {
			t.Fatal("confirmation did not retain its independent admission context")
		}
		job := s.pending[0]
		s.pending = nil
		done := make(chan smartResult, 1)
		began := time.Now()
		go func() { done <- s.runOperation(job) }()
		<-started
		time.Sleep(s.heartbeat)
		clock.Advance(s.heartbeat)
		background := s.backgroundCtx
		s.preemptBackgroundLocked(smartJob{kind: "tcp", urgent: true, member: s.primary})
		if background.Err() == nil || job.ctx.Err() != nil {
			t.Fatal("heartbeat did not preempt ordinary work independently of confirmation")
		}
		result := <-done
		if result.err != nil || result.download <= 0 || result.upload != 0 || uploads != 0 || result.bytes <= 0 || time.Since(began) <= s.heartbeat {
			t.Fatalf("long download-only confirmation failed: result=%+v uploads=%d elapsed=%s", result, uploads, time.Since(began))
		}
		if err := s.ledger.Reconcile(s.ctx, job.reservation, result.bytes, false); err != nil {
			t.Fatal(err)
		}
		s.applyResultLocked(result, clock.Now())
		if candidate.throughput.Upload != previousUpload {
			t.Fatalf("download-only confirmation replaced live upload with %v", candidate.throughput.Upload)
		}
		for _, key := range s.network.Path {
			if upload := s.history.Stats[key][candidate.identity].Upload; upload != previousUpload+1 {
				t.Fatalf("download-only confirmation replaced %q history upload with %v", key, upload)
			}
		}
		data, err := s.history.Encode()
		if err != nil {
			t.Fatal(err)
		}
		restored, err := decodeSmartHistory(data)
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range s.network.Path {
			if upload := restored.Stats[key][candidate.identity].Upload; upload != previousUpload+1 {
				t.Fatalf("persisted confirmation lost %q upload: %v", key, upload)
			}
		}
	})
}

func TestSmartImprovementConfirmationContinuousTrafficStaysPending(t *testing.T) {
	s, clock, candidate := smartPrepareConfirmation(t)
	primary := s.primary
	lease := &smartLease{group: s, member: primary, generation: s.generation, started: clock.Now(), lastRead: clock.Now(), closer: &smartTestCloser{}}
	s.leases[primary] = map[*smartLease]struct{}{lease: {}}
	for range 24 {
		clock.Advance(30 * time.Second)
		lease.lastRead = clock.Now()
		s.chooseLocked(clock.Now())
		if s.scheduleImprovementConfirmationLocked(clock.Now()) || s.admission != nil || len(s.pending) != 0 {
			t.Fatal("continuous traffic admitted an improvement confirmation")
		}
		if s.primary != primary || s.improvementCandidate != candidate || candidate.improvementObservations != 1 {
			t.Fatal("continuous traffic switched or discarded the pending candidate")
		}
	}
	clock.Advance(2 * time.Second)
	if !s.scheduleImprovementConfirmationLocked(clock.Now()) {
		t.Fatal("idle window did not admit the pending confirmation")
	}
	admission := smartAwaitConfirmationAdmission(t, s, clock)
	if admission.err != nil {
		t.Fatal(admission.err)
	}
	s.applyAdmissionLocked(admission, clock.Now())
}

func TestSmartImprovementConfirmationContextCancellation(t *testing.T) {
	for _, change := range []string{"candidate_reset", "network", "unhealthy", "work_cancel", "shutdown"} {
		t.Run(change, func(t *testing.T) {
			s, clock, candidate := smartPrepareConfirmation(t)
			s.workCtx, s.workCancel = context.WithCancel(s.ctx)
			s.backgroundCtx, s.backgroundCancel = context.WithCancel(s.workCtx)
			clock.Advance(s.preference.confirmationInterval)
			if !s.scheduleImprovementConfirmationLocked(clock.Now()) {
				t.Fatal("confirmation was not scheduled")
			}
			admission := smartAwaitConfirmationAdmission(t, s, clock)
			if admission.err != nil {
				t.Fatal(admission.err)
			}
			s.applyAdmissionLocked(admission, clock.Now())
			if len(s.pending) != 1 {
				t.Fatal("confirmation was not queued")
			}
			job := s.pending[0]
			s.pending = nil
			if err := s.ledger.Reconcile(s.ctx, job.reservation, 0, false); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "candidate_reset":
				s.resetProfileImprovementLocked()
			case "network":
				s.newGenerationLocked(s.network, clock.Now())
			case "unhealthy":
				s.applyResultLocked(smartResult{member: candidate, kind: "tcp", generation: s.generation, configGeneration: s.configGeneration, memberGeneration: candidate.generation, err: errors.New("probe failed")}, clock.Now())
			case "work_cancel":
				s.workCancel()
			case "shutdown":
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if !errors.Is(job.ctx.Err(), context.Canceled) {
				t.Fatalf("%s did not cancel the independent confirmation context", change)
			}
		})
	}
}
