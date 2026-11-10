package group

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

type smartMeasurementLeaseAttempt struct {
	lease *smartMeasurementLease
	err   error
}

func smartStartMeasurementLease(s *Smart, ctx context.Context, exclusive bool) <-chan smartMeasurementLeaseAttempt {
	result := make(chan smartMeasurementLeaseAttempt, 1)
	go func() {
		lease, err := s.acquireMeasurementLease(ctx, exclusive)
		result <- smartMeasurementLeaseAttempt{lease: lease, err: err}
	}()
	return result
}

func smartTakeMeasurementLease(t *testing.T, result <-chan smartMeasurementLeaseAttempt) *smartMeasurementLease {
	t.Helper()
	synctest.Wait()
	select {
	case attempt := <-result:
		if attempt.err != nil {
			t.Fatal(attempt.err)
		}
		t.Cleanup(attempt.lease.release)
		return attempt.lease
	default:
		t.Fatal("measurement lease did not acquire")
		return nil
	}
}

func smartAssertMeasurementLeaseWaiting(t *testing.T, result <-chan smartMeasurementLeaseAttempt) {
	t.Helper()
	synctest.Wait()
	select {
	case attempt := <-result:
		if attempt.lease != nil {
			attempt.lease.release()
		}
		t.Fatalf("measurement lease bypassed an execution barrier: %v", attempt.err)
	default:
	}
}

func smartAssertMeasurementLoad(t *testing.T, s *Smart, epoch uint64, count int) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.measurementLoad.epoch != epoch || len(s.measurementLoad.executing) != count {
		t.Fatalf("measurement load = epoch %d, count %d; want epoch %d, count %d", s.measurementLoad.epoch, len(s.measurementLoad.executing), epoch, count)
	}
}

func TestSmartMeasurementLeaseQueuedDataAndBurstNeverOverlap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &Smart{clock: smartRealClock{}, kick: make(chan struct{}, 1)}
		cohort, err := s.acquireMeasurementLease(context.Background(), true)
		if err != nil {
			t.Fatal(err)
		}
		defer cohort.release()

		dataResult := smartStartMeasurementLease(s, context.Background(), false)
		smartAssertMeasurementLeaseWaiting(t, dataResult)
		burstResult := smartStartMeasurementLease(s, context.Background(), true)
		smartAssertMeasurementLeaseWaiting(t, burstResult)
		cohort.release()

		data := smartTakeMeasurementLease(t, dataResult)
		smartAssertMeasurementLeaseWaiting(t, burstResult)
		smartAssertMeasurementLoad(t, s, 1, 1)
		data.release()
		burst := smartTakeMeasurementLease(t, burstResult)
		smartAssertMeasurementLoad(t, s, 1, 0)
		burst.release()
	})
}

func TestSmartMeasurementLeaseFIFOAndSharedCohort(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &Smart{clock: smartRealClock{}}
		first, err := s.acquireMeasurementLease(context.Background(), false)
		if err != nil {
			t.Fatal(err)
		}
		defer first.release()
		second := smartTakeMeasurementLease(t, smartStartMeasurementLease(s, context.Background(), false))
		smartAssertMeasurementLoad(t, s, 2, 2)

		burstResult := smartStartMeasurementLease(s, context.Background(), true)
		smartAssertMeasurementLeaseWaiting(t, burstResult)
		thirdResult := smartStartMeasurementLease(s, context.Background(), false)
		smartAssertMeasurementLeaseWaiting(t, thirdResult)
		fourthResult := smartStartMeasurementLease(s, context.Background(), false)
		smartAssertMeasurementLeaseWaiting(t, fourthResult)
		lastBurstResult := smartStartMeasurementLease(s, context.Background(), true)
		smartAssertMeasurementLeaseWaiting(t, lastBurstResult)

		first.release()
		smartAssertMeasurementLeaseWaiting(t, burstResult)
		second.release()
		burst := smartTakeMeasurementLease(t, burstResult)
		smartAssertMeasurementLeaseWaiting(t, thirdResult)
		smartAssertMeasurementLeaseWaiting(t, fourthResult)
		burst.release()
		third := smartTakeMeasurementLease(t, thirdResult)
		fourth := smartTakeMeasurementLease(t, fourthResult)
		smartAssertMeasurementLoad(t, s, 4, 2)
		smartAssertMeasurementLeaseWaiting(t, lastBurstResult)
		third.release()
		smartAssertMeasurementLeaseWaiting(t, lastBurstResult)
		fourth.release()
		smartTakeMeasurementLease(t, lastBurstResult).release()
		smartAssertMeasurementLoad(t, s, 4, 0)
	})
}

func TestSmartMeasurementLeaseExcludesWholeCohortGap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &Smart{clock: smartRealClock{}}
		firstBurst := make(chan struct{})
		secondBurst := make(chan struct{})
		finishCohort := make(chan struct{})
		cohortDone := make(chan error, 1)
		go func() {
			lease, err := s.acquireMeasurementLease(context.Background(), true)
			if err != nil {
				cohortDone <- err
				return
			}
			defer lease.release()
			close(firstBurst)
			<-secondBurst
			<-finishCohort
			cohortDone <- nil
		}()
		<-firstBurst
		dataResult := smartStartMeasurementLease(s, context.Background(), false)
		smartAssertMeasurementLeaseWaiting(t, dataResult)
		smartAssertMeasurementLoad(t, s, 0, 0)
		close(secondBurst)
		smartAssertMeasurementLeaseWaiting(t, dataResult)
		close(finishCohort)
		if err := <-cohortDone; err != nil {
			t.Fatal(err)
		}
		smartTakeMeasurementLease(t, dataResult).release()
	})
}

func TestSmartMeasurementLeaseWaitSpendsOriginalDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &Smart{clock: smartRealClock{}}
		first, err := s.acquireMeasurementLease(context.Background(), false)
		if err != nil {
			t.Fatal(err)
		}
		defer first.release()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		burstResult := smartStartMeasurementLease(s, ctx, true)
		smartAssertMeasurementLeaseWaiting(t, burstResult)
		dataResult := smartStartMeasurementLease(s, context.Background(), false)
		smartAssertMeasurementLeaseWaiting(t, dataResult)

		// Advance only the bubble's fake time; the existing holder remains active.
		<-ctx.Done()
		attempt := <-burstResult
		if !errors.Is(attempt.err, context.DeadlineExceeded) || attempt.lease != nil {
			t.Fatalf("expired waiter = %v, %v", attempt.lease, attempt.err)
		}
		data := smartTakeMeasurementLease(t, dataResult)
		smartAssertMeasurementLoad(t, s, 2, 2)
		data.release()
		first.release()
		smartAssertMeasurementLoad(t, s, 2, 0)
	})
}

func TestSmartMeasurementLeaseCancellationKeepsExecutingPhaseUntilRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &Smart{clock: smartRealClock{}, kick: make(chan struct{}, 1)}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		data, err := s.acquireMeasurementLease(ctx, false)
		if err != nil {
			t.Fatal(err)
		}
		defer data.release()
		select {
		case <-s.kick:
		default:
			t.Fatal("data start did not wake the owner")
		}
		burstResult := smartStartMeasurementLease(s, context.Background(), true)
		smartAssertMeasurementLeaseWaiting(t, burstResult)

		cancel()
		if !errors.Is(data.context().Err(), context.Canceled) {
			t.Fatal("data lease lost its parent cancellation")
		}
		smartAssertMeasurementLeaseWaiting(t, burstResult)
		smartAssertMeasurementLoad(t, s, 1, 1)
		data.release()
		data.release()
		smartAssertMeasurementLoad(t, s, 1, 0)
		select {
		case <-s.kick:
		default:
			t.Fatal("data end did not wake the owner")
		}
		smartTakeMeasurementLease(t, burstResult).release()
	})
}

func TestSmartMeasurementLeaseGateCheckedAtExecutionAndSkipsOnlyBlockedData(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, _ := smartTestGroup()
		defer s.cancel()
		blocker, err := s.acquireMeasurementLease(context.Background(), true)
		if err != nil {
			t.Fatal(err)
		}
		defer blocker.release()
		dataResult := smartStartMeasurementLease(s, context.Background(), false)
		smartAssertMeasurementLeaseWaiting(t, dataResult)

		s.mu.Lock()
		s.measurementPrimaryGate = s.primary
		s.wakeMeasurementLoadLocked()
		s.mu.Unlock()
		blocker.release()
		smartAssertMeasurementLeaseWaiting(t, dataResult)
		burst := smartTakeMeasurementLease(t, smartStartMeasurementLease(s, context.Background(), true))
		laterBurstResult := smartStartMeasurementLease(s, context.Background(), true)
		smartAssertMeasurementLeaseWaiting(t, laterBurstResult)

		s.mu.Lock()
		s.measurementPrimaryGate = nil
		s.wakeMeasurementLoadLocked()
		s.mu.Unlock()
		laterDataResult := smartStartMeasurementLease(s, context.Background(), false)
		smartAssertMeasurementLeaseWaiting(t, laterDataResult)
		burst.release()
		data := smartTakeMeasurementLease(t, dataResult)
		smartAssertMeasurementLeaseWaiting(t, laterBurstResult)
		smartAssertMeasurementLeaseWaiting(t, laterDataResult)
		data.release()
		laterBurst := smartTakeMeasurementLease(t, laterBurstResult)
		smartAssertMeasurementLeaseWaiting(t, laterDataResult)
		laterBurst.release()
		smartTakeMeasurementLease(t, laterDataResult).release()
		smartAssertMeasurementLoad(t, s, 2, 0)
	})
}

func TestSmartMeasurementLeaseOwnerCancellationAndReleasedContext(t *testing.T) {
	s := &Smart{clock: smartRealClock{}}
	data, err := s.acquireMeasurementLease(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer data.release()
	s.mu.Lock()
	cancel := s.measurementLoad.executing[data]
	s.mu.Unlock()
	if cancel == nil {
		t.Fatal("executing data lease has no owner cancellation function")
	}
	cancel()
	if !errors.Is(data.context().Err(), context.Canceled) {
		t.Fatal("owner cancellation did not reach the executing phase")
	}
	smartAssertMeasurementLoad(t, s, 1, 1)
	data.release()
	smartAssertMeasurementLoad(t, s, 1, 0)

	burst, err := s.acquireMeasurementLease(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	burst.release()
	if !errors.Is(burst.context().Err(), context.Canceled) {
		t.Fatal("release did not cancel the lease context")
	}
}

func TestSmartMeasurementLeaseCanceledWaiterDoesNotOccupyQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &Smart{clock: smartRealClock{}}
		burst, err := s.acquireMeasurementLease(context.Background(), true)
		if err != nil {
			t.Fatal(err)
		}
		defer burst.release()
		ctx, cancel := context.WithCancel(context.Background())
		dataResult := smartStartMeasurementLease(s, ctx, false)
		smartAssertMeasurementLeaseWaiting(t, dataResult)
		laterBurstResult := smartStartMeasurementLease(s, context.Background(), true)
		smartAssertMeasurementLeaseWaiting(t, laterBurstResult)
		cancel()
		attempt := <-dataResult
		if !errors.Is(attempt.err, context.Canceled) || attempt.lease != nil {
			t.Fatalf("canceled waiter = %v, %v", attempt.lease, attempt.err)
		}
		burst.release()
		smartTakeMeasurementLease(t, laterBurstResult).release()
		smartAssertMeasurementLoad(t, s, 0, 0)
		if lease, err := s.acquireMeasurementLease(ctx, false); lease != nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("already canceled acquisition = %v, %v", lease, err)
		}
		smartTakeMeasurementLease(t, smartStartMeasurementLease(s, context.Background(), false)).release()
		smartAssertMeasurementLoad(t, s, 1, 0)
	})
}
