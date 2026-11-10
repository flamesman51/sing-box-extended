package group

import "context"

type smartMeasurementLoad struct {
	executing  map[*smartMeasurementLease]context.CancelFunc
	epoch      uint64
	exclusive  *smartMeasurementLease
	head, tail *smartMeasurementLease
	changed    chan struct{}
}

type smartMeasurementLease struct {
	s          *Smart
	ctx        context.Context
	cancel     context.CancelFunc
	exclusive  bool
	held       bool
	prev, next *smartMeasurementLease
}

func (s *Smart) acquireMeasurementLease(ctx context.Context, exclusive bool) (*smartMeasurementLease, error) {
	s.mu.Lock()
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	load := &s.measurementLoad
	lease := &smartMeasurementLease{s: s, exclusive: exclusive, prev: load.tail}
	if load.tail == nil {
		load.head = lease
	} else {
		load.tail.next = lease
	}
	load.tail = lease
	for {
		if err := ctx.Err(); err != nil {
			load.removeWaiter(lease)
			s.wakeMeasurementLoadLocked()
			s.mu.Unlock()
			return nil, err
		}
		ready := load.exclusive == nil
		if ready {
			next := load.head
			if s.measurementGateClosedLocked() {
				// Gated data keeps its place but cannot block unloaded RTT work.
				for next != nil && !next.exclusive {
					next = next.next
				}
			}
			ready = next == lease && (!exclusive || len(load.executing) == 0)
		}
		if ready {
			load.removeWaiter(lease)
			lease.ctx, lease.cancel = context.WithCancel(ctx)
			lease.held = true
			if exclusive {
				load.exclusive = lease
			} else {
				if load.executing == nil {
					load.executing = make(map[*smartMeasurementLease]context.CancelFunc)
				}
				now := s.clock.Now()
				s.observeAllOutagesLocked(now)
				load.executing[lease] = lease.cancel
				load.epoch++
				s.observeAllOutagesLocked(now)
				select {
				case s.kick <- struct{}{}:
				default:
				}
			}
			s.wakeMeasurementLoadLocked()
			s.mu.Unlock()
			return lease, nil
		}
		if load.changed == nil {
			load.changed = make(chan struct{})
		}
		changed := load.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		s.mu.Lock()
	}
}

func (load *smartMeasurementLoad) removeWaiter(lease *smartMeasurementLease) {
	if lease.prev == nil {
		load.head = lease.next
	} else {
		lease.prev.next = lease.next
	}
	if lease.next == nil {
		load.tail = lease.prev
	} else {
		lease.next.prev = lease.prev
	}
	lease.prev, lease.next = nil, nil
}

// Cancellation stops I/O; the holder must release only after that I/O has ended.
func (lease *smartMeasurementLease) release() {
	s := lease.s
	s.mu.Lock()
	if !lease.held {
		s.mu.Unlock()
		return
	}
	lease.held = false
	if lease.exclusive {
		s.measurementLoad.exclusive = nil
	} else {
		now := s.clock.Now()
		s.observeAllOutagesLocked(now)
		delete(s.measurementLoad.executing, lease)
		s.observeAllOutagesLocked(now)
		select {
		case s.kick <- struct{}{}:
		default:
		}
	}
	s.wakeMeasurementLoadLocked()
	s.mu.Unlock()
	lease.cancel()
}

func (lease *smartMeasurementLease) context() context.Context {
	return lease.ctx
}

func (s *Smart) wakeMeasurementLoadLocked() {
	if s.measurementLoad.changed != nil {
		close(s.measurementLoad.changed)
		s.measurementLoad.changed = nil
	}
}
