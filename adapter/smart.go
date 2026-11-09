package adapter

import (
	"context"
	"errors"
	"io"
	"net"
	"slices"
	"sync"

	"github.com/sagernet/sing/common/bufio"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type SmartGroup interface {
	OutboundGroup
	SelectMember(ctx context.Context, network string, destination M.Socksaddr) (SmartDecision, error)
	Attach(decision SmartDecision, closer io.Closer) SmartLease
}

type SmartSelection struct {
	Group      SmartGroup
	Member     string
	Generation uint64
}

// SmartDecision records a selection without registering or opening a connection.
type SmartDecision struct {
	outbound   Outbound
	selections []SmartSelection
}

func NewSmartDecision(outbound Outbound, selections []SmartSelection) SmartDecision {
	return SmartDecision{outbound: outbound, selections: slices.Clone(selections)}
}

func (d SmartDecision) Outbound() Outbound {
	return d.outbound
}

func (d SmartDecision) Selections() []SmartSelection {
	return slices.Clone(d.selections)
}

// SmartLease counts traffic for accounting, never as proof of member health.
// Close unregisters the attachment without closing the closer passed to Attach.
type SmartLease interface {
	io.Closer
	CountRead(int)
	CountWrite(int)
}

type smartAttachment struct {
	mu     sync.Mutex
	closer io.Closer
	leases []SmartLease
	closed bool
}

// AttachSmartDecision registers each group once and closes the whole chain on revocation.
func AttachSmartDecision(decision SmartDecision, closer io.Closer) SmartLease {
	attachment := &smartAttachment{closer: closer, leases: make([]SmartLease, 0, len(decision.selections))}
	for index, selection := range decision.selections {
		if selection.Group == nil {
			continue
		}
		duplicate := false
		for _, previous := range decision.selections[:index] {
			if previous.Group == selection.Group {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		lease := selection.Group.Attach(decision, attachment)
		if lease == nil {
			continue
		}
		attachment.mu.Lock()
		closed := attachment.closed
		if !closed {
			attachment.leases = append(attachment.leases, lease)
		}
		attachment.mu.Unlock()
		if closed {
			_ = lease.Close()
		}
	}
	return attachment
}

func (a *smartAttachment) Close() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.closed = true
	leases := a.leases
	a.mu.Unlock()
	var err error
	for _, lease := range leases {
		err = errors.Join(err, lease.Close())
	}
	return errors.Join(err, a.closer.Close())
}

func (a *smartAttachment) CountRead(n int) {
	if n <= 0 {
		return
	}
	a.mu.Lock()
	leases, closed := a.leases, a.closed
	a.mu.Unlock()
	if !closed {
		for _, lease := range leases {
			lease.CountRead(n)
		}
	}
}

func (a *smartAttachment) CountWrite(n int) {
	if n <= 0 {
		return
	}
	a.mu.Lock()
	leases, closed := a.leases, a.closed
	a.mu.Unlock()
	if !closed {
		for _, lease := range leases {
			lease.CountWrite(n)
		}
	}
}

func WrapSmartConn(conn net.Conn, decision SmartDecision) net.Conn {
	if len(decision.selections) == 0 {
		return conn
	}
	lease := AttachSmartDecision(decision, conn)
	return &smartConn{
		CounterConn: bufio.NewCounterConn(conn,
			[]N.CountFunc{func(n int64) { lease.CountRead(int(n)) }},
			[]N.CountFunc{func(n int64) { lease.CountWrite(int(n)) }}),
		lease: lease,
	}
}

type smartConn struct {
	*bufio.CounterConn
	lease SmartLease
}

func (c *smartConn) Close() error {
	return c.lease.Close()
}

func WrapSmartPacketConn(conn net.PacketConn, decision SmartDecision) net.PacketConn {
	if len(decision.selections) == 0 {
		return conn
	}
	lease := AttachSmartDecision(decision, conn)
	return bufio.NewNetPacketConn(&smartPacketConn{
		CounterPacketConn: bufio.NewCounterPacketConn(bufio.NewPacketConn(conn),
			[]N.CountFunc{func(n int64) { lease.CountRead(int(n)) }},
			[]N.CountFunc{func(n int64) { lease.CountWrite(int(n)) }}),
		lease: lease,
	})
}

type smartPacketConn struct {
	*bufio.CounterPacketConn
	lease SmartLease
}

func (c *smartPacketConn) Close() error {
	return c.lease.Close()
}
