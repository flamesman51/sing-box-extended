package adapter

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	M "github.com/sagernet/sing/common/metadata"
)

func TestSmartDecisionCopiesSelections(t *testing.T) {
	group := &smartSeamTestGroup{tag: "smart"}
	selections := []SmartSelection{{Group: group, Member: "winner", Generation: 7}}
	decision := NewSmartDecision(group, selections)
	selections[0].Member = "changed"
	copy := decision.Selections()
	copy[0].Generation = 9
	if decision.Outbound() != group || decision.Selections()[0].Member != "winner" || decision.Selections()[0].Generation != 7 {
		t.Fatal("decision changed through an input or accessor slice")
	}
}

func TestSmartAttachmentClosesWholeChainOnce(t *testing.T) {
	first := &smartSeamTestGroup{tag: "first"}
	second := &smartSeamTestGroup{tag: "second"}
	closer := &smartSeamTestCloser{}
	decision := NewSmartDecision(nil, []SmartSelection{{Group: first}, {Group: second}, {Group: first}})
	lease := AttachSmartDecision(decision, closer)
	lease.CountRead(12)
	lease.CountWrite(34)
	lease.CountRead(-1)
	if first.attaches != 1 || second.attaches != 1 {
		t.Fatal("groups were not attached exactly once")
	}
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() { _ = first.closer.Close() })
	}
	workers.Wait()
	_ = lease.Close()
	lease.CountWrite(99)
	if closer.closes.Load() != 1 {
		t.Fatal("underlying closer was not closed exactly once")
	}
	for _, group := range []*smartSeamTestGroup{first, second} {
		if group.lease.closes.Load() != 1 || group.lease.read.Load() != 12 || group.lease.write.Load() != 34 {
			t.Fatal("incorrect lease counts or unregister count")
		}
	}
}

func TestSmartAttachmentHandlesSynchronousRevocation(t *testing.T) {
	first := &smartSeamTestGroup{tag: "first"}
	late := &smartSeamTestGroup{tag: "late", revoke: true}
	last := &smartSeamTestGroup{tag: "last"}
	closer := &smartSeamTestCloser{}
	lease := AttachSmartDecision(NewSmartDecision(nil, []SmartSelection{{Group: first}, {Group: late}, {Group: last}}), closer)
	_ = lease.Close()
	if closer.closes.Load() != 1 {
		t.Fatal("late attachment did not close the connection exactly once")
	}
	for _, group := range []*smartSeamTestGroup{first, late, last} {
		if group.lease.closes.Load() != 1 {
			t.Fatal("a lease returned during revocation remained registered")
		}
	}
}

func TestSmartConnCountsAndUnregisters(t *testing.T) {
	group := &smartSeamTestGroup{tag: "smart"}
	conn := &smartSeamTestConn{reader: bytes.NewReader([]byte("reply"))}
	wrapped := WrapSmartConn(conn, NewSmartDecision(group, []SmartSelection{{Group: group}}))
	buffer := make([]byte, 32)
	if n, err := wrapped.Read(buffer); err != nil || n != 5 {
		t.Fatalf("read: %d, %v", n, err)
	}
	if n, err := wrapped.Write([]byte("request")); err != nil || n != 7 {
		t.Fatalf("write: %d, %v", n, err)
	}
	_ = wrapped.Close()
	_ = wrapped.Close()
	if group.lease.read.Load() != 5 || group.lease.write.Load() != 7 || group.lease.closes.Load() != 1 || conn.closes.Load() != 1 {
		t.Fatal("stream counts or close propagation were incorrect")
	}
}

func TestSmartPacketConnCountsAndUnregisters(t *testing.T) {
	group := &smartSeamTestGroup{tag: "smart"}
	conn := &smartSeamTestPacketConn{}
	wrapped := WrapSmartPacketConn(conn, NewSmartDecision(group, []SmartSelection{{Group: group}}))
	buffer := make([]byte, 32)
	if n, _, err := wrapped.ReadFrom(buffer); err != nil || n != 5 {
		t.Fatalf("read: %d, %v", n, err)
	}
	if n, err := wrapped.WriteTo([]byte("request"), &net.UDPAddr{IP: net.IPv4(1, 1, 1, 1), Port: 443}); err != nil || n != 7 {
		t.Fatalf("write: %d, %v", n, err)
	}
	_ = wrapped.Close()
	_ = wrapped.Close()
	if group.lease.read.Load() != 5 || group.lease.write.Load() != 7 || group.lease.closes.Load() != 1 || conn.closes.Load() != 1 {
		t.Fatal("packet counts or close propagation were incorrect")
	}
}

type smartSeamTestGroup struct {
	OutboundGroup
	tag      string
	attaches int
	closer   io.Closer
	lease    *smartSeamTestLease
	revoke   bool
}

func (g *smartSeamTestGroup) Tag() string { return g.tag }

func (g *smartSeamTestGroup) SelectMember(context.Context, string, M.Socksaddr) (SmartDecision, error) {
	return NewSmartDecision(g, []SmartSelection{{Group: g}}), nil
}

func (g *smartSeamTestGroup) Attach(_ SmartDecision, closer io.Closer) SmartLease {
	g.attaches++
	g.closer = closer
	g.lease = &smartSeamTestLease{}
	if g.revoke {
		_ = closer.Close()
	}
	return g.lease
}

type smartSeamTestLease struct {
	read, write, closes atomic.Int64
}

func (l *smartSeamTestLease) CountRead(n int)  { l.read.Add(int64(n)) }
func (l *smartSeamTestLease) CountWrite(n int) { l.write.Add(int64(n)) }
func (l *smartSeamTestLease) Close() error {
	l.closes.Add(1)
	return nil
}

type smartSeamTestCloser struct{ closes atomic.Int64 }

func (c *smartSeamTestCloser) Close() error {
	c.closes.Add(1)
	return nil
}

type smartSeamTestConn struct {
	net.Conn
	reader *bytes.Reader
	writer bytes.Buffer
	closes atomic.Int64
}

func (c *smartSeamTestConn) Read(p []byte) (int, error)  { return c.reader.Read(p) }
func (c *smartSeamTestConn) Write(p []byte) (int, error) { return c.writer.Write(p) }
func (c *smartSeamTestConn) Close() error {
	c.closes.Add(1)
	return nil
}

type smartSeamTestPacketConn struct {
	net.PacketConn
	closes atomic.Int64
}

func (c *smartSeamTestPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	return copy(p, "reply"), &net.UDPAddr{IP: net.IPv4(1, 1, 1, 1), Port: 443}, nil
}

func (c *smartSeamTestPacketConn) WriteTo(p []byte, _ net.Addr) (int, error) {
	return len(p), nil
}

func (c *smartSeamTestPacketConn) Close() error {
	c.closes.Add(1)
	return nil
}
