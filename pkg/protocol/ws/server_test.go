package ws

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"tuserv/pkg/protocol"
)

type testHandler struct {
	packets atomic.Int32
	closes  atomic.Int32
}

func (h *testHandler) OnOpen(protocol.Peer)                    {}
func (h *testHandler) OnPacket(protocol.Peer, protocol.Packet) { h.packets.Add(1) }
func (h *testHandler) OnClose(protocol.Peer, error)            { h.closes.Add(1) }

func TestPeerShutdownWaitsForInFlightPacket(t *testing.T) {
	h := &orderedHandler{started: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}
	p := &wsPeer{q: make(chan protocol.Packet, 2)}
	p.q <- protocol.Packet{CmdID: 1}
	p.q <- protocol.Packet{CmdID: 2}
	go p.loop(h)
	<-h.started
	p.shutdown(nil)
	select {
	case <-h.closed:
		t.Fatal("在途帧完成前不应调用 OnClose")
	case <-time.After(10 * time.Millisecond):
	}
	close(h.release)
	<-h.closed
	if h.packets.Load() != 1 {
		t.Fatalf("关闭后排队帧不应处理: packets=%d", h.packets.Load())
	}
}

type orderedHandler struct {
	packets atomic.Int32
	started chan struct{}
	release chan struct{}
	closed  chan struct{}
}

func (h *orderedHandler) OnOpen(protocol.Peer) {}
func (h *orderedHandler) OnPacket(protocol.Peer, protocol.Packet) {
	h.packets.Add(1)
	close(h.started)
	<-h.release
}
func (h *orderedHandler) OnClose(protocol.Peer, error) { close(h.closed) }

func TestPeerDropsQueuedPacketsAfterShutdown(t *testing.T) {
	h := &testHandler{}
	p := &wsPeer{q: make(chan protocol.Packet, 2), mu: sync.Mutex{}}
	p.q <- protocol.Packet{CmdID: 1}
	p.q <- protocol.Packet{CmdID: 2}
	p.shutdown(nil)
	p.loop(h)
	if h.packets.Load() != 0 || h.closes.Load() != 1 {
		t.Fatalf("关闭后应丢弃排队帧并只回调一次: packets=%d closes=%d", h.packets.Load(), h.closes.Load())
	}
}
