package ws

import (
	"bufio"
	"io"
	"net"
	"strings"
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

func (h *testHandler) OnOpen(protocol.Peer, string)            {}
func (h *testHandler) OnPacket(protocol.Peer, protocol.Packet) { h.packets.Add(1) }
func (h *testHandler) OnClose(protocol.Peer, error)            { h.closes.Add(1) }

func TestPeerShutdownWaitsForInFlightPacket(t *testing.T) {
	h := &orderedHandler{started: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}
	p := &wsPeer{q: make(chan protocol.Packet, 2)}
	p.q <- protocol.Packet{CmdID: 1}
	p.q <- protocol.Packet{CmdID: 2}
	ready := make(chan struct{})
	close(ready)
	go p.loop(h, ready)
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

func (h *orderedHandler) OnOpen(protocol.Peer, string) {}
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
	ready := make(chan struct{})
	close(ready)
	p.loop(h, ready)
	if h.packets.Load() != 0 || h.closes.Load() != 1 {
		t.Fatalf("关闭后应丢弃排队帧并只回调一次: packets=%d closes=%d", h.packets.Load(), h.closes.Load())
	}
}

func TestOpenReceivesRequestURIBeforePackets(t *testing.T) {
	h := &openOrderHandler{opened: make(chan string, 2), packets: make(chan struct{}, 2)}
	s, err := Serve("127.0.0.1:0", protocol.FrameCodec{}, h, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	uris := []string{"/ws?sid=12&token=a", "/ws?token=b&sid=34"}
	for _, want := range uris {
		conn := dialWebSocket(t, s.Addr(), want)
		frame, err := (protocol.FrameCodec{}).Encode(protocol.Packet{CmdID: 7})
		if err != nil {
			t.Fatal(err)
		}
		writeWSMessage(t, conn, frame)
		select {
		case got := <-h.opened:
			if got != want {
				t.Fatalf("OnOpen URI 不符: got=%q want=%q", got, want)
			}
		case <-time.After(time.Second):
			t.Fatal("未调用 OnOpen")
		}
		select {
		case <-h.packets:
		case <-time.After(time.Second):
			t.Fatal("OnOpen 返回后未处理业务消息")
		}
		_ = conn.Close()
	}
}

type openOrderHandler struct {
	opened  chan string
	packets chan struct{}
}

func (h *openOrderHandler) OnOpen(_ protocol.Peer, requestURI string) { h.opened <- requestURI }
func (h *openOrderHandler) OnPacket(protocol.Peer, protocol.Packet)   { h.packets <- struct{}{} }
func (h *openOrderHandler) OnClose(protocol.Peer, error)              {}

func dialWebSocket(t *testing.T, addr, requestURI string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	request := "GET " + requestURI + " HTTP/1.1\r\nHost: " + addr + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	status, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(status, "101") {
		t.Fatalf("WebSocket 握手失败: status=%q err=%v", status, err)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
	}
	return &testConn{Conn: conn, reader: reader}
}

type testConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *testConn) Read(p []byte) (int, error) { return c.reader.Read(p) }

func writeWSMessage(t *testing.T, conn net.Conn, payload []byte) {
	t.Helper()
	if len(payload) > 125 {
		t.Fatal("测试帧超过 WebSocket 短帧限制")
	}
	mask := [4]byte{1, 2, 3, 4}
	frame := make([]byte, 6+len(payload))
	frame[0] = 0x82
	frame[1] = byte(len(payload)) | 0x80
	copy(frame[2:6], mask[:])
	for i, b := range payload {
		frame[6+i] = b ^ mask[i%4]
	}
	if _, err := conn.Write(frame); err != nil {
		t.Fatal(err)
	}
}
