package gate

import (
	"bufio"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"tuserv/pkg/cluster"
	"tuserv/pkg/logic"
	"tuserv/pkg/protocol"
	"tuserv/pkg/protocol/tcp"
	"tuserv/pkg/protocol/ws"
)

type idleFwd struct {
	mu    sync.Mutex
	picks int
	reqs  []logic.Request
}

func (f *idleFwd) Pick(context.Context) (cluster.Instance, error) {
	f.mu.Lock()
	f.picks++
	f.mu.Unlock()
	return cluster.Instance{Addr: "127.0.0.1:9101", Weight: 1}, nil
}

func (f *idleFwd) Dispatch(_ context.Context, _ string, req logic.Request) (logic.Response, error) {
	f.mu.Lock()
	f.reqs = append(f.reqs, req)
	f.mu.Unlock()
	if req.CmdID < 0 {
		return logic.Response{}, nil
	}
	return logic.Response{PlayerID: 42, CmdID: req.CmdID, Body: []byte("ok")}, nil
}

func (f *idleFwd) count(pred func(logic.Request) bool) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, req := range f.reqs {
		if pred(req) {
			n++
		}
	}
	return n
}

func TestTCPHeartbeatAndIdleClose(t *testing.T) {
	fwd := &idleFwd{}
	g := New("127.0.0.1:9001", fwd, nil)
	idle := 500 * time.Millisecond
	srv, err := tcp.Serve("127.0.0.1:0", protocol.FrameCodec{}, g, idle)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	silent, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	waitDead(t, silent)

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	codec := protocol.FrameCodec{}
	beat, err := codec.Encode(protocol.Packet{CmdID: protocol.CmdHeartbeat, ServerID: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(beat); err != nil {
		t.Fatal(err)
	}
	got := readFrame(t, conn, codec)
	if got.CmdID != protocol.CmdHeartbeat || got.ServerID != 4 {
		t.Fatalf("心跳应答不对: %+v", got)
	}
	if fwd.count(func(logic.Request) bool { return true }) != 0 || fwd.picks != 0 {
		t.Fatal("心跳不应进入逻辑节点")
	}
	time.Sleep(idle / 2)
	if _, err := conn.Write(beat); err != nil {
		t.Fatal(err)
	}
	got = readFrame(t, conn, codec)
	if got.CmdID != protocol.CmdHeartbeat {
		t.Fatalf("第二次心跳应答不对: %+v", got)
	}

	biz, err := codec.Encode(protocol.Packet{CmdID: 1001, ServerID: 4, Body: []byte{9}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(biz); err != nil {
		t.Fatal(err)
	}
	got = readFrame(t, conn, codec)
	if got.CmdID != 1001 || string(got.Body) != "ok" {
		t.Fatalf("业务应答不对: %+v", got)
	}
	waitDead(t, conn)
	waitCount(t, fwd, func(req logic.Request) bool {
		return req.Internal && req.CmdID == logic.CmdDisconnect
	})
}

func TestWebSocketHeartbeatAndIdleClose(t *testing.T) {
	fwd := &idleFwd{}
	g := New("127.0.0.1:9001", fwd, nil)
	idle := 500 * time.Millisecond
	srv, err := ws.Serve("127.0.0.1:0", protocol.FrameCodec{}, g, idle)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	silent := dialWS(t, srv.Addr())
	defer silent.Close()
	waitDead(t, silent)

	conn := dialWS(t, srv.Addr())
	defer conn.Close()
	codec := protocol.FrameCodec{}
	beat, err := codec.Encode(protocol.Packet{CmdID: protocol.CmdHeartbeat, ServerID: 6})
	if err != nil {
		t.Fatal(err)
	}
	writeWS(t, conn, beat)
	got, _, err := codec.Decode(readWS(t, conn))
	if err != nil {
		t.Fatal(err)
	}
	if got.CmdID != protocol.CmdHeartbeat || got.ServerID != 6 {
		t.Fatalf("心跳应答不对: %+v", got)
	}
	if fwd.count(func(logic.Request) bool { return true }) != 0 {
		t.Fatal("心跳不应进入逻辑节点")
	}
	time.Sleep(idle / 2)
	writeWS(t, conn, beat)
	if _, _, err := codec.Decode(readWS(t, conn)); err != nil {
		t.Fatal(err)
	}
	waitDead(t, conn)
}

func readFrame(t *testing.T, c net.Conn, codec protocol.FrameCodec) protocol.Packet {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 0, 64)
	tmp := make([]byte, 64)
	for {
		pkt, _, err := codec.Decode(buf)
		if err == nil {
			return pkt
		}
		if err != protocol.ErrShortBuffer && len(buf) > 0 {
			t.Fatal(err)
		}
		n, rerr := c.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if rerr != nil {
			t.Fatal(rerr)
		}
	}
}

func waitDead(t *testing.T, c net.Conn) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(4 * time.Second))
	buf := make([]byte, 8)
	n, err := c.Read(buf)
	if err != nil {
		return
	}
	if n >= 1 && buf[0]&0x0f == 0x8 {
		return
	}
	t.Fatalf("空闲连接没有断开: %x", buf[:n])
}

func waitCount(t *testing.T, f *idleFwd, pred func(logic.Request) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f.count(pred) > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("断线后没有通知逻辑节点")
}

func dialWS(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.WriteString(c, "GET /ws HTTP/1.1\r\nHost: "+addr+"\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n")
	if err != nil {
		c.Close()
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	status, err := br.ReadString('\n')
	if err != nil {
		c.Close()
		t.Fatal(err)
	}
	if !strings.Contains(status, "101") {
		c.Close()
		t.Fatalf("握手失败: %s", status)
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			c.Close()
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
	}
	return &readerConn{Conn: c, r: br}
}

type readerConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *readerConn) Read(p []byte) (int, error) { return c.r.Read(p) }

func writeWS(t *testing.T, c net.Conn, payload []byte) {
	t.Helper()
	if len(payload) > 125 {
		t.Fatal("测试帧过长")
	}
	mask := [4]byte{0x11, 0x22, 0x33, 0x44}
	buf := make([]byte, 6+len(payload))
	buf[0] = 0x82
	buf[1] = byte(len(payload)) | 0x80
	copy(buf[2:6], mask[:])
	for i, b := range payload {
		buf[6+i] = b ^ mask[i%4]
	}
	if _, err := c.Write(buf); err != nil {
		t.Fatal(err)
	}
}

func readWS(t *testing.T, c net.Conn) []byte {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(c, hdr); err != nil {
		t.Fatal(err)
	}
	if hdr[0]&0x80 == 0 {
		t.Fatal("服务端帧应带结束位")
	}
	if hdr[0]&0x0f == 0x8 {
		t.Fatal("还没到空闲就收到关闭帧")
	}
	n := int(hdr[1] & 0x7f)
	if hdr[1]&0x80 != 0 || n > 125 {
		t.Fatalf("不期望的帧头: %x", hdr)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(c, payload); err != nil {
		t.Fatal(err)
	}
	return payload
}
