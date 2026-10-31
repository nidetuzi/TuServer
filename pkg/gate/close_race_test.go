package gate

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"tuserv/pkg/cluster"
	"tuserv/pkg/logic"
	"tuserv/pkg/protocol"
	"tuserv/pkg/protocol/tcp"
	"tuserv/pkg/protocol/ws"
)

type sentReq struct {
	host string
	req  logic.Request
}

// holdFwd 让第二帧的转发停住，用来把连接关闭插进正在执行的 OnPacket。
type holdFwd struct {
	mu      sync.Mutex
	sent    []sentReq
	calls   int
	picks   int
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newHoldFwd() *holdFwd {
	return &holdFwd{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (f *holdFwd) Pick(context.Context) (cluster.Instance, error) {
	f.mu.Lock()
	f.picks++
	f.mu.Unlock()
	return cluster.Instance{Addr: "10.0.0.8:1", Weight: 1}, nil
}

func (f *holdFwd) Dispatch(_ context.Context, host string, req logic.Request) (logic.Response, error) {
	if req.CmdID == logic.CmdDisconnect {
		f.add(host, req)
		return logic.Response{}, nil
	}
	f.mu.Lock()
	f.calls++
	n := f.calls
	f.mu.Unlock()
	playerID := int64(42)
	if n >= 2 {
		playerID = 99
		f.once.Do(func() { close(f.started) })
		<-f.release
	}
	f.add(host, req)
	return logic.Response{PlayerID: playerID, CmdID: req.CmdID, Body: []byte("ok")}, nil
}

func (f *holdFwd) add(host string, req logic.Request) {
	f.mu.Lock()
	f.sent = append(f.sent, sentReq{host: host, req: req})
	f.mu.Unlock()
}

func (f *holdFwd) snapshot() (picks int, sent []sentReq) {
	f.mu.Lock()
	defer f.mu.Unlock()
	sent = make([]sentReq, len(f.sent))
	copy(sent, f.sent)
	return f.picks, sent
}

func onlyDisconnects(sent []sentReq) []sentReq {
	var out []sentReq
	for _, s := range sent {
		if s.req.CmdID == logic.CmdDisconnect {
			out = append(out, s)
		}
	}
	return out
}

func TestCloseDuringLoginCarriesLatestPlayerID(t *testing.T) {
	t.Run("tcp", func(t *testing.T) {
		testCloseDuringLogin(t,
			func(h protocol.Handler) (string, func(), error) {
				srv, err := tcp.Serve("127.0.0.1:0", protocol.FrameCodec{}, h, 0)
				if err != nil {
					return "", nil, err
				}
				return srv.Addr(), srv.Stop, nil
			},
			func(t *testing.T, addr string) net.Conn {
				c, err := net.Dial("tcp", addr)
				if err != nil {
					t.Fatal(err)
				}
				return c
			},
			func(t *testing.T, c net.Conn, payload []byte) {
				if _, err := c.Write(payload); err != nil {
					t.Fatal(err)
				}
			},
		)
	})
	t.Run("ws", func(t *testing.T) {
		testCloseDuringLogin(t,
			func(h protocol.Handler) (string, func(), error) {
				srv, err := ws.Serve("127.0.0.1:0", protocol.FrameCodec{}, h, 0)
				if err != nil {
					return "", nil, err
				}
				return srv.Addr(), srv.Stop, nil
			},
			func(t *testing.T, addr string) net.Conn { return dialWS(t, addr) },
			func(t *testing.T, c net.Conn, payload []byte) { writeWS(t, c, payload) },
		)
	})
}

func testCloseDuringLogin(t *testing.T, serve func(protocol.Handler) (string, func(), error), dial func(*testing.T, string) net.Conn, write func(*testing.T, net.Conn, []byte)) {
	t.Helper()
	fwd := newHoldFwd()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(fwd.release) }) }
	g := New("127.0.0.1:9001", fwd, nil)
	addr, stop, err := serve(g)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		release()
		stop()
	})
	conn := dial(t, addr)
	defer conn.Close()
	go func() {
		buf := make([]byte, 256)
		for {
			if _, err := conn.Read(buf); err != nil {
				return
			}
		}
	}()
	codec := protocol.FrameCodec{}
	first, err := codec.Encode(protocol.Packet{CmdID: 1001, ServerID: 5, Body: []byte{1}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := codec.Encode(protocol.Packet{CmdID: 1002, ServerID: 5, Body: []byte{2}})
	if err != nil {
		t.Fatal(err)
	}
	write(t, conn, first)
	write(t, conn, second)
	select {
	case <-fwd.started:
	case <-time.After(2 * time.Second):
		t.Fatal("第二帧没有进入转发")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	// 留给 poller 时间先跑到关闭回调。会话协程还停在转发里，这时不能先把断线发出去。
	time.Sleep(100 * time.Millisecond)
	if _, sent := fwd.snapshot(); len(onlyDisconnects(sent)) != 0 {
		t.Fatalf("登录回包还没写回 playerID 就发出了断线: %+v", sent)
	}
	release()
	deadline := time.Now().Add(2 * time.Second)
	var picks int
	var sent []sentReq
	for {
		picks, sent = fwd.snapshot()
		if len(onlyDisconnects(sent)) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("断线通知没有发出: %+v", sent)
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	picks, sent = fwd.snapshot()
	discs := onlyDisconnects(sent)
	if len(discs) != 1 {
		t.Fatalf("断线应只发一次: %+v", sent)
	}
	if picks != 1 {
		t.Fatalf("登录过程中断线不应重新选点: picks=%d", picks)
	}
	got := discs[0]
	if got.host != "10.0.0.8:1" || !got.req.Internal || got.req.CmdID != logic.CmdDisconnect || got.req.PlayerID != 99 || got.req.ServerID != 5 || got.req.GateID != "127.0.0.1:9001" || got.req.SessionID == "" {
		t.Fatalf("断线通知应带上最后一次回包的角色和原来的逻辑节点: %+v", got)
	}
	var business int
	for _, s := range sent {
		if s.req.CmdID == logic.CmdDisconnect {
			continue
		}
		business++
		if s.req.SessionID != got.req.SessionID || s.req.Internal {
			t.Fatalf("业务帧和断线不是同一条会话: %+v", s.req)
		}
	}
	if business != 2 {
		t.Fatalf("两帧业务都应在断线前转发完: %+v", sent)
	}
	if sent[len(sent)-1].req.CmdID != logic.CmdDisconnect {
		t.Fatalf("断线应排在已入队的业务帧之后: %+v", sent)
	}
}

type raceFwd struct {
	mu   sync.Mutex
	sent []sentReq
}

func (f *raceFwd) Pick(context.Context) (cluster.Instance, error) {
	return cluster.Instance{Addr: "10.0.0.8:1", Weight: 1}, nil
}

func (f *raceFwd) Dispatch(_ context.Context, host string, req logic.Request) (logic.Response, error) {
	if req.CmdID != logic.CmdDisconnect {
		// 停在 OnPacket 里，让关闭回调和写 playerID 叠在一起。
		time.Sleep(time.Millisecond)
	}
	f.mu.Lock()
	f.sent = append(f.sent, sentReq{host: host, req: req})
	f.mu.Unlock()
	if req.CmdID == logic.CmdDisconnect {
		return logic.Response{}, nil
	}
	return logic.Response{PlayerID: 77, CmdID: req.CmdID, Body: []byte("ok")}, nil
}

func (f *raceFwd) snapshot() []sentReq {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]sentReq, len(f.sent))
	copy(out, f.sent)
	return out
}

func TestOnCloseConcurrentWithOnPacket(t *testing.T) {
	payload, err := protocol.FrameCodec{}.Encode(protocol.Packet{CmdID: 1001, ServerID: 3, Body: []byte{7}})
	if err != nil {
		t.Fatal(err)
	}
	t.Run("tcp", func(t *testing.T) {
		testOnCloseConcurrentWithOnPacket(t, payload,
			func(h protocol.Handler) (string, func(), error) {
				srv, err := tcp.Serve("127.0.0.1:0", protocol.FrameCodec{}, h, 0)
				if err != nil {
					return "", nil, err
				}
				return srv.Addr(), srv.Stop, nil
			},
			func(t *testing.T, addr string) net.Conn {
				c, err := net.Dial("tcp", addr)
				if err != nil {
					t.Fatal(err)
				}
				return c
			},
			func(c net.Conn, payload []byte) error {
				_, err := c.Write(payload)
				return err
			},
		)
	})
	t.Run("ws", func(t *testing.T) {
		testOnCloseConcurrentWithOnPacket(t, payload,
			func(h protocol.Handler) (string, func(), error) {
				srv, err := ws.Serve("127.0.0.1:0", protocol.FrameCodec{}, h, 0)
				if err != nil {
					return "", nil, err
				}
				return srv.Addr(), srv.Stop, nil
			},
			func(t *testing.T, addr string) net.Conn { return dialWS(t, addr) },
			func(c net.Conn, payload []byte) error {
				return writeWSFrame(c, payload)
			},
		)
	})
}

func testOnCloseConcurrentWithOnPacket(t *testing.T, payload []byte, serve func(protocol.Handler) (string, func(), error), dial func(*testing.T, string) net.Conn, write func(net.Conn, []byte) error) {
	t.Helper()
	fwd := &raceFwd{}
	g := New("127.0.0.1:9001", fwd, nil)
	addr, stop, err := serve(g)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	const n = 24
	conns := make([]net.Conn, n)
	for i := range conns {
		conns[i] = dial(t, addr)
	}
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for _, c := range conns {
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			if err := write(c, payload); err != nil {
				errCh <- err
				return
			}
			// 关掉连接是为了和对面的 OnPacket 叠在一起。对端已经先关时，这里的错误没有意义。
			_ = c.Close()
		}(c)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	var sent []sentReq
	for {
		sent = fwd.snapshot()
		if reason := badDisconnect(sent); reason != "" {
			t.Fatal(reason)
		}
		if disconnectsPaired(sent) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("登录和断线没有一一对应: %s", describePairs(sent))
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	sent = fwd.snapshot()
	if reason := badDisconnect(sent); reason != "" {
		t.Fatal(reason)
	}
	if !disconnectsPaired(sent) {
		t.Fatalf("晚到的回调打乱了登录和断线: %s", describePairs(sent))
	}
}

func badDisconnect(sent []sentReq) string {
	for _, s := range sent {
		if s.req.CmdID != logic.CmdDisconnect {
			if s.req.CmdID != 1001 || s.req.Internal {
				return fmt.Sprintf("并发路径转发了不该转发的帧: %+v", s.req)
			}
			continue
		}
		if s.host != "10.0.0.8:1" || !s.req.Internal || s.req.PlayerID != 77 || s.req.ServerID != 3 || s.req.GateID != "127.0.0.1:9001" {
			return fmt.Sprintf("并发断线通知字段不对: host=%s req=%+v", s.host, s.req)
		}
	}
	return ""
}

func disconnectsPaired(sent []sentReq) bool {
	logins := make(map[string]struct{})
	discs := make(map[string]int)
	for _, s := range sent {
		if s.req.CmdID == logic.CmdDisconnect {
			discs[s.req.SessionID]++
			continue
		}
		logins[s.req.SessionID] = struct{}{}
	}
	if len(logins) == 0 || len(logins) != len(discs) {
		return false
	}
	for id, n := range discs {
		if _, ok := logins[id]; !ok || n != 1 {
			return false
		}
	}
	return true
}

func describePairs(sent []sentReq) string {
	logins := 0
	discs := 0
	for _, s := range sent {
		if s.req.CmdID == logic.CmdDisconnect {
			discs++
			continue
		}
		logins++
	}
	return fmt.Sprintf("logins=%d discs=%d sent=%d", logins, discs, len(sent))
}

func writeWSFrame(c net.Conn, payload []byte) error {
	if len(payload) > 125 {
		return fmt.Errorf("测试帧过长: %d", len(payload))
	}
	mask := [4]byte{0x11, 0x22, 0x33, 0x44}
	buf := make([]byte, 6+len(payload))
	buf[0] = 0x82
	buf[1] = byte(len(payload)) | 0x80
	copy(buf[2:6], mask[:])
	for i, b := range payload {
		buf[6+i] = b ^ mask[i%4]
	}
	_, err := c.Write(buf)
	return err
}
