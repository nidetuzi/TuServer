package ws

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/nbio/nbhttp"
	"github.com/lesismal/nbio/nbhttp/websocket"

	"tuserv/pkg/log"
	"tuserv/pkg/protocol"
)

// Server 是面向客户端的 WebSocket 入口。
type Server struct {
	engine *nbhttp.Engine
	codec  protocol.Codec
	handle protocol.Handler
	idle   time.Duration
	addr   string
	seq    atomic.Uint64

	// peers 在 OnOpen 里发布已经填好的连接。
	// nbio 的 Session 读写标了 norace，OnMessage/OnClose 不能靠它和 OnOpen 建立同步。
	peersMu sync.Mutex
	peers   map[*websocket.Conn]*wsPeer
}

type wsPeer struct {
	id   string
	conn *websocket.Conn
	code protocol.Codec
	q    chan protocol.Packet

	mu       sync.Mutex
	closed   bool
	closeErr error
}

// Serve 在 addr 上提供 /ws。客户端用二进制消息发送与 TCP 相同的帧。
// idle 是读空闲上限，TCP 和 WebSocket 用同一个配置。小于等于 0 时关掉库自带的 120 秒保活，避免没配超时却偷偷断开。
func Serve(addr string, codec protocol.Codec, handle protocol.Handler, idle time.Duration) (*Server, error) {
	if codec == nil {
		codec = protocol.FrameCodec{}
	}
	s := &Server{
		codec:  codec,
		handle: handle,
		idle:   idle,
		peers:  make(map[*websocket.Conn]*wsPeer),
	}
	upgrader := websocket.NewUpgrader()
	// KeepaliveTime 会在收到每条消息后重设读截止时间，半开连接到期由 nbio 关闭。
	upgrader.KeepaliveTime = idle
	upgrader.OnOpen(func(c *websocket.Conn) {
		p := &wsPeer{
			id:   strconv.FormatUint(s.seq.Add(1), 10),
			conn: c,
			code: s.codec,
			q:    make(chan protocol.Packet, 128),
		}
		// 先发布，再交给读回调。字段写入在这次解锁之前，OnMessage/OnClose 加锁取出后才能读。
		s.peersMu.Lock()
		s.peers[c] = p
		s.peersMu.Unlock()
		c.SetSession(p)
		go p.loop(s.handle)
		if s.handle != nil {
			s.handle.OnOpen(p)
		}
	})
	upgrader.OnMessage(func(c *websocket.Conn, _ websocket.MessageType, data []byte) {
		s.peersMu.Lock()
		p := s.peers[c]
		s.peersMu.Unlock()
		if p == nil {
			return
		}
		raw := append([]byte(nil), data...)
		pkt, _, err := s.codec.Decode(raw)
		if err != nil {
			log.Warn("ws frame", "err", err, "session_id", p.id)
			_ = c.Close()
			return
		}
		closed, full := p.enqueue(pkt)
		if closed {
			return
		}
		if full {
			log.Warn("ws queue full", "session_id", p.id)
			_ = c.Close()
		}
	})
	upgrader.OnClose(func(c *websocket.Conn, err error) {
		s.peersMu.Lock()
		p := s.peers[c]
		delete(s.peers, c)
		s.peersMu.Unlock()
		if p == nil {
			return
		}
		// 这里只关队列。OnClose 放到 loop 退出之后，才能读到 OnPacket 刚写回的 playerID。
		p.shutdown(err)
	})
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		if _, err := upgrader.Upgrade(w, r, nil); err != nil {
			log.Warn("ws upgrade", "err", err)
		}
	})
	engine := nbhttp.NewEngine(nbhttp.Config{
		Network: "tcp",
		Addrs:   []string{addr},
		Handler: mux,
		Listen:  s.listen,
	})
	if err := engine.Start(); err != nil {
		return nil, err
	}
	s.engine = engine
	return s, nil
}

// Stop 停止 WebSocket 监听。
// 必须带上截止时间：nbhttp 在 ctx 为空时会直接解引用，进程退出就会 panic。
func (s *Server) Stop() {
	if s.engine == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = s.engine.Shutdown(ctx)
}

// Addr 返回实际监听地址。端口写 0 时用它拿到系统分配的端口。
func (s *Server) Addr() string { return s.addr }

func (s *Server) listen(network, addr string) (net.Listener, error) {
	ln, err := net.Listen(network, addr)
	if err != nil {
		return nil, err
	}
	s.addr = ln.Addr().String()
	return ln, nil
}

// ID 在网关会话表里标识这条连接。
func (p *wsPeer) ID() string { return p.id }

// WritePacket 把一帧写成一条二进制消息。可与读协程并发调用。
func (p *wsPeer) WritePacket(pkt protocol.Packet) error {
	buf, err := p.code.Encode(pkt)
	if err != nil {
		return err
	}
	return p.conn.WriteMessage(websocket.BinaryMessage, buf)
}

// Close 主动断开客户端。
func (p *wsPeer) Close() error { return p.conn.Close() }

// enqueue 把一帧交给会话协程。closed 表示连接已经在关，不能再往已关闭的队列里送。
func (p *wsPeer) enqueue(pkt protocol.Packet) (closed, full bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return true, false
	}
	select {
	case p.q <- pkt:
		return false, false
	default:
		return false, true
	}
}

// shutdown 关闭队列并记下断线原因。可与 enqueue 并发，只生效一次。
// closeErr 写在 close(q) 之前：loop 因队列关闭而退出时，一定能看见这次写入。
func (p *wsPeer) shutdown(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	p.closeErr = err
	close(p.q)
}

func (p *wsPeer) loop(h protocol.Handler) {
	for pkt := range p.q {
		if h != nil {
			h.OnPacket(p, pkt)
		}
	}
	if h == nil {
		return
	}
	// 队列关闭已经和 shutdown 同步；再拿锁读取 closeErr，避免和写入分在两个协程上却没有同步。
	p.mu.Lock()
	err := p.closeErr
	p.mu.Unlock()
	// 和 OnPacket 同一条协程、且排在所有已入队帧之后，断线通知里的 playerID 才是最新的，也只会发一次。
	h.OnClose(p, err)
}
