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
}

type wsPeer struct {
	id   string
	conn *websocket.Conn
	code protocol.Codec
	once sync.Once
	q    chan protocol.Packet
}

// Serve 在 addr 上提供 /ws。客户端用二进制消息发送与 TCP 相同的帧。
// idle 是读空闲上限，TCP 和 WebSocket 用同一个配置。小于等于 0 时关掉库自带的 120 秒保活，避免没配超时却偷偷断开。
func Serve(addr string, codec protocol.Codec, handle protocol.Handler, idle time.Duration) (*Server, error) {
	if codec == nil {
		codec = protocol.FrameCodec{}
	}
	s := &Server{codec: codec, handle: handle, idle: idle}
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
		c.SetSession(p)
		go p.loop(s.handle)
		if s.handle != nil {
			s.handle.OnOpen(p)
		}
	})
	upgrader.OnMessage(func(c *websocket.Conn, _ websocket.MessageType, data []byte) {
		v := c.Session()
		p, ok := v.(*wsPeer)
		if !ok || p == nil {
			return
		}
		raw := append([]byte(nil), data...)
		pkt, _, err := s.codec.Decode(raw)
		if err != nil {
			log.Warn("ws frame", "err", err, "session_id", p.id)
			_ = c.Close()
			return
		}
		select {
		case p.q <- pkt:
		default:
			log.Warn("ws queue full", "session_id", p.id)
			_ = c.Close()
		}
	})
	upgrader.OnClose(func(c *websocket.Conn, err error) {
		v := c.Session()
		p, ok := v.(*wsPeer)
		if !ok || p == nil {
			return
		}
		p.once.Do(func() { close(p.q) })
		if s.handle != nil {
			s.handle.OnClose(p, err)
		}
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

func (p *wsPeer) loop(h protocol.Handler) {
	for pkt := range p.q {
		if h != nil {
			h.OnPacket(p, pkt)
		}
	}
}
