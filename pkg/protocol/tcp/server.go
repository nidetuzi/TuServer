package tcp

import (
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/nbio"

	"tuserv/pkg/log"
	"tuserv/pkg/protocol"
)

// Server 是面向客户端的 TCP 入口。Kitex 端口不走这里。
type Server struct {
	engine *nbio.Engine
	codec  protocol.Codec
	handle protocol.Handler
	idle   time.Duration
	addr   string
	seq    atomic.Uint64
}

type tcpPeer struct {
	id   string
	conn *nbio.Conn
	code protocol.Codec
	asm  *protocol.Assembler
	once sync.Once
	q    chan protocol.Packet
}

// Serve 开始监听。调用方在进程退出时 Stop。
// idle 是读空闲上限：这段时间没有任何入站字节就断开，用来收掉 TCP 没发 FIN 的死连接。
// idle 小于等于 0 表示不启用，测试和显式关闭时用。
func Serve(addr string, codec protocol.Codec, handle protocol.Handler, idle time.Duration) (*Server, error) {
	if codec == nil {
		codec = protocol.FrameCodec{}
	}
	s := &Server{codec: codec, handle: handle, idle: idle}
	engine := nbio.NewEngine(nbio.Config{
		Network: "tcp",
		Addrs:   []string{addr},
		Listen:  s.listen,
	})
	engine.OnOpen(s.onOpen)
	engine.OnData(s.onData)
	engine.OnClose(s.onClose)
	if err := engine.Start(); err != nil {
		return nil, err
	}
	s.engine = engine
	return s, nil
}

// Stop 停止接受新连接。
func (s *Server) Stop() {
	if s.engine != nil {
		s.engine.Stop()
	}
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

func (s *Server) armRead(c *nbio.Conn) {
	if s.idle <= 0 {
		return
	}
	// 每次有入站字节都把截止时间往后推。到期仍没有字节，nbio 会关掉连接并走 OnClose。
	_ = c.SetReadDeadline(time.Now().Add(s.idle))
}

func (s *Server) onOpen(c *nbio.Conn) {
	p := &tcpPeer{
		id:   strconv.FormatUint(s.seq.Add(1), 10),
		conn: c,
		code: s.codec,
		asm:  protocol.NewAssembler(s.codec),
		q:    make(chan protocol.Packet, 128),
	}
	c.SetSession(p)
	s.armRead(c)
	go p.loop(s.handle)
	if s.handle != nil {
		s.handle.OnOpen(p)
	}
}

func (s *Server) onData(c *nbio.Conn, data []byte) {
	v := c.Session()
	p, ok := v.(*tcpPeer)
	if !ok || p == nil {
		return
	}
	s.armRead(c)
	// nbio 会复用 data，必须先拷贝再交给拆包器。
	raw := append([]byte(nil), data...)
	pkts, err := p.asm.Push(raw)
	if err != nil {
		log.Warn("tcp frame", "err", err, "session_id", p.id)
		_ = c.Close()
		return
	}
	for _, pkt := range pkts {
		select {
		case p.q <- pkt:
		default:
			// 会话协程跟不上时不能堵在 poller 上，关掉连接让客户端重连。
			log.Warn("tcp queue full", "session_id", p.id)
			_ = c.Close()
			return
		}
	}
}

func (s *Server) onClose(c *nbio.Conn, err error) {
	v := c.Session()
	p, ok := v.(*tcpPeer)
	if !ok || p == nil {
		return
	}
	p.once.Do(func() { close(p.q) })
	if s.handle != nil {
		s.handle.OnClose(p, err)
	}
}

// ID 在网关会话表里标识这条连接。
func (p *tcpPeer) ID() string { return p.id }

// WritePacket 可从 Kitex 的 Push 协程调用。nbio 允许并发写。
func (p *tcpPeer) WritePacket(pkt protocol.Packet) error {
	buf, err := p.code.Encode(pkt)
	if err != nil {
		return err
	}
	_, err = p.conn.Write(buf)
	return err
}

// Close 主动断开客户端。
func (p *tcpPeer) Close() error { return p.conn.Close() }

func (p *tcpPeer) loop(h protocol.Handler) {
	for pkt := range p.q {
		if h != nil {
			h.OnPacket(p, pkt)
		}
	}
}
