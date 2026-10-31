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

	// peers 在 OnOpen 里发布已经填好的连接。
	// nbio 的 Session 读写标了 norace，OnData/OnClose 不能靠它和 OnOpen 建立同步。
	peersMu sync.Mutex
	peers   map[*nbio.Conn]*tcpPeer
}

type tcpPeer struct {
	id   string
	conn *nbio.Conn
	code protocol.Codec
	asm  *protocol.Assembler
	q    chan protocol.Packet

	mu       sync.Mutex
	closed   bool
	closeErr error
}

// Serve 开始监听。调用方在进程退出时 Stop。
// idle 是读空闲上限：这段时间没有任何入站字节就断开，用来收掉 TCP 没发 FIN 的死连接。
// idle 小于等于 0 表示不启用，测试和显式关闭时用。
func Serve(addr string, codec protocol.Codec, handle protocol.Handler, idle time.Duration) (*Server, error) {
	if codec == nil {
		codec = protocol.FrameCodec{}
	}
	s := &Server{
		codec:  codec,
		handle: handle,
		idle:   idle,
		peers:  make(map[*nbio.Conn]*tcpPeer),
	}
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
	// 先发布，再交给 poller。字段写入在这次解锁之前，OnData/OnClose 加锁取出后才能读。
	s.peersMu.Lock()
	s.peers[c] = p
	s.peersMu.Unlock()
	c.SetSession(p)
	s.armRead(c)
	go p.loop(s.handle)
	if s.handle != nil {
		s.handle.OnOpen(p)
	}
}

func (s *Server) lookup(c *nbio.Conn) *tcpPeer {
	s.peersMu.Lock()
	p := s.peers[c]
	s.peersMu.Unlock()
	return p
}

func (s *Server) onData(c *nbio.Conn, data []byte) {
	p := s.lookup(c)
	if p == nil {
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
		closed, full := p.enqueue(pkt)
		if closed {
			return
		}
		if full {
			// 会话协程跟不上时不能堵在 poller 上，关掉连接让客户端重连。
			log.Warn("tcp queue full", "session_id", p.id)
			_ = c.Close()
			return
		}
	}
}

func (s *Server) onClose(c *nbio.Conn, err error) {
	s.peersMu.Lock()
	p := s.peers[c]
	delete(s.peers, c)
	s.peersMu.Unlock()
	if p == nil {
		return
	}
	// 这里只关队列。OnClose 放到 loop 退出之后，才能读到 OnPacket 刚写回的 playerID。
	p.shutdown(err)
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

// enqueue 把一帧交给会话协程。closed 表示连接已经在关，不能再往已关闭的队列里送。
func (p *tcpPeer) enqueue(pkt protocol.Packet) (closed, full bool) {
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
func (p *tcpPeer) shutdown(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	p.closeErr = err
	close(p.q)
}

func (p *tcpPeer) loop(h protocol.Handler) {
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
