package gate

import (
	"context"
	"sync"

	"tuserv/pkg/cluster"
	"tuserv/pkg/log"
	"tuserv/pkg/logic"
	"tuserv/pkg/protocol"
)

// Forwarder 是网关使用的逻辑节点客户端。
type Forwarder interface {
	Dispatch(ctx context.Context, host string, req logic.Request) (logic.Response, error)
	Pick(ctx context.Context) (cluster.Instance, error)
}

// Gate 持有会话表，并实现客户端连接回调。
type Gate struct {
	id   string
	fwd  Forwarder
	code protocol.Codec

	mu       sync.Mutex
	sessions map[string]*session
}

type session struct {
	peer      protocol.Peer
	logicAddr string
	serverID  int32
	playerID  int64
	// dead 表示已经拒绝过非法帧。队列里排在后面的包不能再转发。
	// 会话先留在表里，等连接关闭回调通知逻辑节点，避免人还挂在在线表上。
	dead bool
}

// New 创建网关。id 是逻辑节点回推时使用的 Gate 地址。
func New(id string, fwd Forwarder, codec protocol.Codec) *Gate {
	if codec == nil {
		codec = protocol.FrameCodec{}
	}
	return &Gate{id: id, fwd: fwd, code: codec, sessions: make(map[string]*session)}
}

// OnOpen 记录连接。此时还没有逻辑节点，要等第一帧才知道区服。
func (g *Gate) OnOpen(p protocol.Peer) {
	g.mu.Lock()
	g.sessions[p.ID()] = &session{peer: p}
	g.mu.Unlock()
}

// OnPacket 在会话协程里运行。第一次按权重选节点，失败时清掉地址再选一次。
// 心跳在本地应答。非正协议号直接丢掉并关掉连接。断线和主人调用不走客户端帧。
func (g *Gate) OnPacket(p protocol.Peer, pkt protocol.Packet) {
	s := g.session(p.ID())
	if s == nil || s.dead {
		return
	}
	if pkt.CmdID == protocol.CmdHeartbeat {
		// 探活只证明连接还在，不能拿去选节点或改 serverId，否则空心跳会把已绑定的区服冲掉。
		if err := p.WritePacket(protocol.Packet{CmdID: protocol.CmdHeartbeat, ServerID: pkt.ServerID}); err != nil {
			log.Warn("heartbeat", "err", err, "session_id", p.ID())
		}
		return
	}
	if pkt.CmdID <= 0 {
		s.dead = true
		log.Warn("reject client cmd", "cmd_id", pkt.CmdID, "session_id", p.ID())
		_ = p.Close()
		return
	}
	// 角色绑定后区服以当时那一帧为准。再改只回错误帧：断开会把人踢下线，转发则会按新服申领租约。
	if s.playerID != 0 && pkt.ServerID != s.serverID {
		if err := p.WritePacket(protocol.Packet{
			CmdID:    protocol.CmdError,
			ServerID: s.serverID,
			Body: protocol.MarshalError(protocol.ErrorBody{
				Code:   logic.ErrServerMismatch.Errno(),
				ReqCmd: pkt.CmdID,
				Detail: logic.ErrServerMismatch.Error(),
			}),
		}); err != nil {
			log.Warn("write client", "err", err, "session_id", p.ID())
		}
		return
	}
	s.serverID = pkt.ServerID
	if s.logicAddr == "" {
		inst, err := g.fwd.Pick(context.Background())
		if err != nil {
			log.Error("pick logic", "err", err, "session_id", p.ID())
			return
		}
		s.logicAddr = inst.Addr
		log.Info("session bind", "server_id", pkt.ServerID, "logic", inst.Addr, "session_id", p.ID())
	}
	resp, err := g.fwd.Dispatch(context.Background(), s.logicAddr, logic.Request{
		GateID:    g.id,
		SessionID: p.ID(),
		ServerID:  pkt.ServerID,
		PlayerID:  s.playerID,
		CmdID:     pkt.CmdID,
		Body:      pkt.Body,
	})
	// 业务错误已经编码进回包，这里的 err 只剩选路和传输失败。
	// 清掉地址是为了下一帧重选节点；玩法失败不能走这里，否则客户端既看不到错误码，也会丢掉还活着的 Logic。
	if err != nil {
		log.Warn("dispatch", "err", err, "session_id", p.ID(), "logic", s.logicAddr)
		s.logicAddr = ""
		return
	}
	if resp.PlayerID != 0 {
		s.playerID = resp.PlayerID
	}
	if err := p.WritePacket(protocol.Packet{CmdID: resp.CmdID, ServerID: pkt.ServerID, Body: resp.Body}); err != nil {
		log.Warn("write client", "err", err, "session_id", p.ID())
	}
}

// OnClose 通知逻辑节点丢掉这条会话，避免区服广播继续推给已断开的连接。
func (g *Gate) OnClose(p protocol.Peer, _ error) {
	s := g.drop(p.ID())
	if s == nil || s.logicAddr == "" {
		return
	}
	_, _ = g.fwd.Dispatch(context.Background(), s.logicAddr, logic.Request{
		GateID:    g.id,
		SessionID: p.ID(),
		ServerID:  s.serverID,
		PlayerID:  s.playerID,
		CmdID:     logic.CmdDisconnect,
		Internal:  true,
	})
}

// Push 把逻辑节点的推送写回指定会话。踢线帧不写给客户端，直接关掉这条连接。
func (g *Gate) Push(sessionID string, cmdID, serverID int32, body []byte) error {
	s := g.session(sessionID)
	if s == nil {
		return nil
	}
	if cmdID == logic.CmdKick {
		return s.peer.Close()
	}
	return s.peer.WritePacket(protocol.Packet{CmdID: cmdID, ServerID: serverID, Body: body})
}

func (g *Gate) session(id string) *session {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.sessions[id]
}

func (g *Gate) drop(id string) *session {
	g.mu.Lock()
	defer g.mu.Unlock()
	s := g.sessions[id]
	delete(g.sessions, id)
	return s
}
