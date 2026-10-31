package protocol

// Peer 是一条客户端连接。网关只依赖这个接口，因此 TCP 和 WebSocket 可以互换。
type Peer interface {
	ID() string
	WritePacket(pkt Packet) error
	Close() error
}

// Handler 接收连接生命周期。
// OnPacket 和 OnClose 都由会话协程调用，不在 nbio 的 poller 上。
// OnClose 要等这条连接队列里的包都处理完才调用，这样断线通知能看到刚刚写回的会话状态，并且只会调用一次。
type Handler interface {
	OnOpen(Peer)
	OnPacket(Peer, Packet)
	OnClose(Peer, error)
}
