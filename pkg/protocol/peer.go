package protocol

// Peer 是一条客户端连接。网关只依赖这个接口，因此 TCP 和 WebSocket 可以互换。
type Peer interface {
	ID() string
	WritePacket(pkt Packet) error
	Close() error
}

// Handler 接收连接生命周期。OnPacket 由会话协程调用，不在 nbio 的 poller 上。
type Handler interface {
	OnOpen(Peer)
	OnPacket(Peer, Packet)
	OnClose(Peer, error)
}
