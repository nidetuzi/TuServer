package protocol

// Peer 是一条客户端连接。网关只依赖这个接口，因此 TCP 和 WebSocket 可以互换。
type Peer interface {
	ID() string
	WritePacket(pkt Packet) error
	Close() error
}

// Handler 接收连接的数据与关闭事件。
// OnPacket 和 OnClose 都由会话协程调用，不在 nbio 的 poller 上。
// OnClose 要等当前正在处理的包返回后才调用；关闭时尚未处理的排队包会丢弃，并且只会调用一次。
type Handler interface {
	OnPacket(Peer, Packet)
	OnClose(Peer, error)
}

// OpenHandler 接收连接建立事件及其请求 URI。TCP 连接的 requestURI 为空。
// 框架在 OnOpen 返回前不会处理该连接的业务消息。
type OpenHandler interface {
	OnOpen(Peer, string)
}

// LegacyOpenHandler 兼容尚未迁移到 OpenHandler 的业务回调。
type LegacyOpenHandler interface {
	OnOpen(Peer)
}

// NotifyOpen 调用连接建立回调，并优先使用携带 requestURI 的接口。
func NotifyOpen(h Handler, p Peer, requestURI string) {
	if h == nil {
		return
	}
	if opener, ok := h.(OpenHandler); ok {
		opener.OnOpen(p, requestURI)
		return
	}
	if opener, ok := h.(LegacyOpenHandler); ok {
		opener.OnOpen(p)
	}
}
