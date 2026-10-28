// Package tcp 用 nbio 接受客户端 TCP 连接。
// 拆包之后把完整帧交给会话协程，避免在 poller 里做 Kitex 调用。
package tcp
