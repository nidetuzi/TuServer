// Package ws 用 nbio 的 HTTP 引擎接受 WebSocket。
// 一条二进制消息对应一帧，因此和 TCP 共用 Codec，只是不再做粘包。
package ws
