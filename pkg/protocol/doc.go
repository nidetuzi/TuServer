// Package protocol 定义客户端帧和连接抽象。
// TCP 与 WebSocket 共用同一套编解码，网关才不用为每种传输各写一份业务解析。
package protocol
