// Package protocol 定义客户端帧和连接抽象。
// Codec（帧解析器）可替换；TCP 与 WebSocket 共用注入的编解码，网关不解析玩法协议体。
package protocol
