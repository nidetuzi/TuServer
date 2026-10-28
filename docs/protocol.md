# 客户端协议与 TCP / WebSocket 接入

本文依据当前 `pkg/protocol`、传输实现及已有测试编写；不把注释中的意图视为已经完成的校验。

## 职责与源码入口

- [protocol 包说明](../pkg/protocol/doc.go)：客户端帧与连接抽象，不负责业务 protobuf 的解析、登录或路由。
- [frame.go](../pkg/protocol/frame.go)：`Packet`、`Codec`、默认 `FrameCodec` 与 TCP `Assembler`。
- [peer.go](../pkg/protocol/peer.go)：`Peer` 和连接生命周期 `Handler`。
- [tcp/server.go](../pkg/protocol/tcp/server.go)：基于 nbio 的 TCP 监听、拆包及每连接队列。
- [ws/server.go](../pkg/protocol/ws/server.go)：基于 nbhttp/websocket 的 `/ws` 接入。
- [Gate 启动入口](../cmd/gate/main.go)：分别启动客户端监听；节点内部 RPC 不走这里。

上下游见 [网关文档](gate.md)、[集群文档](cluster.md)。

## 帧格式与大小

默认帧依次为：`uint32 payloadLength | int32 cmdId | int32 serverId | body`。所有整数按**大端序**读写；`cmdId`、`serverId` 通过对应的 32 位位模式编码。

- 长度字段占 4 字节，统计 `cmdId + serverId + body`，**不包含自身**。
- 元信息占 8 字节，因此合法的最小长度字段是 8，最小完整帧是 **12 字节**，允许空 body。
- body 有 N 字节时，长度字段为 `8 + N`，总帧大小为 `12 + N`。
- body 在玩法层通常是 protobuf，但 `FrameCodec` 只复制字节，并不验证是否为合法 protobuf。
- 线上的长度字段最多表示 `2^32 - 1` 字节 payload；这不是实现承诺可接收的安全上限。当前自研编解码器和 TCP 累积缓冲**没有应用层最大帧限制**，`Encode` 也未检查长度转换为 `uint32` 时的溢出。部署不能将它理解为已有 1 MiB、4 MiB 等保护。

`FrameCodec.Decode(buf)` 返回一个包、消耗字节数 `n` 和错误。不足 4 字节或不足声明的整帧时返回 `ErrShortBuffer`；长度字段小于 8 返回 `ErrBadFrame`。成功后 body 独立复制，尾随数据由调用方处理。

## 实际接入架构与生命周期

### TCP

1. `tcp.Serve(addr, codec, handler)` 创建 nbio TCP engine；`codec == nil` 时使用 `FrameCodec{}`。
2. 打开连接时，在该 `Server` 内用从 1 起的十进制递增字符串生成 ID，创建容量 **128** 的 `chan Packet`、连接专属 `Assembler`，启动消费协程，然后调用 `handler.OnOpen`。
3. nbio `OnData` 将输入复制后交给 `Assembler.Push`。Assembler 自己也会追加复制，以免 nbio 重用底层数据。
4. Assembler 保存半包；连续调用 `Decode` 提取所有完整帧，因此支持拆包与粘包。每轮会把剩余缓冲复制出来保存。
5. 完整包非阻塞入队。每连接的消费协程按队列顺序调用 `OnPacket`，避免业务占住 poller。
6. 拆包错误或入队时队列已满，则记录日志并关闭连接。关闭回调用 `sync.Once` 关闭队列，再调用 `OnClose`。

注意：如果同一次 `Push` 先解出合法帧、随后遇到非法帧，TCP 回调先检查错误并退出，这批已解出的包也不会入队。

### WebSocket

1. `ws.Serve(addr, codec, handler)` 创建 HTTP mux，固定注册 **`/ws`**；示例地址是 `ws://127.0.0.1:8002/ws`。本入口没有配置 TLS。
2. Upgrade 成功后同样生成服务器内递增 ID、容量 128 的队列及消费协程，再调用 `OnOpen`。
3. 每次 `OnMessage` 复制消息内容，调用一次 `codec.Decode`，然后入队；无 TCP Assembler。
4. 推荐约定：**一条 WebSocket 二进制消息只装一帧完整的默认协议帧**，仍包含 4 字节长度头，不能只发 protobuf body。
5. 当前回调忽略 WebSocket 消息类型，也忽略 Decode 返回的消耗长度：并未强制只收二进制消息；若同一消息放多帧或尾随字节，只处理第一帧，剩余内容丢弃。
6. 一帧不能拆到多次 `OnMessage` 中拼接；本实现不会跨消息累积，`ErrShortBuffer` 也会导致关闭连接。这里说的是应用消息，不是底层 WebSocket 分片。
7. 解码错误或队列满时关闭连接；关闭回调行为与 TCP 相同。出站始终调用 `WriteMessage(BinaryMessage, frame)`。

## 主要 API 与使用方法

- `Packet{CmdID, ServerID, Body}`：业务无关的单帧。
- `Codec.Encode(Packet)` / `Decode([]byte)`：可注入替代编解码器；替代实现必须遵守消耗字节和半包错误的约定，并确保成功时推进缓冲。
- `NewAssembler(codec)` / `Push(data)`：用于单条 TCP 流；不要在多条连接间共享，也没有并发访问保护。
- `Peer.ID()`、`WritePacket(pkt)`、`Close()`：网关只依赖该接口，不关心传输类型。
- `Handler.OnOpen`、`OnPacket`、`OnClose`：生命周期适配点。只有 `OnPacket` 明确由每连接队列协程调用；不能假定三个回调都在同一个协程。
- `tcp.Serve` / `ws.Serve` 返回 `*Server`；独立集成时应保留返回值并调用 `Stop()`。TCP 调用 engine 的 `Stop()`，WS 调用 `Shutdown(nil)`。

以下是编码与拆包的最小调用示例，不是额外的消息格式：

```go
codec := protocol.FrameCodec{}
raw, err := codec.Encode(protocol.Packet{
    CmdID: 1001, ServerID: 7, Body: []byte{1, 2, 3},
})
if err != nil {
    return err
}
asm := protocol.NewAssembler(codec)
packets, err := asm.Push(raw)
if err != nil {
    return err
}
_ = packets
```

进程配置沿用 [config/gate.yaml](../config/gate.yaml)：`tcp_addr: :8001`、`ws_addr: :8002`，任意字段留空即不启动对应入口；`listen_addr` 则是另一个 Kitex 内部 RPC 端口。具体启动和地址语义见 [网关文档](gate.md)。

## 约束与失败边界

- 当前未配置应用层最大包长、连接认证、读写超时、心跳、限流或自定义 Origin 校验；WebSocket 库的默认行为不等于本包提供了这些能力。
- 声明巨大 payload 的 TCP 半包会一直占用连接缓冲等待补齐；128 限制的是已解码包的**数量**，不是字节总量。
- `WritePacket` 返回编码/传输写错误，不提供客户端确认、消息重试或投递保证。TCP 实现依赖 nbio 的并发写能力。
- 队列关闭后消费协程仍会排空已有元素，没有关闭时清空队列或等待消费协程退出的逻辑；`OnClose` 可能与仍在执行的 `OnPacket` 重叠。
- TCP 与 WS 各自维护 ID 计数器，ID 仅在单个传输 `Server` 内生成，不保证跨监听器、跨进程或重启后的唯一性。同一 Gate 同时接入两个入口时存在 ID 冲突风险，见 [网关文档](gate.md)。
- `cmdId` 的负值和 0 在 Logic 层具有内部含义；传输层不拦截，也不是客户端鉴权边界。
- 当前 Gate 主程序没有保留两个传输 Server 的返回值，退出路径只显式停止 Kitex，不能把传输层 `Stop()` 的存在等同于主程序已完成连接排空。

## 相关测试与覆盖范围

[frame_test.go](../pkg/protocol/frame_test.go) 的 `TestFrameRoundTripAndShortBuffer` 覆盖默认帧往返、字段/body 保真、短缓冲、TCP 半包追加后完成拆包。

当前没有 TCP/WS 专用测试；已有测试未覆盖非法长度、超大帧、多帧粘包、WS 尾随帧/消息类型、真实网络连接、队列溢出或关闭并发时序。此次仅阅读实现和测试，未运行测试。
