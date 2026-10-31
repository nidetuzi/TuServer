# Gate 网关架构与使用

本文以当前 `pkg/gate`、进程入口及 Logic 对端实现为准。Gate 保存连接与 Logic 绑定、转发协议和写回响应，不执行玩法 handler，也不管理实体存储。

## 职责与源码入口

- [gate.go](../pkg/gate/gate.go)：`Forwarder`、`Gate`、会话表与连接回调、`Push`。
- [cmd/gate/main.go](../cmd/gate/main.go)：加载配置、初始化日志、创建 cluster 客户端、注册 Kitex Gate 服务、启动 TCP/WS。
- [config/gate.yaml](../config/gate.yaml)、[config.go](../pkg/config/config.go)：当前配置及字段。
- [peer.go](../pkg/protocol/peer.go)：Gate 对接的传输抽象。
- [logic/app.go](../pkg/logic/app.go)、[cmd/logic/main.go](../cmd/logic/main.go)：Dispatch 对端及在线会话广播。
- [node.proto](../idl/node.proto)：内部 `Dispatch`、`Push`、`NotifyServer` RPC 契约。

帧和传输细节见 [协议文档](protocol.md)，发现、权重与广播实现见 [集群文档](cluster.md)。

## 实际架构与请求生命周期

客户端经 TCP 或 WebSocket 进入 `protocol.Handler`，Gate 以 `Peer.ID()` 索引内存会话，再通过 Kitex 定向调用 Logic。etcd 只提供实例发现和权重，不持有会话绑定。

### 建连与第一帧

1. `OnOpen(peer)` 写入一个新会话，记录 `peer`；此时 `logicAddr`、`serverID`、`playerID` 为零值，没有预先选 Logic。
2. 每个传输连接的队列协程串行调用 `OnPacket`。Gate 查不到会话就直接忽略包。
3. 每一帧都会把 `session.serverID` 更新为该帧的 `ServerID`，没有“第一帧区服不可变”的检查。
4. `logicAddr` 为空时调用 `Forwarder.Pick(context.Background())`。选择失败只写日志并返回：不关闭连接、不发送客户端错误帧，下一帧仍会再选。
5. 选择成功后保存地址，并用 `Dispatch` 发送 Gate 的回推地址、当前 sessionId、帧中的 serverId/cmdId/body，以及 Gate 已保存的 playerId。

### 后续请求与返回

- 只要 `logicAddr` 非空，后续所有帧都发往该地址，不再均衡；即使客户端改变 serverId，也不会因此换 Logic。
- Logic 普通协议以 `session/<sessionId>` 为键进入 actor；成功后更新 Logic 自己的在线会话表，并返回 `PlayerID`、`CmdID`、`Body`。共享实体使用独立的 `owner/...` 命名空间，不占用会话邮箱。
- Gate 仅在返回 `PlayerID != 0` 时更新会话 playerId；返回 0 不会清除原绑定。
- 同步回包采用返回的 cmdId/body，以及**当前请求帧的 serverId**，直接调用 `peer.WritePacket`。
- 写客户端失败只记录日志，没有自动清除绑定、关闭连接或重新发送响应。

### 绑定失效和断开

- 任何 `Dispatch` 错误都会清空当前 `logicAddr`。包括透过 RPC 返回的业务处理错误，并非仅连接错误才触发。
- **出错的当前帧不重试，也没有错误回包**；只有下一帧才再次 `Pick`。源码注释中“再选一次”不能理解成同一帧内重试。
- 清空地址时保留 playerId、serverId 和客户端连接；再次选中其他 Logic 不是已有状态的迁移协议。
- `OnPacket` 拒绝 `cmdId <= 0`：打日志、标记会话作废并关闭连接，不转发、不改区服。队列里排在后面的帧也不再转发。
- `OnClose` 先删除 Gate 会话。如果之前仍有 logicAddr，则同步发一次 `Internal: true`、`CmdDisconnect`（-1）的 `Dispatch`。这个标记不来自客户端帧。错误被忽略。
- Logic 只在 `Request.Internal` 为真时处理 0 和负数。客户端伪造的主人调用或断线到不了 `Owners.Handle`。详见 [owner 文档](owner.md)。
- 如果 Dispatch 已清空 logicAddr，后续 OnClose 不会通知原 Logic；异常断开通知、Gate 崩溃也没有其他会话回收协议兜底。

## 推送与广播

`Gate.Push(sessionID, cmdID, serverID, body)` 查找本地会话并写客户端；找不到时返回 nil，表示静默丢弃，不表示消息到达。写失败则向调用者返回错误。

当前 [pushAPI.Push](../cmd/gate/main.go) 只接收 `PushReq{session_id, cmd_id, body}`，调用 `Gate.Push` 时将 serverId 固定传 **0**。因此：

- 同步 Dispatch 回包的 serverId 是请求里的区服；
- 经 Kitex Push 的普通推送和区服广播，最终客户端帧的 serverId 都是 0；
- 直接调用 `Gate.Push` 则可由调用方显式指定 serverId。

广播不由 Gate 根据区服扫描会话。调用链是 `Nodes.NotifyAll` 依次通知全部 Logic，每个 Logic 按自己内存中的 `Sessions(serverID)` 逐个向会话 Gate 地址推送。Gate 不验证此次推送来自当前绑定 Logic，也不按 playerId 或 serverId 二次过滤。广播的部分失败和确认语义见 [集群文档](cluster.md)。

## 主要 API 与集成方法

- `New(id, fwd, codec)`：`id` 实际应是 Logic 能连回来的 **Gate Kitex 地址**，不是展示名称、客户端监听地址或区服编号。`codec == nil` 时保存默认 codec，但当前 Gate 后续转发路径并不使用该字段；真正编解码由 TCP/WS 的 `Serve` 参数决定。
- `Forwarder.Pick(ctx)`：为未绑定会话选择 `cluster.Instance`。
- `Forwarder.Dispatch(ctx, host, logic.Request)`：向会话绑定地址发送请求。可由 `cluster.Nodes` 实现，也可注入测试替身。
- `OnOpen` / `OnPacket` / `OnClose`：实现 `protocol.Handler`；由传输层驱动，不需要业务手动调用。
- `Push`：供 RPC 适配器或本地集成调用，不负责发现 Gate 地址。

进程使用 [config/gate.yaml](../config/gate.yaml)，下面展示接入与集群相关字段，日志字段可保留原文件配置：

```yaml
service_name: Gate
weight: 10
listen_addr: 127.0.0.1:9001
advertise_addr: 127.0.0.1:9001
tcp_addr: :8001
ws_addr: :8002
etcd:
  endpoints:
    - 127.0.0.1:2379
```

- `listen_addr` 是 Gate 内部 Kitex 监听地址；`advertise_addr` 是注册并作为 `GateID` 发给 Logic 的回推地址。
- `tcp_addr` 为客户端 TCP 入口；`ws_addr` 提供 `ws://127.0.0.1:8002/ws`，路径固定 `/ws`。
- 某个客户端地址留空就不启动该监听。当前两个传输的 ID 会冲突，见下节；不要把同时配置两个入口视为已支持无冲突的混合会话。
- 跨机器部署时必须修改 `advertise_addr`，示例的回环地址只适合单机。
- 服务名应为 `Gate`；Logic 需要以 `Logic` 注册。权重配置作用于注册；Gate 本身并不通过 Gate 权重为已有连接选择返回路径。

在仓库根目录可用 PowerShell 启动：`go run ./cmd/gate`；指定其他配置文件用 `go run ./cmd/gate ./config/gate.yaml`。启动前需要可访问的 etcd 和已注册的 Logic；仅连上客户端端口不代表有可用 Logic。

当前入口顺序是加载配置/日志、创建 Nodes、创建 Gate、注册 Kitex 并异步 Run，再分别启动 TCP/WS，最后等待 SIGINT/SIGTERM。Kitex Run 失败只记日志，未触发整个进程退出；TCP/WS 启动失败则 `os.Exit(1)`。退出时只显式 `svr.Stop()`，没有保留 TCP/WS Server 句柄并调用其 Stop，也没有实现先停止接入、排空在途请求、再逐会话通知 Logic 的完整退出流程。

## 约束与失败边界

### 会话身份与一致性

- TCP、WS 的序号是各自 Server 的局部计数器，都从字符串 `"1"` 开始。同一个 Gate 同时启用两个入口时，`OnOpen` 会按相同 ID 覆盖已有会话，Push 和关闭删除可能作用到错误连接。
- 即使只有单入口，不同 Gate 进程及重启后的 ID 也会重复。[Logic 在线表与 actor 键](../pkg/logic/app.go) 只使用 sessionId，没有与 Gate 地址组合，存在跨 Gate 会话覆盖、串行键混用及断开误删风险。当前没有全局会话 ID 或重启代次。
- Gate 并不验证 client serverId 的合法范围或会话区服是否变化，保留的 playerId 也不会随区服变化自动清零；不能把 Gate 当作身份认证或跨区服权限校验层。
- `OnPacket` 依赖传输层每连接串行消费；会话 map 的互斥锁只保护查找/增删，不保护返回 session 指针里的字段。`OnClose` 和队列中残留 `OnPacket` 可能并发，当前没有等待或排空协调，不应宣称所有生命周期操作都严格串行。

### 失败与投递保证

- 选点/Dispatch 使用 `context.Background()`，无应用层显式超时、取消或 Gate 层重试策略。
- Dispatch 失败后没有向旧 Logic 发送清理通知；新节点成功后可能留下旧在线记录。没有自动踢旧会话、跨节点角色排他、脏数据迁移或恢复确认。
- 队列满、非法帧等由传输层关闭连接；协议层最小帧 12 字节，当前没有自研应用层最大帧大小限制，WS 一消息只解第一帧。详见 [协议文档](protocol.md)。
- 未实现客户端心跳/空闲回收、限流、鉴权、客户端错误码统一编码、服务端推送确认或断线补投。
- Gate 只保存内存路由，不读写 Mongo，不持久化会话；etcd 节点摘除不主动重绑定现存会话。

## 相关测试与覆盖范围

当前 `pkg/gate` 与 `cmd/gate` **没有专用测试文件**。相关现有测试：

- [frame_test.go](../pkg/protocol/frame_test.go)：默认帧往返和半包拆包，是 Gate 的输入基础，不覆盖网关会话行为。
- [pick_test.go](../pkg/cluster/pick_test.go)：权重 1:3 的可复现选择与固定列表下的实体主人稳定性，不覆盖绑定失效或 RPC。
- [demo_test.go](../example/demo/demo_test.go)：通过 `logic.App` 和内存存储直接验证登录、升级、道具、聊天及公会流程；聊天广播使用替身，不经过真实 Gate/Kitex。

目前未覆盖真实 TCP/WS 到 Logic 的全链路、双传输/多 Gate ID 冲突、错误后重选、关闭通知失败、推送 serverId 或退出并发行为。本文仅阅读源码和已有测试，未运行测试。
