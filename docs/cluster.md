# 集群发现、选点与节点 RPC

本文描述当前 `pkg/cluster` 的实际实现。它是节点发现和定向调用的薄封装，不是会话迁移、分布式一致性或可靠广播系统。

## 职责与源码入口

- [rpc.go](../pkg/cluster/rpc.go)：`Nodes`、etcd resolver/registry、Kitex Server 配置、发现与 RPC。
- [pick.go](../pkg/cluster/pick.go)：`Instance`、加权随机 `PickWeighted`、实体键选主 `Owner`。
- [convert.go](../pkg/cluster/convert.go)：领域请求与生成 RPC 请求之间的转换。
- [node.proto](../idl/node.proto)：节点 RPC 契约；[kitex_gen/node](../kitex_gen/node/) 是生成代码，不应手改。
- [Gate 入口](../cmd/gate/main.go)、[Logic 入口](../cmd/logic/main.go)：注册服务并适配 RPC 到 Gate/Logic。
- [owner.go](../cmd/logic/owner.go)、[owner 运行时](../pkg/owner/owner.go)：共享实体选主、统一邮箱与 `Dispatch` 转发的当前集成，详见 [owner 文档](owner.md)。

连接与帧格式见 [协议文档](protocol.md)，会话路由见 [网关文档](gate.md)。

## 实际架构与职责划分

**etcd** 用于 Kitex 服务实例的注册与发现，返回实例地址和权重。它不保存当前 Gate 会话绑定，不转发业务包，也不在本模块中承担实体锁或主人租约。

**Kitex** 承担节点间的请求/响应 RPC。`Dial(endpoints)` 创建共享 resolver，并分别创建服务名固定为 `Logic`、`Gate` 的客户端；业务协议号作为 `DispatchReq.cmd_id` 传递，而不是每个玩法生成一个 Kitex 方法。

**Gate** 在第一帧调用 `Nodes.Pick`，保存选中的 Logic 地址。后续 `Nodes.Dispatch` 通过 `callopt.WithHostPort(host)` 定向发包，不重新按权重均衡。一个 Logic 能同时服务多个区服，区服不是固定节点归属。

### 普通请求生命周期

1. Gate 获得客户端帧，尚未绑定时使用 `List(ctx, "Logic")` 取得实例列表，再按权重选择。
2. Gate 将自己可被回推的 Kitex 地址、sessionId、serverId、已绑定 playerId、cmdId、body 传给 `Nodes.Dispatch`。
3. `convert.go` 生成 `DispatchReq`，Kitex 定向调用目标 Logic。
4. [Logic RPC 适配器](../cmd/logic/main.go) 调用 [logic.App.Handle](../pkg/logic/app.go)，普通协议进入会话 actor，由 Router 执行。
5. 响应中的 playerId、cmdId、body 返回 Gate，再编码成客户端帧。错误直接向上返回，cluster 本身不重选、不重放。

### 推送与区服广播生命周期

- `Nodes.Push(ctx, gateHost, sessionID, cmdID, body)` 定向调用持有连接的 Gate，不按区服选择 Gate。
- `Nodes.NotifyAll(ctx, serverID, cmdID, body)` 每次发现**全部 Logic 实例**，逐个同步调用其 `NotifyServer`；这里不根据权重或区服过滤实例。
- 每个 Logic 的 `NotifyServer` 只遍历 `app.Sessions(serverID)` 返回的本节点内存会话，再逐个调用 `Nodes.Push` 回到各会话记录的 Gate 地址。在线表仅由成功的普通 Dispatch 更新，不是从 etcd 或 Mongo 查询出来的全服在线列表。
- `NotifyAll` 某节点失败后仍继续调用后续节点，最终返回遇到的第一个 RPC 错误；列表为空时返回 nil。
- Logic 对单会话 Push 失败只记录日志，最终仍返回成功的 `NotifyResp`。Gate 找不到 session 时也返回 nil。因此 `NotifyAll == nil` **不代表每位客户端都收到消息**。
- [聊天服务](../example/demo/service/chat.go) 已在独立 goroutine 中发起广播，避免玩家 actor 同步等待整个广播链路；`NotifyAll` 自身不是异步 API。
- `PushReq` 不包含 serverId；当前 Gate RPC 适配器将客户端推送帧的 serverId 固定写为 **0**。`NotifyReq.server_id` 只用于 Logic 筛选会话，不会保留进最终推送帧。

## 主要 API 和选点规则

### 创建客户端与注册服务

- `Dial(endpoints, secret) (*Nodes, error)`：创建 resolver、Logic/Gate 客户端，并给每次调用带上 HMAC。`secret` 为空时直接返回，不会去连 etcd。返回成功不应作为所有服务已就绪的业务保证。
- `NewRegistry(endpoints)`：创建 etcd registry。
- `ServerOptions(cfg, reg)`：解析 TCP 监听地址及对外注册地址，设置服务名、注册权重和验签中间件。密钥缺失或地址无法解析时返回错误。监听地址是通配（`:port` 或 `0.0.0.0`）时只告警，仍按配置监听。
- `List(ctx, service)`：直接调用 resolver，将每个实例转换为 `Instance{Addr, Weight}`；不在本层过滤、去重或排序。

`ServerOptions` 使用 `cfg.ListenAddr` 监听，`cfg.AdvertiseAddr` 非空时用它注册，否则使用监听地址；设置 `SkipListenAddr: true` 防止注册地址被监听地址覆盖。示例默认只绑 `127.0.0.1`。多机部署要写成其他节点能连上的具体地址，不要用通配地址，也不要在跨机器时用回环地址。注册权重小于等于 0 时会被改成 **10**，因此不能通过本函数把 `weight: 0` 当成摘流开关。

节点间 `Dispatch`、`NotifyServer`、`Push`、`Ping` 在进入业务前校验 HMAC-SHA256。签名覆盖时间戳、是否内部调用和方法名，允许约 5 分钟时钟偏差。内部命令的 `Request.Internal` 由验签后的标记决定，不写进客户端帧，也不靠 `gate_id` 字符串。缺密钥时进程拒绝启动。相同请求在时间窗内可以被重放。

`Dial` 和 `NewRegistry` 都会检查进程环境变量 `KITEX_ETCD_REGISTRY_LEASE_TTL`：仅在为空时设为字符串 `5`，否则保留原值。它配置 registry 的默认租约 TTL，不代表节点故障后精确 5 秒完成所有发现缓存更新，也不代表自动迁移已有绑定。

配置示例摘取自 [config/logic.yaml](../config/logic.yaml)：

```yaml
service_name: Logic
weight: 10
listen_addr: 127.0.0.1:9101
advertise_addr: 127.0.0.1:9101
etcd:
  endpoints:
    - 127.0.0.1:2379
```

该配置仅展示集群字段，完整 Logic 启动还需要示例文件中的 Mongo 等配置。Gate 应注册为 `Gate`，对应配置见 [config/gate.yaml](../config/gate.yaml)；客户端发现名写死为 `Logic`/`Gate`，不能随意改名后期待自动关联。

### 会话选点与实体选主

- `Pick(ctx)`：发现 `Logic` 后，在随机数生成器互斥锁保护下调用 `PickWeighted`。只应在新会话或 Gate 清空绑定后使用。
- `PickWeighted(list, roll)`：只累计正权重实例；总权重为 0 返回 `ErrNoInstance`。`roll(total)` 应返回 `[0,total)`；越界时实现回退为 0。这是按权重随机，不是按负载、在线人数或区服分配。
- `Owner(list, key)`：复制列表，按地址字符串排序，计算键的 FNV-1a 32 位哈希，使用 `hash % 实例数` 选中实例；空列表返回 `ErrNoInstance`。
- `Owner` **不看权重**，零/负权重实例也可能成为主人。它只是相同列表与键下的稳定选择，不是一致性哈希；列表增删会重映射键，也没有数据搬迁或双主防护。

当前 `cmd/logic/owner.go` 为 `owner.Runtime` 注入 `List("Logic")` 与 `Owner(list, key.String())`。本地和远端调用都进入 `owner/<区服>/<种类>/<编号>` 实体邮箱；远端通过带 `Internal` 的 `CmdOwner`（0）`Dispatch` 传递 owner JSON 请求，不再依赖 `SessionID` 承载实体键，也没有新增玩法专用 Kitex 方法。发现失败或空列表直接报错，不回退本地。接收端在实体邮箱内重新选主，主人改变返回 `owner.moved`，不会继续转发；这些检查仍不提供发现视图分裂下的分布式锁或双主防护，详见 [owner 文档](owner.md)。

## 约束与失败边界

- `Dispatch`、`Push`、`NotifyAll` 都是普通同步 RPC 封装；本模块没有显式配置 RPC 超时、应用级重试、幂等或事务。调用者应传合适的 context，不应把底层库默认策略当作业务投递保证。
- Gate 当前调用使用 `context.Background()`，失败后仅清空地址，等**下一帧**才重选；不是在同一请求中自动故障切换。
- 服务发现改变不会主动改写已有会话地址。Logic 下线后旧绑定仍会先尝试原地址；玩家缓存、脏数据和在线表不会由 cluster 自动迁移。
- 广播逐节点、逐会话同步进行，慢节点/慢推送会延长链路；没有并行 fan-out、失败重试、离线补投、全局顺序或恰好一次语义。
- 会话表只在内存中；Gate 异常退出或断开通知失败可能留下 Logic 旧会话。旧/新 Logic 同时持有记录时，广播也可能重复尝试推送。
- actor 内禁止同步 RPC 调回自身会进入的同一个 actor，以免自等待。实体选主只是寻址工具，调用方仍须保证这一约束。
- `Nodes` 没有暴露 Close API；本包未实现 resolver/client 生命周期统一回收或集群健康管理。
- Cross 在 [node.proto](../idl/node.proto) 中只有预留的 `Ping`，`Nodes` 也未创建 Cross 客户端；不能据此视为已有跨服玩法调度。

## 相关测试与覆盖范围

[pick_test.go](../pkg/cluster/pick_test.go)：

- `TestPickWeightedRatio` 用可复现 roll 验证权重 1:3 在 400 次选择中为 100:300。
- `TestOwnerStable` 验证固定列表和同一个实体键重复调用的主人相同；未验证扩缩容后的稳定性。

[demo_test.go](../example/demo/demo_test.go) 的 `TestDemoFlows` 使用内存存储和假的广播器，验证聊天请求发起正确区服/协议号的广播；不是 etcd、Kitex 或真实客户端推送测试。[guild_test.go](../example/demo/service/guild_test.go) 通过进程内 Transport 调用 `owner.Handle`，覆盖仅主人缓存、DTO 隔离和失败不修改玩家等契约；[owner_test.go](../pkg/owner/owner_test.go) 覆盖发现失败不回退、主人变化拒绝执行和单次传输，也不代表真实跨节点排他性已验证。

当前没有 cluster RPC/etcd 集成测试，没有覆盖租约过期、发现失败、节点切换、广播部分失败和真实跨节点推送。此次未运行测试。
