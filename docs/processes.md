# 进程：启动、装配与运维

## 进程边界

TuServer 模块名为 `tuserv`，框架位于 `pkg`，可执行入口位于 `cmd`，示例玩法位于 `example/demo`。具体实现分别见各模块文档，本篇负责进程级使用说明。

### Gate

入口 [`cmd/gate/main.go`](../cmd/gate/main.go)，配置 [`config/gate.yaml`](../config/gate.yaml)。默认客户端 TCP `:8001`、WebSocket `:8002/ws`，Kitex `:9001`，服务名 `Gate`。

装配顺序为配置 → 日志/Kitex 日志适配 → etcd 发现客户端 → Gate 会话管理器 → Kitex 注册与 Push 服务 → 可选 TCP/WS 监听。客户端连接只接入 Gate；玩法不在 Gate 解 protobuf。详情见 [Gate](gate.md) 和 [协议](protocol.md)。

### Logic

入口 [`cmd/logic/main.go`](../cmd/logic/main.go)，主人适配位于 [`owner.go`](../cmd/logic/owner.go)，配置 [`config/logic.yaml`](../config/logic.yaml)。默认 Kitex `:9101`，服务名 `Logic`。

装配包括 Mongo Store、Cache、Bus、PlayerService、Router、Logic App、Owner Runtime、公会主人操作注册与 Demo 协议注册，之后启动 Kitex 和定时 Flush。Logic 既处理玩家会话，也可承载按键选择的共享实体，不固定属于某个区服。详情见 [Logic](logic.md)、[主人运行时](owner.md)、[Demo](demo.md)。

### Cross

入口 [`cmd/cross/main.go`](../cmd/cross/main.go)，配置 [`config/cross.yaml`](../config/cross.yaml)。默认 Kitex `:9201`，注册名 `CrossNode`。

当前仅初始化日志、注册 Kitex 服务并应答 `Ping`，没有跨服匹配、战斗、实体缓存、刷盘或玩法路由。不要因为存在进程入口就认为跨服玩法已实现。扩展时应显式装配需要的框架模块，不把客户端游戏协议逐个加成 Kitex 方法。

## 本地启动

### 前置条件

- 按 [`go.mod`](../go.mod) 准备 Go 工具链：当前声明 `go 1.27`、`toolchain go1.27.3`。
- 启动 etcd，默认 `127.0.0.1:2379`。
- 启动 MongoDB，默认 `mongodb://127.0.0.1:27017`。
- 从仓库根目录执行命令，确保配置和日志相对路径正确。

分别在不同终端启动：

```powershell
go run ./cmd/logic config/logic.yaml
go run ./cmd/gate config/gate.yaml
go run ./cmd/cross config/cross.yaml
```

配置路径是第一个位置参数，可以省略使用默认值。Cross 当前不参与 Demo 主链路。Logic 未发现时 Gate 无法为新会话选择节点。进程出现启动日志不代表依赖、注册与全部监听已就绪，需检查后续日志并做真实请求验证。

## 多节点部署

1. 复制 Logic YAML，为每个实例设置不同 `listen_addr`、可达且唯一的 `advertise_addr` 和独立日志名/目录。
2. 所有需要互相发现的节点连接同一组 etcd，服务名与代码期望一致。
3. 同一业务集群的 Logic 使用一致的 Mongo URI 和库前缀。区服 3 的默认库为 `game_3`。
4. 新会话按权重选一次 Logic 并钉住地址；权重变化不会主动迁移已有会话。
5. 公会主人按实体键和节点列表选择，不使用会话权重。当前拓扑变化没有完整迁移、fencing 和旧缓存回收机制，不应把增删节点当作无损在线扩缩容。

跨机器不能使用示例里的回环对外地址。Gate 会把自身地址带到 Logic，供推送回连；因此 Gate 的 `advertise_addr` 也必须可达，不应留空。当前会话 ID 还存在跨传输/跨 Gate 唯一性边界，详见 [Gate 限制](gate.md)。

注册租约由 registry-etcd 管理；仓库说明中的默认 TTL 为 5 秒，可通过依赖支持的 `KITEX_ETCD_REGISTRY_LEASE_TTL` 配置。租约、发现刷新、一次请求失败与角色恢复不是原子事务，也不是无损故障转移承诺。

## 数据与退出

实体修改只打脏。Logic 默认每秒 Flush，最终库名由区服决定；角色和公会文档 `_id` 是 ObjectID，业务编号使用区服加序号，详见 [实体](entity.md) 与 [存储](store.md)。

三个入口均等待 SIGINT/SIGTERM。Logic 当前退出顺序是最终 Flush → App.Stop → Kitex.Stop → 关闭 Mongo；定时刷盘协程没有完整取消协调，也没有先停接入、排空邮箱再最终刷盘的屏障。Gate 未保留 TCP/WS 服务句柄来完整停止监听和排空会话。不能据此宣称退出前所有请求都已持久化。

崩溃前尚未 Flush 的数据只在内存。缩短刷盘周期不能代替事务、幂等、备份、在线排他或可靠停机协议。客户端错误也可能导致 Gate 清掉 Logic 绑定，本次帧不重试；详见 [Logic 错误路径](logic.md)。

## 测试与生成

单元测试使用内存存储，不要求本机启动 etcd/Mongo；真实多节点网络与持久化仍需要另外做集成验证。

```powershell
go test ./...
```

只在修改协议源后重新生成：

```powershell
protoc --go_out=. --go_opt=module=tuserv example/demo/proto/game.proto
kitex -module tuserv -type protobuf idl/node.proto
```

生成目录 `example/demo/gen` 和 `kitex_gen` 不手改。`protoc`、Go protobuf 插件及 Kitex 工具需自行准备，普通运行不要求每次生成。

## 排查入口

- 新连接无回包：检查 Gate 监听、etcd Logic 列表、对外地址以及 Dispatch 错误；当前没有统一客户端业务错误包。
- 推送缺失：检查 Logic 会话索引、Gate 回连地址、目标 session 是否仍存在；广播成功不代表客户端确认收到。
- 数据未落库：确认对应区服库、Flush 日志、脏标记及快照实现；不要在 handler 临时直接 Save 绕过框架。
- 主人操作失败：检查 owner 错误码和拓扑，发现失败不会自动退化成本地写；不要盲目重试非幂等操作。
- 日志文件混写：为实例分配不同 Name/Dir，详见 [日志](log.md)。
