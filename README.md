# TuServer

用 Go 写的游戏后端框架。客户端只连 Gate，节点之间用 Kitex 和 etcd。玩法走 handler、service、entity。每个区服单独一个 Mongo 库，本节点内存里的脏数据定时落库。

Go 版本是 `go 1.27`，工具链 `go1.27.3`。

## 进程

| 进程 | 配置 | 作用 |
| --- | --- | --- |
| `cmd/gate` | `config/gate.yaml` | 客户端 TCP `:8001`、WebSocket `:8002/ws`，Kitex `127.0.0.1:9001` |
| `cmd/logic` | `config/logic.yaml` | 玩家 actor、缓存刷盘，Kitex `127.0.0.1:9101` |
| `cmd/cross` | `config/cross.yaml` | 只向 etcd 注册 `CrossNode` 并应答 `Ping`，没有跨服玩法 |

区服不固定在某一台 LogicNode 上。新会话按 etcd 里的权重选一次逻辑节点，地址记在会话上，之后的包都发到这个地址。同一区服的其他玩家可以在别的节点。节点租约到期后清掉绑定，下次再选，角色从 Mongo 重新加载。租约默认 5 秒（`KITEX_ETCD_REGISTRY_LEASE_TTL`）。

已登录角色的协议按角色编号进同一个邮箱，新登录会踢掉旧会话。跨节点写入靠 Mongo 租约任期，网关重选 Logic 时旧节点先刷盘再交权。公会主人用加权 rendezvous hashing 选择，增减节点只迁移少量公会；本节点不是主人时用 Kitex 转发。

## 依赖服务

- etcd：`127.0.0.1:2379`
- MongoDB：`mongodb://127.0.0.1:27017`

库名是 `game_{serverId}`，前缀在 `config/logic.yaml` 的 `mongo.db_prefix`。刷盘间隔默认 `1s`，进程退出前再刷一次。

角色和公会各有两个编号。`_id` 是创建时生成的 Mongo ObjectID。协议里的 `playerId` 和 `guildId` 是 `serverID * 100000 + 自增号`，自增号来自该区服库的 `uuid` 集合，从 1 开始且小于 100000。调用方不传入这两个编号。

## 启动

在仓库根目录执行。配置路径是第一个参数，省略时用上表里的默认文件。多开 LogicNode 时复制 `config/logic.yaml`，改 `listen_addr`、`advertise_addr` 和 `weight`。对外地址必须能被其他节点连上，并和配置里的 `advertise_addr` 一致。节点间 Kitex 需要共享密钥：设置 `TUSERV_CLUSTER_SECRET`，或在 yaml 写 `cluster_secret`。两者都空时进程拒绝启动。示例里的 Kitex 只监听 `127.0.0.1`。

```text
go run ./cmd/logic config/logic.yaml
go run ./cmd/gate config/gate.yaml
go run ./cmd/cross config/cross.yaml
```

日志在 `logs/`，按天和大小分割，超龄文件会删掉。成功的普通协议包不打 Info。

## 客户端帧

TCP 和 WebSocket 共用同一套帧，大端：

```text
uint32 长度 | int32 cmdId | int32 serverId | protobuf body
```

长度不含自身，只含 `cmdId`、`serverId` 和 body。`serverId` 在帧头里，Gate 不用解析玩法协议就能选库和做广播。WebSocket 一条二进制消息对应一帧，路径是 `/ws`。换编解码器时实现 `pkg/protocol.Codec`，传输层不用改。

## Demo 协议

协议定义在 `example/demo/proto/game.proto`。名字最长 16 个字符，聊天文本 1 到 80 个字符。升级所需经验是 `100 * 当前等级`，每次升级给背包道具 `9001` 一个。

| cmdId | 消息 | 说明 |
| --- | --- | --- |
| 1001 | `LoginReq` / `LoginResp` | 按区服和名字登录，没有角色就建 1 级号 |
| 1002 | `LevelUpReq` / `LevelUpResp` | `exp` 必须大于 0 |
| 1003 | `UseItemReq` / `UseItemResp` | 道具不足返回错误 |
| 1004 | `ChatReq` / `ChatResp` | 同区服广播 |
| 2001 | `ChatPush` | 聊天推送，不是请求回包 |
| 1005 | `CreateGuildReq` / `GuildResp` | 已在公会中不能再建 |
| 1006 | `JoinGuildReq` / `GuildResp` | 已在公会中不能再加 |

登录完成前调用 `GetPlayer` 会返回未登录错误。

## 目录

```text
cmd/            gate、logic、cross 进程入口
config/         各进程 yaml
idl/            节点间 Kitex 协议：Dispatch、NotifyServer、Push、Ping
kitex_gen/      由 kitex 生成，不要手改
pkg/            协议、路由、actor、缓存、日志、集群
example/demo/   登录、升级、道具、聊天、公会
```

业务包只能通过缓存的 `Get` 读实体。内存没有时框架内部再从 Mongo 加载。

## 测试

单测不依赖本机 etcd 和 Mongo。

```text
go test ./...
```

## 重新生成代码

客户端协议：

```text
protoc --go_out=. --go_opt=module=tuserv example/demo/proto/game.proto
```

节点协议：

```text
kitex -module tuserv -type protobuf idl/node.proto
```
