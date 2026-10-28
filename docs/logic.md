# Logic：逻辑节点运行时

本文以当前实现为准，区分框架保证与 Demo/进程适配层行为。相关文档：[路由与请求上下文](router-handler.md)、[Demo 玩法](demo.md)、[项目启动说明](../README.md)。

## 1. 职责和源码入口

- [`pkg/logic/app.go`](../pkg/logic/app.go)：`Request`、`Response`、`Session` 和 `App`，将普通请求交给会话 actor，维护本节点广播用的会话表。
- [`cmd/logic/main.go`](../cmd/logic/main.go)：Mongo、缓存、事件、Demo 服务、Kitex、etcd 的装配，以及定时刷盘与退出处理。
- [`cmd/logic/owner.go`](../cmd/logic/owner.go)：`newOwners` 装配通用主人解析与 Kitex 传输，不包含公会专用操作。
- [`pkg/owner/owner.go`](../pkg/owner/owner.go)：类型化操作注册、实体键邮箱、本地/远端 DTO 隔离、主人复核和编码错误。
- [`example/demo/service/guild.go`](../example/demo/service/guild.go)：`RegisterOwners` 注册公会操作，业务实现位于主人运行时的操作函数内。
- [`pkg/actor/actor.go`](../pkg/actor/actor.go)：按字符串键建立容量为 64 的串行邮箱，`Do` 等待一次工作结束。
- [`pkg/gate/gate.go`](../pkg/gate/gate.go) 和 [`pkg/cluster/rpc.go`](../pkg/cluster/rpc.go)：会话钉住 Logic 地址、Kitex 请求转发和回推。

`pkg/logic` 不负责 TCP/WebSocket 解帧、protobuf 业务消息解码、鉴权、Mongo 连接或自动刷盘。它也不会因为实体声明了 `PlacementRouted` 就自动选择主人。

## 2. 实际请求链路

1. Gate 首帧按权重选择 Logic，将地址记在连接会话上；区服并不绑定一台 Logic。
2. Gate 将 `GateID`、`SessionID`、帧头 `ServerID`、会话中的 `PlayerID`、`CmdID` 和 `Body` 交给 Kitex `Dispatch`。
3. `cmd/logic` 的 `logicAPI.Dispatch` 转成 `logic.Request`，调用 `App.Handle`。
4. 普通请求使用 `"session/" + SessionID` 作为 actor 键；空会话号对应 `"session/player"`，不是按 `PlayerID` 建 actor。该命名空间与 `owner/...` 实体邮箱隔离。
5. actor 内创建 `handler.Context`，注入 `App.Load` 与绑定回调，经 `Router.Dispatch` 执行 protobuf 解码、handler、service、实体修改、同步进程内事件和响应编码。
6. 分发成功后构造 `Response{PlayerID, CmdID, Body}`。非空会话号被写入本节点会话表，记录当前区服、角色和 Gate 地址。
7. Kitex 将结果交回 Gate。非零 `Response.PlayerID` 更新 Gate 会话，Gate 用请求帧的区服号写客户端响应。

串行边界是同一个 actor 键，而不是同一个角色编号；不同会话即使指向同一角色，也不自动互斥。业务应在会话 actor 内完成 handler、service、实体修改及本地事件，不把角色指针交给其他协程修改。

### 登录身份如何回传

[`OnLogin`](../example/demo/handler/login.go) 调用 `PlayerService.Login` 后执行 `ctx.BindPlayer(p.PlayerID)`，更新当前上下文和 `App` 的请求副本。真正对外生效依赖整次路由成功：`App` 从 `hctx.PlayerID` 构造响应，Gate 再记住编号。`App` 不会在下一次请求时从自己的会话表补上 `PlayerID`，调用方必须随请求提供身份。

会话表是广播索引，不是鉴权系统。名字登录、同角色多会话及跨区服帧均未在本模块施加安全约束，详见 [Demo 限制](demo.md#6-错误与当前实现边界)。

## 3. 命令分支与主要 API

- `logic.New(r, load)`：创建运行时与独立 actor 系统。`r` 应预先注册完毕；`load` 签名是 `func(context.Context, int32, int64) (any, error)`，Demo 使用 `players.GetByID`，最终走缓存 `Get`。
- `App.Handle(ctx, Request)`：同步等待请求处理，返回响应或 Go 错误。没有内建业务错误包格式。
- `CmdID < 0`：立即删除会话表项并返回空响应；不经过 actor，不分发路由，不刷盘，不移除 actor 或缓存。
- `CmdID == 0`：直接在会话 actor 外调用 `App.Owners.Handle(ctx, req.Body)`，由主人运行时根据 Body 中的实体键进入实体邮箱。不创建 `handler.Context`、不走 Router、不登记在线会话；`SessionID` 不决定实体邮箱。
- `CmdID == 0 && App.Owners == nil`：返回 `logic: owner runtime unavailable`，不会回落到普通路由。0 保留给节点内部操作，客户端玩法使用正数。
- `Sessions(serverID)`：返回本节点对应区服的会话值切片；遍历顺序不保证，只代表最近一次成功普通请求记录的状态。
- `App.Stop()`：关闭 actor 邮箱，不等同于停止 Kitex、刷盘或等待所有 actor 退出。

### 最小装配示例

下面使用内存存储演示真实 API，可放在单独示例程序中。注册阶段应先于请求接入；依赖不支持任意留空。

```go
package main

import (
    "context"
    "fmt"

    "google.golang.org/protobuf/proto"
    "tuserv/example/demo"
    "tuserv/example/demo/gen/game"
    demohandler "tuserv/example/demo/handler"
    "tuserv/example/demo/service"
    "tuserv/pkg/cache"
    "tuserv/pkg/event"
    "tuserv/pkg/logic"
    "tuserv/pkg/owner"
    "tuserv/pkg/router"
    "tuserv/pkg/store/memory"
)

func main() {
    st := memory.New()
    c := cache.New(st)
    bus := event.New()
    players := service.NewPlayerService(c, st, bus)
    r := router.New[int32]()
    app := logic.New(r, players.GetByID)
    defer app.Stop()
    owners := owner.NewLocal(app.Actors)
    guilds := service.NewGuildService(c, st, owners)
    if err := guilds.RegisterOwners(); err != nil {
        panic(err)
    }
    app.Owners = owners
    demo.Register(r, demo.Deps{
        Players: players,
        Bags: service.NewBagService(),
        Chat: service.NewChatService(nil),
        Guilds: guilds,
        Bus: bus,
    })
    body, err := proto.Marshal(&game.LoginReq{Name: "ann"})
    if err != nil {
        panic(err)
    }
    // 此示例只调用登录；聊天实际使用前必须注入广播器。
    resp, err := app.Handle(context.Background(), logic.Request{
        GateID: "127.0.0.1:9001", SessionID: "s1",
        ServerID: 3, CmdID: demohandler.CmdLogin, Body: body,
    })
    if err != nil {
        panic(err)
    }
    fmt.Println(resp.PlayerID)
}
```

缓存 `Put` 使新角色变脏，但上述示例没有自动持久化循环。进程版的实际装配见 [`main.go`](../cmd/logic/main.go)；接业务时不要把这个内存示例误当生产入口。

## 4. 公会主人路由：当前实现与限制

### 装配与通用 API

当前入口先创建 `app`，取 `cfg.AdvertiseAddr`（为空则取 `cfg.ListenAddr`）作为 self，调用 `newOwners(self, app.Actors, nodes)`，将运行时传给 `NewGuildService`，检查 `guilds.RegisterOwners()` 的注册错误，设置 `app.Owners=owners`，最后注册客户端协议。单节点测试显式用 `owner.NewLocal(app.Actors)`；公会服务不再允许 nil 主人运行时。

- `owner.EntityKey` 由 `ServerID/Kind/ID` 组成；`owner.Key(serverID, kind, id)` 构造键，`String()` 产生 `owner/<serverID>/<转义kind>/<ID>`。公会 `RouteKey()` 使用同一格式，不再是二段字符串。
- `owner.New(self, actors, resolver, transport)` 注入地址解析和传输。`Resolver` 返回当前主人地址，`Transport` 传递编码后的请求与响应；缺失配置返回错误，发现失败不自动降级本地。
- `owner.Register(runtime, name, fn)` 注册 `func(context.Context, owner.EntityKey, Req) (Resp, error)`。操作名重复返回 `owner.duplicate`，与 Router 覆盖同键的语义不同。
- `owner.Call[Resp](ctx, runtime, key, operation, args)` 对参数和响应执行 JSON 编解码，本地与远端都复制 DTO，调用者不会取得主人实体指针。
- `Runtime.Handle(ctx, body)` 是内部接收入口，必须在实体 actor 外调用；App 的 cmd 0 分支直接交给它，避免先占据邮箱再同步等待自己。

### 从玩家到主人再返回

`GuildService.Create/Join` 在玩家 actor 调用 `owner.Call[GuildResult]`，操作名分别为 `guild.create`、`guild.join`。本地主人也进入 `EntityKey.String()` 对应的实体 actor，和远端请求共用执行路径，**不再在玩家 actor 直接修改公会**。

远端传输由 `cmd/logic/owner.go` 通过 Kitex 通用 `Dispatch` 发送 `Request{CmdID:0, Body:body}`。Body 是通用 JSON 信封（operation/key/args/hops），不是公会专用载荷，不靠 `SessionID` 路由；接收端要求 hops 恰为 1，不转发第二跳。进入实体邮箱后再次解析主人，若已不是 self 则返回 `owner.moved`，未知操作返回 `owner.unknown_operation`。

操作函数在主人端读取/修改公会、打脏并返回 `GuildResult` 快照；发起端仅在调用成功后改会话角色 `GuildID` 并打脏，不缓存另一份远端公会。本地操作同样隔离响应 DTO。

### 错误、重入与剩余边界

`owner.CodeError`/`owner.NewError(code,message)` 以错误码实现 `errors.Is`。主人操作产生的编码错误会放进响应信封，`owner.Call` 在发起端恢复 CodeError；普通未编码错误在操作边界归为 `owner.internal`。这项能力只覆盖主人调用边界，不等同于客户端业务错误协议。主人操作 panic 被转换成 `owner.panic`，取消后的结果通过缓冲通道发布，不共享可变结果变量；取消仍不撤销已执行的实体副作用。

传递操作收到的上下文再次同步调用同一个 Runtime 时，运行时返回 `owner.reentry`，即使目标是另一实体键也拒绝。检测依赖上下文标记和 Runtime 身份，不能当作任意分布式回调环的全局检测；不要换 Background 绕过标记或同步 RPC 回自己。玩家 actor 首次调用独立主人邮箱是支持的，主人操作内则应避免同步嵌套主人调用；跨节点通知使用独立协程或单向调用。

仍需明确以下限制：

- [`cluster.Owner`](../pkg/cluster/pick.go) 是地址排序后 `FNV-1a(key.String()) % 节点数`，**不是一致性哈希环**。权重不参与，拓扑变化可能大规模改主人，没有迁移、fencing、旧缓存失效或共识协议。邮箱内复核只能拒绝当前解析已变更的请求，不构成全局所有权租约。
- 主人变化返回 `owner.moved`，运行时不自动重定向或重试；调用侧需设计幂等和安全重试，不能盲目重放创建。
- 内部 JSON 信封和公会操作有格式/参数校验，但没有节点身份认证、用户权限检查；cmd 0 应由可信节点通道使用，App/Gate 本身并未建立额外的客户端 cmd 0 访问控制。
- 公会创建当前私有 `keep` 不做幂等冲突校验；角色与公会也没有跨实体事务或失败补偿。

更完整的实体和业务步骤见 [Demo 公会链路](demo.md#4-公会创建与加入)。

## 5. 广播、持久化和生命周期

`ChatService.Send` 先在玩家 actor 内读取角色、编码 `ChatPush`，再用独立协程 `NotifyAll`。每个 Logic 的 `logicAPI.NotifyServer` 用 `Sessions(serverID)` 筛选本节点在线会话，通过该会话的 Gate 地址回推。广播范围是区服，节点可以同时服务多个区服；内部公会请求不会污染在线表。

刷盘不由 `App` 管理：`cmd/logic` 创建定时器，调用 `cache.Flush`；收到退出信号后再 Flush，然后 `app.Stop()`、`svr.Stop()`、关闭存储。实体修改由 service/事件调用 `MarkDirty`，不在 handler 逐次写库。新实体 `cache.Put` 已打脏。

当前退出实现没有先停止接入、等待 actor 清空、再做最终刷盘的完整屏障，定时器也未显式取消。`Actor.Stop` 关闭 channel 后仍可能消费队列，不等待消费完；不要把退出前这次 Flush 理解成所有并发请求已完成后的持久化保证。

## 6. 错误与并发边界

- protobuf 解码、业务处理、编码或未注册路由失败：`App` 不构造成功响应，也不更新会话表；之前已发生的内存修改没有事务回滚。
- 普通客户端路径中 `logicAPI.Dispatch` 原样返回错误，不能假设外层 Kitex 保留服务内哨兵身份；现有客户端协议没有统一业务错误码响应。主人调用另有 JSON 错误信封，在 `owner.Call` 边界可按 CodeError 错误码匹配，不要混淆两条边界。
- Gate 对任何 Dispatch 错误都记录日志、清掉 Logic 绑定、不给客户端写本次错误包；不是只对网络故障处理，也不会立即重试这一帧。下一帧重新选址，已有 `playerID` 不被清空。
- `Actors.Do` 等待时上下文取消可使调用先返回错误，但已经入队/运行的任务不因此撤销；实体副作用可能稍后完成。超时不等于未执行，不宜盲目重试非幂等创建。
- 普通会话 actor 会 recover panic 并记录日志，但不把 panic 转成 `Handle` 的业务错误；闭包未正确设置结果时可能表现为空响应、nil 错误。主人运行时对自己的操作另有 recover，会返回 `owner.panic`，不能将普通会话路径的限制套用到它。
- 会话表的锁只保护表本身，不应在业务添加角色互斥锁来替代 actor。现有实体局部锁用于与持久化快照协调，不能扩大理解成跨会话/跨节点事务。
- 断线删除不经过同一个 actor，可能被仍在执行的成功请求重新登记；actor 不会随断线回收。会话标识没有 Gate 命名空间前缀，部署时还需关注全局唯一性。

## 7. 扩展及测试依据

新增玩法优先按 [路由与上下文](router-handler.md) 注册正数协议，handler 只做参数校验与组包，service 通过 `ctx.GetPlayer()` 读角色，修改实体后打脏；本地事件留在同次 actor 调用中。新增共享实体复用 `owner.Key`、`owner.Register` 与 `owner.Call`，返回纯 DTO，启动阶段注册操作，不自行复制一套主人判定/转发。`PlacementRouted` 本身不触发调用，也不允许业务绕过主人直接 Get 后修改。

现有覆盖：

- [`example/demo/demo_test.go`](../example/demo/demo_test.go) 的 `TestDemoFlows` 通过真实 `App.Handle` 验证登录、升级及奖励、道具不足、区服聊天广播和本地建会。
- [`pkg/logic/owner_test.go`](../pkg/logic/owner_test.go)：`TestInternalOwnerDispatch` 验证 cmd 0 经实体邮箱运行且不创建玩家会话；`TestInternalWithoutOwnersFails` 验证缺运行时直接失败。
- [`pkg/owner/owner_test.go`](../pkg/owner/owner_test.go)：键命名空间/转义、本地及远端 DTO 隔离、业务错误码和 panic、解析失败不降级、陈旧主人拒绝、同步重入拒绝、100 次同键串行、信封/注册校验以及取消后结果发布。
- [`example/demo/service/guild_test.go`](../example/demo/service/guild_test.go)：本地创建加入与重载、远端创建加入只在主人缓存、结果切片隔离、重复加入不重新打脏、传输失败不改角色、错误码、实体种类校验、缺运行时及重复注册拒绝。
- [`pkg/router/router_test.go`](../pkg/router/router_test.go) 验证两类路由键；[`pkg/event/bus_test.go`](../pkg/event/bus_test.go) 验证同步监听。

远端测试以进程内 Transport 调用真实 `Runtime.Handle`，并非真实 Kitex/etcd 多进程测试。会话删除/重新登记竞争、外层客户端错误响应、实际拓扑迁移与退出刷盘屏障仍未据这些测试获得保证。本次只修订文档，未运行测试。
