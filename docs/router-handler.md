# Router 与 Handler：协议注册和请求上下文

相关文档：[Logic 运行时](logic.md)、[Demo 玩法与扩展](demo.md)、[启动与帧格式](../README.md)。本文中的 `handler.Context` 指框架包；`example/demo/handler` 是具体玩法 handler，两者不要混淆。

## 1. 职责和源码入口

- [`pkg/router/router.go`](../pkg/router/router.go)：泛型 `Router[K comparable]` 保存键到原始处理函数的映射，`Register` 在边界执行 protobuf 解码和编码。
- [`pkg/handler/context.go`](../pkg/handler/context.go)：请求身份、角色读取依赖、登录绑定回调和统一日志字段。
- [`pkg/logic/app.go`](../pkg/logic/app.go)：在会话 actor 内创建上下文、注入 loader、执行路由并收集响应身份。
- [`example/demo/register.go`](../example/demo/register.go)：真实注册示例。
- [`pkg/router/router_test.go`](../pkg/router/router_test.go)：`int32` 与 `string` 键的端到端编解码测试。

Router 不选择节点、不建立 actor、不做认证、不维护角色缓存，也没有中间件、错误码映射或并发注册保护。Handler 上下文不是存储层，不缓存 `GetPlayer` 的结果，也不自动回包。

## 2. 实际执行链路

普通客户端帧的 `cmdId` 是 `int32`。Gate 经 Kitex 转成 `logic.Request` 后，`App.Handle` 先进入会话 actor，再创建框架 `handler.Context`，最后执行：

`Router.Dispatch(key, ctx, body)` → 查找原始函数 → `proto.Unmarshal` → 具体玩法 handler → service → entity/同步本地事件 → handler 组响应 → `proto.Marshal`。

普通会话 actor 的实际键为 `session/<SessionID>`，空会话号为 `session/player`。cmd 0 是独立的通用主人入口：`App.Owners.Handle` 在会话邮箱外接收信封，再进入 `owner/...` 实体邮箱，不经过 Router 或 handler.Context；缺 Owners 直接失败。主人操作通过 `owner.Register/Call` 注册调用，不是用 Router 注册游戏 cmd 0，详见 [Logic 主人运行时](logic.md#4-公会主人路由当前实现与限制)。

`Register` 只包装协议边界，不会把函数自行调度到 actor。直接在任意协程调用 `Dispatch`，业务就会在该协程运行。生产玩家协议依靠 `App.Handle` 保证同会话串行；不要绕过它并发改角色。

身份来源是 `Meta{ServerID, PlayerID, SessionID, GateID, CmdID}`。框架不会验证这些字段来自可信连接，也不检查 `Dispatch` 的路由键与 `Meta.CmdID` 一致。默认装配由 Logic 填好它们，独立调用者须自行保证。

## 3. Router 的主要 API

### 创建、注册与分发

- `router.New[K]()` 创建空路由。`K` 必须 comparable，例如 `int32` 或 `string`；两种键使用不同实例，不在一张表中混用。
- `r.Handle(key, h)` 注册 `router.Handler`，签名为 `func(*handler.Context, []byte) ([]byte, error)`。适合测试或非 protobuf 的原始处理；业务通常使用类型化 `Register`。
- `router.Register(r, key, h)` 接收 `func(*handler.Context, *Req) (*Resp, error)`。泛型约束要求请求和响应的指针实现 `proto.Message`，所以应使用生成消息类型。
- `r.Dispatch(key, ctx, body)` 同步执行已注册函数。没有该键返回 `router: no handler for <key>`，没有独立的哨兵错误。

后注册的相同键会覆盖旧函数，没有重复注册告警。内部 map 无锁，应在启动接入前完成注册，不在请求执行时热改；注册 nil 原始函数也没有保护。

### 类型化编解码的精确语义

1. 为每次请求创建新的 `Req` 值并反序列化 Body。
2. 解码失败直接返回错误，业务函数不会运行。
3. 业务返回 error 时直接返回该错误，不编码其响应。
4. 业务返回 `(nil, nil)` 时返回 `(nil, nil)` Body；这是成功的空响应，不代表框架自动发送推送或跳过 Gate 回包。
5. 非 nil 响应交给 `proto.Marshal`，其错误向外传播。

空 Body 在 protobuf 中可能是合法的零值消息；“是否必填”“是否允许空名字”等语义必须由 handler/service 校验。

## 4. Context 的主要 API

### 构造与角色读取

`handler.New(ctx, meta)` 保留传入的 `context.Context`，nil 时换成 `context.Background()`；`Meta` 被嵌入，可直接用 `ctx.ServerID` 等字段。它不是不可变结构，修改身份字段需格外谨慎。

`WithLoader(load)` 注入 `handler.Loader`：

```go
func(ctx context.Context, serverID int32, playerID int64) (any, error)
```

loader 实现必须通过缓存 `Get` 取得实体，缓存未命中时由框架内部加载。Demo 注入 [`PlayerService.GetByID`](../example/demo/service/player.go)。不要在新增业务 handler 中调用存储 `Load`；Demo 业务编号到 ObjectID 的索引查询细节见 [Demo 数据边界](demo.md#2-实体身份缓存与持久化)。

`ctx.GetPlayer()` 的顺序是：

1. `PlayerID == 0` 返回 `handler.ErrNotLoggedIn`，即使没有 loader 也是先返回未登录。
2. loader 为空返回 `handler: player loader is not set` 普通错误。
3. 调用 loader，透传返回值和错误，框架不验证实体类型。业务根据约定断言为自己的角色类型；不安全断言可能 panic。

负数 `PlayerID` 不会被本层当作未登录。每次 `GetPlayer` 都调用 loader，不过正常 loader 会命中同一缓存实体。上下文应只在当前 actor 请求内使用，不把它及角色实体传给别的 actor 修改。

### 登录绑定与日志

`WithBind(func(playerID int64))` 设置同步回调，`BindPlayer(id)` 先修改当前上下文的 `PlayerID`，再调用回调。没有 loader、鉴权、编号范围或防止重复登录校验；它只是绑定机制。默认 Logic 在成功响应中带回 `PlayerID`，Gate 再写会话，因此后续请求必须携带 Gate 记住的编号。

`ctx.Logger()` 返回带 `Info/Warn/Error` 的接口，内部通过 `tuserv/pkg/log.With` 添加 `server_id`、`player_id`、`cmd_id`。它没有默认加上 Session/Gate 字段；需要时作为结构化参数补充。不要另用标准库日志。

## 5. 实际使用示例

下面可作为独立程序验证类型化注册和调用；不涉及角色修改，因此直接 `Dispatch`，不是玩家玩法的 actor 装配替代品。

```go
package main

import (
    "context"
    "fmt"

    "google.golang.org/protobuf/proto"
    "google.golang.org/protobuf/types/known/wrapperspb"
    "tuserv/pkg/handler"
    "tuserv/pkg/router"
)

func main() {
    r := router.New[int32]()
    router.Register(r, int32(7), func(ctx *handler.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
        ctx.Logger().Info("收到演示请求")
        return wrapperspb.String(req.GetValue() + "-ok"), nil
    })
    body, err := proto.Marshal(wrapperspb.String("hello"))
    if err != nil {
        panic(err)
    }
    ctx := handler.New(context.Background(), handler.Meta{ServerID: 1, CmdID: 7})
    out, err := r.Dispatch(7, ctx, body)
    if err != nil {
        panic(err)
    }
    var resp wrapperspb.StringValue
    if err := proto.Unmarshal(out, &resp); err != nil {
        panic(err)
    }
    fmt.Println(resp.GetValue())
}
```

### 扩展一条真实玩法协议

1. 在 [`game.proto`](../example/demo/proto/game.proto) 添加请求/响应；按 [Demo 生成说明](demo.md#7-扩展玩法的步骤) 重新生成，禁止手改 `gen`。
2. 在业务 handler 包定义正数 `int32` 命令号，不占用 Logic 的内部 0 和断线负数。推送号与请求号分开。
3. handler 校验结构和范围，调用 service；service 使用 `ctx.GetPlayer()`，修改实体并 `MarkDirty`，不在 handler 落库或编写 Mongo 查询。
4. 在启动阶段的 `demo.Register` 集中调用 `router.Register`，注入真实依赖。
5. 本地事件同步在当前 actor 执行。共享实体操作复用 `owner.Key/Register/Call`，本地与远端都在实体邮箱修改并返回 DTO；调用依赖须在启动阶段注册、装配到 App.Owners。玩家 actor 可首次调用独立主人邮箱，但主人操作内不要同步重入同一 Runtime（返回 owner.reentry），也不要绕过上下文标记同步 RPC 回自己；跨节点通知独立协程或单向发出。
6. 通过真实 `App.Handle` 验证身份、actor 与回包，而不只测裸 handler。

## 6. 错误传播与不保证的行为

Router 和 Context 不将错误包装为协议错误码。`App.Handle` 对路由错误不登记本次会话、不生成成功响应；`logicAPI.Dispatch` 将 error 交给 Kitex；Gate 对所有 Dispatch 错误记录日志、清除 Logic 绑定并丢弃本帧响应，下一帧才重新选择节点。业务错误也走同一路径，客户端目前收不到结构化的道具不足等错误消息。

错误不意味着无副作用：service 已修改实体后发生监听者异常、响应编码错误或上游超时，都没有事务撤销。普通会话路径中 `Actors.Do` 的 panic recover 仅记日志，不转成业务错误，因此不能把 panic 当成正常错误返回手段。最外层客户端 Kitex 错误也不应依赖 `errors.Is` 保持本地哨兵身份。

通用主人路径不同：Runtime 将操作 panic 转为 `owner.panic`，操作错误通过 JSON 信封恢复为 `owner.CodeError`，`owner.Call` 的本地/远端调用均可按错误码 `errors.Is`（如 `guild.not_found`）。这不是 Router 的能力，也没有自动变成客户端 protobuf 错误包；公会 handler 返回该错误时，Gate 仍按上述普通 Dispatch 失败处理。更多运行时限制见 [Logic 错误边界](logic.md#6-错误与并发边界)。

## 7. 测试依据和建议

- [`TestIntAndStringKeys`](../pkg/router/router_test.go) 实测 `int32` 与 `string` 的类型化注册、Dispatch、请求/响应 protobuf 编解码。
- [`TestDemoFlows`](../example/demo/demo_test.go) 经 `App.Handle` 覆盖登录绑定、loader 读角色、经验升级、本地同步奖励、道具不足错误、聊天和创建公会；`errors.Is` 的断言只针对进程内调用。
- [`pkg/logic/owner_test.go`](../pkg/logic/owner_test.go) 验证内部 cmd 0 走实体邮箱、不登记会话及缺运行时失败；[`pkg/owner/owner_test.go`](../pkg/owner/owner_test.go) 验证主人操作的错误码、panic、重入、串行、DTO 隔离和协议/注册校验。它们不能替代客户端 Router 的编解码测试，远端 Transport 也是进程内测试适配器而非真实 Kitex。
- 当前 `pkg/handler` 没有独立测试，Router 测试也没有覆盖缺失路由、非法 protobuf、nil 响应、重复注册和编码失败。

后续测试应补 `GetPlayer` 未登录/未注入/loader 错误、绑定回调、业务错误短路、非法 Body、空 Body 语义及真实网关错误响应策略。本文只据源码和已有测试说明现状，本次未运行测试。
