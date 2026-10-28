# owner：共享实体主人路由与统一邮箱

本文以当前 `pkg/owner`、Logic 装配和已有测试源码为准。`owner` 把共享实体操作送到当前主人节点，在实体邮箱中串行执行；本地与远端调用使用同一套注册、参数复制、执行和错误返回语义。本文仅阅读源码，未运行测试。

## 职责与源码入口

- [owner.go](../pkg/owner/owner.go)：`EntityKey`、`Runtime`、类型化 `Register` / `Call`、JSON DTO、单跳校验、重入检查与错误码。
- [doc.go](../pkg/owner/doc.go)：包级约束。
- [owner_test.go](../pkg/owner/owner_test.go)：运行时单测。
- [cmd/logic/owner.go](../cmd/logic/owner.go)：把服务发现、实体选主和 Kitex Dispatch 注入运行时。
- [cmd/logic/main.go](../cmd/logic/main.go)：共享 `app.Actors`，注册公会操作，设置 `app.Owners`。
- [logic/app.go](../pkg/logic/app.go)、[Logic owner 测试](../pkg/logic/owner_test.go)：`CmdID == 0` 的内部入口与会话邮箱隔离。
- [公会 service](../example/demo/service/guild.go)、[公会实体](../example/demo/entity/guild.go)、[公会测试](../example/demo/service/guild_test.go)：真实共享实体用法。
- 配套：[actor](actor.md)、[cluster](cluster.md)、[entity](entity.md)、[cache](cache.md)、[gate](gate.md)。

本包不负责 Mongo 查询、实体模型、编号分配、脏标记或刷盘，也不读取 `Entity.Placement()` 自动决定路由。业务显式使用 `owner.Call`，主人操作内部再使用缓存 `Get` 和实体方法；缓存未命中加载与持久化交给框架。

## 实体键与统一邮箱

`owner.Key(serverID, kind, id)` 返回 `EntityKey{ServerID, Kind, ID}`，`String()` 的格式为 `owner/<serverID>/<转义后的 kind>/<id>`。例如 `owner.Key(12, "guild/one", 99).String()` 为 `owner/12/guild%2Fone/99`。种类使用 `url.PathEscape`，区服、种类和编号共同隔离键空间。

- 同一实体的所有操作使用同一个 `key.String()` 进入 `actor.System.Do`；操作名不参与邮箱键。本地调用和远程到达的调用不会各用一套邮箱。
- Logic 装配把 `app.Actors` 传给 owner。普通客户端协议使用 `session/<sessionId>`；共享实体使用 `owner/...`，避免会话 ID 占用实体邮箱。
- 公会的 `RouteKey()` 也返回 `owner.Key(g.Server, "guild", g.GuildID).String()`，因此实体声明、主人选择和邮箱使用同一键格式。
- 串行性只在同一个 `actor.System`、同一个键范围内成立。不同键可以并发执行，操作处理函数不能随意共享无保护的可变状态；同一进程为相同实体建立不同 actor 系统，也不会自动合并邮箱。

运行时仅要求操作名非空、实体 `Kind` 非空以及参数是有效 JSON；**没有统一检查 `ServerID > 0` 或 `ID > 0`，也不绑定操作名与 Kind**。这些业务约束必须在主人处理函数中验证。

## 主要 API 与装配

- `New(self string, actors *actor.System, resolve Resolver, transport Transport) *Runtime`：注入节点身份、邮箱、选主和传输。构造时不验证依赖，调用时才报配置错误。
- `NewLocal(actors *actor.System) *Runtime`：显式单节点模式，身份和选主结果均为 `"local"`，不需要注册中心或远程传输。发现失败不会隐式切换到此模式。
- `Resolver` 为 `func(context.Context, EntityKey) (string, error)`；返回地址必须可与 `self` 直接字符串比较。
- `Transport` 为 `func(context.Context, string, []byte) ([]byte, error)`；接收目标地址和编码请求，返回编码响应。
- `Register[Req, Resp](r, name, fn) error`：处理函数类型是 `func(context.Context, EntityKey, Req) (Resp, error)`。泛型函数是包级函数，不是 Runtime 方法。
- `Call[Resp](ctx, r, key, operation, args) (Resp, error)`：调用方指定响应类型；参数类型由 `args any` 接收，并不是编译期强类型的客户端桩。
- `(*Runtime).Handle(ctx, body) ([]byte, error)`：远端入站入口，必须在 actor 外调用，不应从会话或实体邮箱里同步调用它。
- `NewError(code, message) error`、`CodeError`：提供可跨节点按码匹配的业务错误。

注册表使用读写锁，支持并发注册与读取；操作名称在一个 Runtime 内全局唯一，不按 Kind 分区。重复名称报错而不是覆盖，没有注销接口。建议在启动接流量前完成所有注册，任一注册失败就停止装配；连续注册多个操作不是事务，后续失败不会撤销前面已注册项。

### 类型化注册与调用示例

下面是独立的 DTO 示例，展示真实 API、参数验证和错误匹配；它只计算返回值，不代表持久化实体实现。请求和响应类型可不导出，但供 JSON 处理的字段必须导出。

```go
package main

import (
    "context"
    "errors"
    "fmt"

    "tuserv/pkg/actor"
    "tuserv/pkg/owner"
)

type doubleArgs struct {
    Value int `json:"value"`
}

type doubleResult struct {
    Values []int `json:"values"`
}

func example(ctx context.Context) error {
    actors := actor.NewSystem()
    defer actors.Stop()
    runtime := owner.NewLocal(actors)

    err := owner.Register[doubleArgs, doubleResult](runtime, "counter.double",
        func(ctx context.Context, key owner.EntityKey, req doubleArgs) (doubleResult, error) {
            // 主人入口也校验实体种类，避免操作借用其他种类的邮箱。
            if key.Kind != "counter" || key.ServerID <= 0 || key.ID <= 0 || req.Value < 0 {
                return doubleResult{}, owner.NewError("counter.bad_arg", "invalid counter arguments")
            }
            if err := ctx.Err(); err != nil {
                return doubleResult{}, err
            }
            return doubleResult{Values: []int{req.Value * 2}}, nil
        })
    if err != nil {
        return err
    }

    result, err := owner.Call[doubleResult](ctx, runtime,
        owner.Key(1, "counter", 100001), "counter.double", doubleArgs{Value: 4})
    if errors.Is(err, owner.NewError("counter.bad_arg", "")) {
        return fmt.Errorf("参数不合法: %w", err)
    }
    if err != nil {
        return err
    }
    fmt.Println(result.Values[0])
    return nil
}
```

注册时的 `Req`、`Resp` 由泛型约束处理函数，但 `Call` 的操作名、参数结构与返回类型仍需调用方保持一致。它不是 schema 注册中心；不匹配可能报 JSON 错误，也可能因默认 JSON 解码接受未知字段、缺失字段而得到零值，不能代替业务参数校验。

### 当前 Logic 的远程装配

`cmd/logic/main.go` 先构造 `logic.App`，再用 `newOwners(self, app.Actors, nodes)` 创建 Runtime。`self` 优先取 `AdvertiseAddr`，为空时取 `ListenAddr`；应与发现返回的可达地址一致，避免同一节点被误认为远端。

`newOwners` 注入两部分：

1. Resolver 每次 `nodes.List(ctx, "Logic")`，再调用 `cluster.Owner(list, key.String())`，返回实例地址。发现失败或空列表错误直接返回，不退化为本地写入。
2. Transport 调用 `nodes.Dispatch(ctx, host, logic.Request{CmdID: 0, Body: body})`，只取响应 Body。不需要把实体键放进 `SessionID`，也没有新增玩法专用 Kitex 方法。

随后 `service.NewGuildService(c, store, owners)`、`guilds.RegisterOwners()` 注册 `guild.create` 与 `guild.join`，并设置 `app.Owners = owners`。`logic.App.Handle` 对 `CmdID == 0` 在进入会话 actor **之前**调用 `Owners.Handle`；该路径不更新在线会话表。未配置 Owners 时直接返回普通 Go 错误。

## 调用生命周期与 DTO 复制

### 本地与远程共用的执行链

1. `Call` 检查上下文重入和取消状态，把参数 JSON 编码，然后调用 Resolver。
2. 若目标等于 `self`，直接进入 `execute`；否则要求 Transport 非空，把内部请求的 `hops` 设置为 1，再发往选中的地址。
3. 远端 `Handle` 检查上下文、解析请求，只接受 `hops == 1`，随后也进入 `execute`。
4. `execute` 用 `key.String()` 进入实体邮箱；**在邮箱里重新 Resolver**，确认当前仍归本节点。发生变更返回 `owner.moved`，不会执行操作，也不会向新主人继续转发。本地调用同样经历首次寻址和邮箱内二次检查。
5. 按操作名查找处理函数；在附有执行标记的上下文中解码请求、调用处理函数、编码结果。panic 被转换为 `owner.panic`，邮箱可以继续处理后续任务。
6. 调用方解码结果为指定的 `Resp`，或收到编码后的错误。

内部 JSON 请求包含 `operation`、`key`、`args`、`hops`；响应包含 `result` 或 `error{code,message}`。这些结构在包内私有，业务应使用 Register / Call，不自行拼接内部报文。

### 本地也必须复制

请求先 `json.Marshal(args)`，在邮箱中 `json.Unmarshal` 成注册的 Req；返回值在邮箱内编码，调用方再解码成 Resp。因此，即使不跨节点，也不把调用方切片或返回实体指针直接共享到另一侧。测试同时检查请求切片修改不回写调用方，以及修改响应切片不改变邮箱内状态。

这是 **JSON 可表示数据的值隔离**，不是任意 Go 对象的通用深拷贝：未导出字段不传输、标签和自定义 JSON 方法影响结果，循环引用和 channel 等不可编码。应使用独立 DTO，不把包含锁、运行时状态或持久化细节的实体当响应契约；更不能从处理函数通过闭包、全局变量等旁路泄漏实体指针。

## 错误码与错误传播

`CodeError.Is` 仅比较 `Code`，不比较 Message。本地和远程的已编码业务错误都可用 `errors.Is(err, owner.NewError("guild.not_found", ""))` 判断；也可以 `errors.As` 到 `*owner.CodeError` 读取码与诊断消息。业务错误码应避开框架的 `owner.` 前缀。

框架当前产生的码：

- `owner.invalid`：注册参数不合法、Runtime 为 nil，或执行请求缺少操作名 / Kind / 有效 JSON 参数。
- `owner.duplicate`：操作名重复注册。
- `owner.config`：缺少 self、actor、Resolver，或远端调用缺少 Transport。
- `owner.resolve`：Resolver 无错误但返回空地址。Resolver 自身返回的错误不是自动改成此码。
- `owner.encode` / `owner.decode`：参数、结果的 JSON 编解码失败；Handle 收到非法 JSON 请求也是 `owner.decode`。
- `owner.hops`：Handle 收到的网络跳数不是 1。
- `owner.moved`：邮箱内复查发现当前主人不是本节点。
- `owner.unknown_operation`：未找到注册操作。
- `owner.reentry`：上下文执行链中已存在相同 Runtime。
- `owner.panic`：实体邮箱内执行发生 panic。
- `owner.internal`：需要编码回传的错误不包含 `*CodeError`。

操作返回的包装错误通过 `errors.As` 提取 Code，完整 `err.Error()` 作为诊断 Message；原错误具体类型、原始包装链和 sentinel 身份不会随 JSON 保留。即便本地操作返回普通 sentinel，也会被归一为 `owner.internal`。

不是所有错误都一定是 CodeError：Call 首次 Resolver 错误以 `resolve owner: %w` 包装，Transport 错误、远端响应外层 JSON 解析错误、actor 等待的 context 错误可直接返回。邮箱内 Resolver 错误则经响应编码，普通错误会成为 `owner.internal`。Handle 多数应用错误被放进响应 Body，其返回的 Go error 通常为 nil；因此只检查 Kitex 是否成功不能判断操作是否成功，业务应让 Call 解码响应。

## 重入、单跳与取消

- 处理函数收到的上下文包含 Runtime 执行链标记。沿用它再次同步 `Call` **同一 Runtime** 会立即得到 `owner.reentry`，即使实体键不同也拒绝。这是保守的运行时级保护，不只是同键递归检查；Handle 也执行同样的检查。
- 不要用 `context.Background()` 替换处理函数的上下文来规避保护。上下文标记不是 goroutine 身份或全局调用图，直接 actor.Do、丢弃上下文或真实 RPC 丢失 context.Value 都可能绕过它。
- `Handle` 必须由 actor 外部入口调用。框架不能自动识别所有从 actor 内同步等待自身邮箱的情形，跨节点回调环也不会仅凭该标记得到完整检测。
- `hops == 1` 是内部报文的单跳协议校验，不是整条业务调用链的分布式追踪。接收端不会二次转发，主人变更直接报错；本包没有自动重选、重试或无限转发。
- nil context 被替换成 Background；已取消 context 在 Call 入口即可返回。入队后取消可能让调用方提前返回，但**不会撤销已入队或已开始的业务操作**，处理函数应主动关注 ctx。
- 执行结果通过容量为 1 的独立通道发布，避免提前取消的调用方与仍运行的邮箱共享可变结果；这不是事务回滚，也不表示调用报超时后实体一定没被修改。

需要后续通知时，应设计独立异步任务 / 单向通知并使用 DTO；不能简单沿用带当前 Runtime 执行标记的上下文再 Call 同一 Runtime，也不要把异步投递理解为已有可靠消息保证。

## 公会真实用法与持久化边界

`GuildService.Create` 在玩家会话侧校验名称、角色公会状态，申请区服自增号并经 `entity.WithServer` 编码；创建 ObjectID 后以 `guild.create` 调主人。主人入口再次校验键与参数，构造公会并放入主人缓存。`Join` 调用 `guild.join`，主人通过 `Get` 使用框架缓存加载公会，新增成员后 `MarkDirty()`。

两者都返回 `GuildResult` 快照，成功后调用方再修改本地角色 `GuildID` 并打脏标记；非主人不缓存另一份公会。`guildResult` 显式复制 Members，运行时 JSON 边界再提供本地 / 远程一致的 DTO 隔离。重复加入已有成员不会重复追加或产生无意义脏标记。

这不构成玩家与公会之间的分布式事务：远端已成功但响应丢失、调用取消、修改玩家前进程退出等情况仍可能不一致。创建前分配的编号不会在后续失败时回退；运行时也不提供创建去重、幂等键或补偿事务。业务 `Get` 的“仅主人邮箱内调用”是使用约定，不是 owner 对缓存接口施加的强制权限。

## 实际限制

- 当前集成按地址排序后，用实体键 FNV-1a 32 位哈希取模选择主人，不看权重，不是一致性哈希。节点列表增删会重映射键。
- 邮箱内复查只针对本次发现结果；没有分布式锁、租约、任期、fencing token 或发现快照版本。各节点发现视图不同或操作执行中拓扑变化时，不能保证严格的集群唯一写入。
- 没有实体缓存迁移、旧主人驱逐、脏数据交接或新旧主人刷盘协调。稳定寻址与本地串行执行不等于完整主从切换协议。
- 没有 owner 层超时默认值、重试、持久队列、投递确认、恰好一次或跨实体事务。Transport 是普通同步函数，调用者应传合适 deadline。
- 运行时不鉴权、不验证调用来源，也不校验业务权限。Gate 当前不拦截客户端 cmdId 0，而 Logic 将 0 作为 owner 入口；需要部署隔离和入口权限治理，不能把内部 JSON 协议视为安全边界。
- Runtime 不拥有 actor 生命周期，没有 Stop；由装配方停止共享的 actor.System。底层邮箱容量为 64，满时 Do 等待或随 context 取消；邮箱按键创建，没有 owner 层空闲回收、限流或全局容量管理。
- panic 转换针对实体邮箱内执行，不是对 Resolver 首次调用、Transport 等所有外部依赖的全局 panic 保护，也不会撤销 panic 前已完成的修改。

## 已有测试与未覆盖边界

[运行时单测](../pkg/owner/owner_test.go) 实际包含：

- `TestKey`：区服 / 种类 / 编号隔离，以及斜杠、百分号转义样本。
- `TestLocalAndRemoteIsolation`：本地和模拟远程的参数 / 返回切片隔离。
- `TestErrorsAndPanics`：未知操作、包装业务错误的错误码、panic 转换，以及 panic 后邮箱仍可执行。
- `TestResolverFailureAndStaleOwner`：发现失败不回退本地、接收端发现主人改变后不执行业务且只传输一次、接收端发现错误。
- `TestReentry`：处理函数沿用上下文调用同 Runtime 的另一个实体键，返回重入错误。
- `TestSerializedExecution`：100 个并发调用同键时没有处理函数重叠，结果不丢失。
- `TestProtocolAndRegistrationValidation`：重复 / 无效注册、非法 JSON、跳数 0 / 2、参数类型错误、不可编码参数和 nil Runtime。
- `TestCancellationDoesNotPublishSharedResult`：已开始处理时取消，调用先返回 context.Canceled，释放处理函数后仍可结束。

[Logic 测试](../pkg/logic/owner_test.go) 检查 cmdId 0 经 owner 执行且不创建玩家在线会话，以及未配置 Owners 时拒绝内部请求。[公会测试](../example/demo/service/guild_test.go) 检查本地与模拟远程创建 / 加入、仅主人缓存、快照隔离、重复加入、缓存重新加载、调用失败不修改玩家、错误码、错误 Kind 拒绝、缺少 Runtime 和重复注册失败。

这些远程测试使用进程内 Transport 直接调用 Handle，没有启动真实 Kitex / etcd / Mongo。现有测试不能证明真实网络下的上下文传播、跨节点调用环防护、扩缩容迁移、发现视图分裂时的排他性或持久化可靠性。也未完整覆盖所有配置 / 编解码分支、不同键并行性、队列饱和、Stop 与在途调用并发、取消后的业务补偿或真实 RPC 超时后结果不确定性。上述仅为源码中的断言与覆盖范围说明，不是测试通过报告。
