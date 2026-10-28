# Demo：登录、升级、背包、聊天和公会

本示例演示 handler → service → entity 的分层，以及会话 actor 内同步事件和公会主人转发。它不是完整账号系统或分布式事务实现。相关文档：[Logic 运行时](logic.md)、[Router 与 Handler](router-handler.md)、[启动与依赖](../README.md)。

## 1. 职责与源码入口

- [`register.go`](../example/demo/register.go)：`Deps` 集中注入 `Players/Bags/Chat/Guilds/Bus`，`Register` 注册六条协议，并在 Bus 非 nil 时注册升级奖励监听。
- [`entity/player.go`](../example/demo/entity/player.go)、[`entity/guild.go`](../example/demo/entity/guild.go)：内存实体、Mongo 字段、缓存键、路由键、玩法数据方法和持久化快照，不使用 protobuf 充当存档。
- [`service/player.go`](../example/demo/service/player.go)、[`bag.go`](../example/demo/service/bag.go)、[`chat.go`](../example/demo/service/chat.go)、[`guild.go`](../example/demo/service/guild.go)：读实体、执行规则、修改内存、打脏和发事件/通知。业务编号查找辅助在 [`id.go`](../example/demo/service/id.go)，哨兵错误在 [`errors.go`](../example/demo/service/errors.go)。
- [`handler`](../example/demo/handler)：协议参数校验、服务调用及响应转换；命令常量在 [`cmd.go`](../example/demo/handler/cmd.go)。框架上下文是另一个包 `tuserv/pkg/handler`。
- [`event/levelup.go`](../example/demo/event/levelup.go)：`PlayerLevelUp` 和背包奖励监听。
- [`proto/game.proto`](../example/demo/proto/game.proto)：客户端消息源定义；[`gen/game/game.pb.go`](../example/demo/gen/game/game.pb.go) 是工具生成物，只在网络边界使用，不手改。
- [`cmd/logic/main.go`](../cmd/logic/main.go)、[`cmd/logic/owner.go`](../cmd/logic/owner.go)：运行时装配、真实广播与通用主人 Kitex 适配，不在 Demo service 内建立网络客户端。

所有玩家普通请求由 `App.Handle` 放进会话邮箱，handler、service 和进程内事件在这次调用内串行完成；Router 自身不提供 actor 调度。共享实体则需要显式确认主人，不能在各节点随意保留可写副本。

## 2. 实体身份、缓存与持久化

`Player` 和 `Guild` 嵌入 `pkg/entity.State`，各有创建时生成的 Mongo ObjectID（`bson:"_id"`），`CacheKey()` 使用 `OID.Hex()`。业务编号 `PlayerID/GuildID` 存在 `bson:"id"`，由 service 调用存储 `NextID` 后通过 [`WithServer`](../pkg/entity/id.go) 编码：`serverID * 100000 + seq`。区服必须正数，序号从 1 开始且小于 100000；越界返回 `ErrIDRange`，不能由请求提供业务编号创建实体。

角色 `PlacementLocal`，内存随会话所在节点；公会 `PlacementRouted`，路由键为 `owner.Key(serverID, "guild", guildID).String()`，例如 `owner/3/guild/300001`。这只是声明，缓存并不自动按 Placement 路由。

读取边界：

- 普通角色玩法使用 `ctx.GetPlayer()`，默认 loader 是 `PlayerService.GetByID`。
- `Login` 先用 `cache.Get(serverID, "player", "name:"+name, nil)` 查内存别名；未命中则 `store.Find` 查 `name` 字段，再按 ObjectID 调用缓存 `Get`，并建立名字、十进制业务编号别名。
- `GetByID` 和公会读取辅助 `remember` 先查十进制编号别名，未命中用 `store.Find` 查 `id`，再按查到的 ObjectID 走缓存 `Get` 并挂别名。业务编号不是 Mongo `_id`。
- 当前 Demo 确实包含这些 `store.Find` 与 `NextID` 调用，并非声称所有 service 都完全不接触 Store；它们用于索引解析/分配，不应在新增玩法 handler 中复制 Mongo 查询。实体本体加载通过 `cache.Get` 的内部 Load 完成，不从业务直接调用 `store.Load`。
- `GuildService.Get` 封装上述 `remember` 缓存读取，只应在主人邮箱内调用；普通玩法通过主人操作取得快照，不直接持有公会可变实体。

新实体 `cache.Put` 自动 `MarkDirty`。修改已有实体后由 service 或监听者打脏，定时与退出 Flush 在 Logic 进程，不在每个协议中同步 Save。`Persist()` 用 `State.Capture` 拷贝 map/slice 形成文档快照，再以代数避免刷盘期间的新修改被错误清脏。

现有 `AddExp/BagAdd/AddMember` 等方法里有与快照协调的局部锁；这是实现现状，不是把角色执行改成锁模型。不要新增业务互斥锁替代 actor 串行；也不能据此认为所有直接字段写入都已获得完整的快照同步保证。

## 3. 普通玩法执行链路与 API

### 3.1 登录：1001

[`OnLogin`](../example/demo/handler/login.go) 接收 `game.LoginReq{Name}`，先校验名字非空且 `len(name) <= 16`；`PlayerService.Login(ctx, serverID, name)` 再验证一次，以防非协议调用绕过。

缓存和索引查询都未找到时，创建 1 级、经验 0、空背包角色，申请序号，编码业务编号，Put 并挂名字/编号别名。找到已有角色则返回同一缓存对象。handler 调用 `ctx.BindPlayer(p.PlayerID)`，响应 `LoginResp{PlayerId, Level, Exp}`。Logic 成功回包携带角色编号，Gate 将编号写到连接会话；后续请求靠 Gate 传入该编号，而不是 Logic 从会话表自动补齐。

此处仅按区服和名字识别角色，没有密码、token、账号归属、重登踢人或跨节点在线排他。两个会话可能操作同一缓存角色却使用不同邮箱；不能当作生产登录安全保证。

### 3.2 加经验：1002 与同步奖励

[`OnLevelUp`](../example/demo/handler/levelup.go) 接收 `LevelUpReq{Exp}`，handler 与 `PlayerService.AddExp` 都要求 `Exp > 0`。service 通过 `ctx.GetPlayer()` 得到 `*entity.Player`：

1. `p.AddExp(exp)` 累加经验；升级阈值 `NeedExp(level) = 100 * level`，最低按 1 级计算。
2. 可一次升多级，实体方法返回每次达到的新等级列表。
3. service 为每个等级发布一个值类型 `event.PlayerLevelUp{Player:p, Level:level}`。
4. [`pkg/event.Bus.Publish`](../pkg/event/bus.go) 按动态类型匹配，当前协程依次调用监听；`OnLevelUpReward` 每次为道具 `9001` 加 1 并 `MarkDirty`。
5. service 最后 `p.MarkDirty()`，handler 返回当前 `Level/Exp`。

监听者在同一玩家 actor 内同步修改同一实体，没有异步奖励队列。发布前经验和全部等级已更新，监听者应通过 `ev.Level` 知道对应哪一级，而不能认为 `ev.Player.Level` 是逐级推进的中间值。值事件与指针事件类型不同，不会相互匹配。

示例：1 级加 300 经验会达到 3 级、剩余 0，发布等级 2、3 两个事件，共奖励 2 个道具。

### 3.3 使用道具：1003

[`OnUseItem`](../example/demo/handler/item.go) 接收 `UseItemReq{ItemId, Count}`；`BagService.Use(ctx, itemID, count)` 也校验二者正数。通过 Get 读角色，库存不足返回 `ErrNotEnough` 且不扣减，足够则 `BagAdd(itemID, -count)`、`MarkDirty` 并返回剩余数。数量扣到 0 时删除 map 键。

当前只演示扣库存，不包含道具使用效果、配置表、流水、事务或溢出保护。

### 3.4 区服聊天：1004，推送 2001

[`OnChat`](../example/demo/handler/chat.go) 与 `ChatService.Send` 都验证 `1 <= len(text) <= 80`，通过 Get 读取角色名字，编码 `ChatPush{Name, Text}`。它捕获编码后的 Body 和区服号，独立协程调用 `Broadcaster.NotifyServer(context.Background(), serverID, 2001, body)`，普通请求立即返回空消息 `ChatResp`。

进程广播器经 `Nodes.NotifyAll` 访问所有 Logic；每个 Logic 用 `App.Sessions(serverID)` 筛选本节点会话，再按会话 Gate 地址推送。因此区服是广播范围，不代表某台固定节点。不在玩家 actor 内同步等待可能回本节点的广播 RPC。

广播错误被忽略，使用 Background 而非请求取消上下文；成功的 `ChatResp` 只表示完成本地校验及发起异步工作，不承诺送达。`ChatService` 不改实体或写库。

## 4. 公会创建与加入

### 服务 API 与装配

[`GuildService`](../example/demo/service/guild.go) 显式依赖 [`owner.Runtime`](../pkg/owner/owner.go)，不再由业务分成“本地直接改”和“远端专用接口”两条路径。

- `NewGuildService(cache, store, owners)`：owners 必须非 nil，否则立即 panic；单节点通过 `owner.NewLocal(actors)` 显式配置。
- `RegisterOwners() error`：启动阶段注册 `guild.create`、`guild.join`，重复注册报 `owner.duplicate`，必须检查错误。
- `Create(ctx, name) (*GuildResult, error)`、`Join(ctx, guildID) (*GuildResult, error)`：在玩家 actor 内先 Get 角色并检查未入会，然后调用通用 `owner.Call[GuildResult]`，成功后才更新角色并打脏。
- `GuildResult` 是快照 DTO，字段为 `GuildID/ServerID/Name/Members`，没有实体 OID、State 或可调用的实体修改方法。返回的 Members 不引用主人实体切片；本地调用也隔离。
- `Get(ctx, serverID, guildID)`：内部通过 `remember` 获取缓存实体，只应在主人邮箱中使用。方法自身不验证调用位置，不能在普通玩家 handler 直接拿来修改共享实体。
- 新公会缓存方法 `keep` 为私有，Put 并挂业务编号别名，不再提供外部写缓存接口。

### 创建：1005

[`OnCreateGuild`](../example/demo/handler/guild.go) 与 service 校验名字，角色已有公会返回 `ErrAlreadyInGuild`。service 从存储分配序号、WithServer 编码，创建 ObjectID，调用 `owner.Call[GuildResult](ctx, owners, owner.Key(serverID,"guild",guildID), "guild.create", args)`。

主人邮箱中的 `createOnOwner` 再校验 Kind、区服、编号、OID、玩家号、名字；参数不合法返回 `guild.bad_arg`。合法时建立仅含创建者的公会实体，私有 keep 放入主人缓存并打脏，返回快照。发起端收到成功快照后设置本地角色 GuildID 并 MarkDirty，handler 返回 `GuildResp{GuildId, Name}`。调用失败时不改角色，但序号可能已消耗；如果主人已执行而响应丢失，也可能留下部分成功。

### 加入：1006

handler/service 拒绝 0 公会号，Get 角色并检查未入会；随后调用同一实体键的 `guild.join`。主人操作再次要求正区服、正公会号、正玩家号和 Kind=guild，负公会号会在此返回 `guild.bad_arg`，不是继续当合法编号查库。

`joinOnOwner` 在主人邮箱内通过 `Get` 获取实体，不存在转换为 `owner.NewError("guild.not_found", ...)`。只有尚无该成员时才 AddMember 和 MarkDirty，重复主人操作不会追加成员或制造无意义脏标记。发起端仅在成功后更新角色 GuildID 并打脏。注意普通 `Join` 对已入会角色仍返回 ErrAlreadyInGuild，幂等成员语义是主人操作层的行为。

### 通用主人执行和传输

[`cmd/logic/owner.go`](../cmd/logic/owner.go) 的 `newOwners` 将 `cluster.Owner(list,key.String())` 注入 Resolver，将 Kitex `Dispatch(Request{CmdID:0,Body:wire})` 注入 Transport。进程入口使用 app.Actors 创建运行时，RegisterOwners 后设置 app.Owners；cmd 0 在会话邮箱外交给 Runtime.Handle，不依赖 SessionID，也不登记在线会话。

通用 JSON 信封包含 operation/key/args/hops，接收端要求远程 hops=1。无论本地还是远端，操作都进入 `owner/<区服>/<转义种类>/<编号>` 邮箱，**本地主人不再在玩家 actor 直接修改公会**。邮箱内再解析当前主人，不匹配则返回 owner.moved，不转发第二跳；发现失败直接失败，不降级为本地。操作未知返回 owner.unknown_operation，不再空成功。

参数和结果均通过 JSON 复制。主人操作错误封装为 CodeError 信封，发起端 owner.Call 可通过 `errors.Is(err, owner.NewError("guild.not_found", ""))` 按码匹配；操作 panic 返回 owner.panic。普通错误归为 owner.internal。这是内部主人边界的错误保证，不代表最外层客户端请求已有错误码回包。

### 重入与剩余限制

运行时利用操作上下文标记拒绝同步重入同一 Runtime，返回 owner.reentry，目标为其他实体键也拒绝。不要换 Background 绕过标记，也不要同步 RPC 回当前 actor；该机制不能检测任意跨进程/新上下文回调环。玩家 actor 首次等待独立实体邮箱是支持的；主人操作内不要同步嵌套调用同一运行时，跨节点通知使用异步或单向形式。

仍存在以下边界：

- 主人选择是地址排序后 FNV-1a 取模，不是一致性哈希环，权重不参与；没有迁移、fencing、旧缓存失效，邮箱内复核不等同于全局租约。
- owner.moved 不自动重定向/重试；超时和取消也不撤销已执行副作用。调用方要自行设计安全幂等和补偿。
- 创建的私有 keep 尚不检查重复创建冲突；角色与公会也没有跨实体事务，刷盘或响应失败可能导致部分成功。
- 内部格式与业务参数有校验，但没有节点认证及成员权限系统；cmd 0 入口不能仅靠“内部保留号”获得安全性。

更多运行时 API 与信封说明见 [Logic 公会路由](logic.md#4-公会主人路由当前实现与限制)。

## 5. 实际接入方式

完整的内存装配和 `App.Handle` 示例见 [Logic 最小装配](logic.md#最小装配示例)。生产进程已有 [`cmd/logic/main.go`](../cmd/logic/main.go)，真实依赖包含缓存/存储、Bus、Broadcaster 和通用 owner.Runtime。先创建 app，再用 app.Actors 装配运行时，检查 RegisterOwners 后设置 app.Owners，最后注册客户端协议；单节点同样必须显式构造 NewLocal。

`demo.Register(r, Deps{...})` 应在第一条请求前执行，并且整个进程通常只执行一次：Router 的同键注册会覆盖，但事件监听会追加，重复注册会造成升级重复发奖励。Bus 必须与 `PlayerService` 注入的 Publisher 指向同一事件总线；不同 Bus 会注册了却收不到事件。

不要将 `Deps` 的可空理解成自动禁用对应协议：

- `Deps.Bus == nil` 只是跳过奖励监听；PlayerService 如果 Publisher 为空，发生升级时仍可能 panic。
- `ChatService` 的 broadcaster 为空时，合法聊天会在新协程触发 nil 调用，该 panic 不受玩家 actor 的 recover 保护。
- 服务指针为空不会在注册时统一校验，调用对应协议才暴露问题。

正常客户端使用正数请求号 1001 至 1006，聊天推送号 2001。帧为 `uint32 长度 | int32 cmdId | int32 serverId | protobuf`；客户端不传 PlayerID，Gate 从登录成功会话补入。调用裸 `App.Handle` 的测试/工具则须在后续 Request 填入登录返回的 PlayerID，否则 GetPlayer 仍会判为未登录。

## 6. 错误与当前实现边界

### 输入与返回错误

- `ErrBadName`：名字空或 Go `len` 大于 16，适用于角色/公会。
- `ErrBadExp`：经验不大于 0。
- `ErrBadArg`：道具号/数量非正数或加入公会号为 0。负公会号会继续进入主人调用，再由主人参数校验返回编码错误 `guild.bad_arg`。
- `ErrBadText`：聊天文本字节数不在 1 到 80。
- `ErrNotEnough`：背包数量不足。
- `ErrAlreadyInGuild`：角色已有公会，不区分重复加入同一会。
- `handler.ErrNotLoggedIn`：GetPlayer 看到 PlayerID 为 0；另有 loader 未设置、存储 Find/Get/NextID、ObjectID 解析、RPC、编号越界等错误。

长度按 UTF-8 字节数，不是 Unicode 字符数，也没有 TrimSpace；中文通常一个汉字占多个字节，纯空格名字/文本可能通过。升级经验与道具数量均使用 `int32`，没有完整数值上限、等级上限或算术溢出保护。

### 请求错误走到哪里

handler/service 返回错误 → `router.Register` 不编码响应 → `App.Handle` 返回 error，且本次不更新在线会话表 → `logicAPI.Dispatch` 返回 RPC 错误 → Gate 记录日志、清空会话 Logic 地址并丢掉本次响应，下一帧重新选节点。这里**没有业务错误 protobuf 回包或统一错误码**，道具不足也会触发同一路径。普通哨兵错误不保证经最外层 Kitex 保持身份。另须区分：内部 `owner.Call` 会从 JSON 错误信封恢复 CodeError，因此主人业务错误在本地/远端均可按码 errors.Is；但 handler 再将该错误返回给 Gate 时，仍走外层 RPC 错误路径。

失败没有事务回滚；请求取消不撤销已入邮箱的任务；监听者 panic 由 actor 记录但不转成明确业务错误，可能出现空成功响应。重复/超时请求没有通用幂等键。不要将“没有成功回包”理解成“没有改内存”。

### 数据与在线限制

名字/业务编号索引解析在 service 中完成，没有跨节点按名字创建的并发排他。未刷盘的数据只存在本节点，错误导致重新选址或进程崩溃后可能读不到；缓存不自动随断线回收。实体与异步持久化的安全性不能替代账号级单会话或跨节点唯一写者机制。

## 7. 扩展玩法的步骤

1. **协议源**：修改 `example/demo/proto/game.proto`，从仓库根目录运行生成命令（本文仅提供命令，没有执行）：

```text
protoc --go_out=. --go_opt=module=tuserv example/demo/proto/game.proto
```

2. **生成边界**：只让工具更新 `example/demo/gen`，禁止手改 `game.pb.go`；节点接口需要变动时另改 `idl/node.proto` 并运行 Kitex 生成，禁止手改 `kitex_gen`。不要为每个玩法协议新增 Kitex 方法。
3. **实体**：新增存档字段放 entity，必要时同步更新快照文档和 `Persist`；不将生成 protobuf 当存档。玩法方法与业务序列化仍围绕 actor，不引入业务锁来替代邮箱。
4. **服务**：通过 `ctx.GetPlayer()` 或缓存 Get 读实体，修改后 `MarkDirty`。复用服务处理校验，不直接在 handler 拼查询、Save/Flush。新增共享实体使用 `owner.Key`、启动阶段的 `owner.Register` 及调用端 `owner.Call[DTO]`，在主人操作里 Get/修改/打脏并返回快照，不再自行实现专用转发器或暴露实体指针。
5. **事件**：用具体值类型注册同步监听，与发布类型保持一致；监听者在当前 actor 可改该角色并打脏，不启动协程继续改实体。跨节点通知捕获不可变载荷后异步发出。
6. **Handler 与注册**：handler 只负责校验、服务调用与组包，新增命令集中进 `cmd.go` 和 `Register`；依赖通过 `Deps` 注入，入口装配不得留缺失服务。
7. **测试**：先测实体规则/服务错误，再经 App.Handle 验证协议、身份及事件，最后对需要的分布式边界补集成测试。所有新增 Go 示例/代码注释使用中文。

## 8. 已有测试及覆盖边界

- [`demo_test.go`](../example/demo/demo_test.go) 的 `TestDemoFlows`：内存 Store + Cache + Bus + Router + Logic；区服 3 登录为 1 级，加 100 经验到 2 级、奖励道具 9001、使用一次成功后再次得到 ErrNotEnough、聊天记录区服 3 与命令 2001、本地建会后创建者在成员中。
- [`service/guild_test.go`](../example/demo/service/guild_test.go) 的 `TestLoginUsesObjectIDAndIncrement`：不同 ObjectID、业务号 100001/100002；Flush 后换缓存按业务号读取，ObjectID 不变。
- 同文件 `TestGuildRemoteCreateJoinOnlyOwnerCaches`：两套 Runtime/Cache 经进程内 Transport 执行真实远端 Handle，验证远端创建/加入、仅主人缓存、返回 DTO 与 Members 切片隔离，以及重复主人加入不产生多余脏标记。
- 同文件 `TestGuildLocalFlowAndGetReload`：显式 NewLocal 下创建加入、结果隔离、Flush 后新缓存 Get 重载及复用。
- 同文件 `TestGuildFailuresDoNotChangePlayer`：传输失败和公会不存在不改变角色公会/脏标记，远端 guild.not_found 可按码匹配；`TestGuildOwnerRejectsWrongEntityKind`、`TestGuildRequiresRuntime`、`TestGuildRegisterOwnersRejectsDuplicate` 验证种类、装配依赖和重复操作注册。
- [`pkg/owner/owner_test.go`](../pkg/owner/owner_test.go)：键隔离与转义、本地/远端 DTO、编码业务错误/panic、解析失败不降级、陈旧主人拒绝、重入拒绝、100 次同键串行、信封/注册校验和取消后结果发布。
- [`pkg/logic/owner_test.go`](../pkg/logic/owner_test.go)：内部 cmd 0 经实体邮箱执行、不登记玩家会话，缺运行时失败。
- [`pkg/event/bus_test.go`](../pkg/event/bus_test.go)：验证发布时同步调用监听；[`pkg/router/router_test.go`](../pkg/router/router_test.go)：泛型路由编解码。

上述远端测试覆盖真实 Runtime 的序列化与接收路径，但未启动真实 Kitex/etcd。通用同键串行和陈旧主人拒绝已有单测，不等于已覆盖真实拓扑迁移、多人本地/远端混合入会、跨节点名字登录竞争、重复注册奖励事件、非法客户端 protobuf、客户端错误回包、广播失败或部分持久化。不得将文档示例理解为这些性质已有集成测试保证。本次仅修订文档，未运行测试。
