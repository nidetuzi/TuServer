# entity：实体契约、脏代数与区服编号

## 职责与源码入口

本包只定义缓存可识别的实体接口、可嵌入的脏状态和业务编号编码，不包含 BSON 文档模型、数据库访问或路由执行器。

- [实体与 State 实现](../pkg/entity/entity.go)、[编号实现](../pkg/entity/id.go)、[包说明](../pkg/entity/doc.go)。
- 真实实体：[Player](../example/demo/entity/player.go)、[Guild](../example/demo/entity/guild.go)。
- 配套：[缓存](cache.md)、[存储](store.md)、[actor](actor.md)、[事件](event.md)。

## 实际架构与执行流程

`Entity` 要求实现：`CacheKey() string`、`ServerID() int32`、`Collection() string`、`RouteKey() string`、`Placement() Placement`，以及 `MarkDirty()`、`Dirty() bool`、`BeginSave() uint64`、`EndSave(uint64)`。

- `CacheKey` 是持久化主键的字符串表示。Demo 使用创建时生成的 Mongo ObjectID 的十六进制字符串，业务编号另存 `id` 字段。
- `ServerID` 和 `Collection` 决定缓存命名空间与存储分区。
- `PlacementLocal = 0`、`PlacementRouted = 1` 声明实体归属；角色本地、公会按实体键找主人。**仅声明不会触发路由**，缓存和存储均不检查这两个路由方法。当前公会 `RouteKey()` 使用 `owner.Key(serverID, "guild", guildID).String()`，与主人选点和实体邮箱共用 `owner/...` 命名空间；service 显式通过类型化注册与 `owner.Call` 执行主人操作，本地与远端都走同一邮箱并复制 DTO。主人内部才使用 `Get` 访问公会实体，详见 [owner 文档](owner.md)。

`State` 的零值可用，初始 `dirty=false`、`gen=0`。一次修改后的保存流程是：

1. service 修改实体后调用 `MarkDirty()`，代数加一，标记变脏。
2. 缓存筛选脏实体，调用实体的可选 `Persist() (uint64, any)`；没有该方法则退化为 `BeginSave()` 加实体本身。
3. `Persist` 可通过 `State.Capture` 在状态锁内深拷贝持久化字段并取代数；网络保存不持有这把锁。
4. 保存成功调用 `EndSave(gen)`。仅当前代数仍等于保存代数时清脏；期间发生 `MarkDirty()` 就保留脏标记，等待下一次 Flush。

`State` 只维护标记，并不自动拦截字段赋值。`BeginSave` 只读代数，不拍快照、不清脏；`EndSave` 也不验证数据库结果，由调用方保证仅在保存成功后调用。

## 主要 API 与使用示例

使用已有 Demo 实体的调用片段：

```go
func grant(p *demoentity.Player, exp int32) {
    // AddExp 自身不负责脏标记，持久化由后续统一刷盘完成。
    p.AddExp(exp)
    p.MarkDirty()
}
```

`demoentity` 对应导入 `tuserv/example/demo/entity`。真实服务还会发布升级事件，见 [PlayerService.AddExp](../example/demo/service/player.go)；不要用这个简化片段替代完整玩法流程。

自定义实体应嵌入 `entity.State`，并用 `bson:"-"` 排除状态字段；持久化快照使用独立文档结构，map、slice 必须复制底层内容。`Capture(copy func()) uint64` 不是 `Entity` 接口成员，但嵌入 `State` 即可使用。参考 Player 的 `Persist`：复制背包 map 后返回文档，不能浅拷贝包含 mutex 的整个实体。

编号使用 `WithServer(serverID int32, seq int64) (int64, error)`，规则为 `serverID * IDSpan + seq`，`IDSpan = 100000`：

```go
seq, err := st.NextID(ctx, serverID, "player")
if err != nil {
    return err
}
playerID, err := entity.WithServer(serverID, seq)
if err != nil {
    return err
}
_ = playerID
```

片段中 `st` 是 `store.Store`；编号由框架/服务从计数器获取，不由客户端传入。`WithServer(3, 1)` 得到 `300001`。

## 并发、错误与边界

- `State` 方法通过内部 `sync.Mutex` 保护脏状态，但不会让任意业务字段自动线程安全；业务仍由所属 actor 串行修改。
- 当前 Demo 的 `AddExp`、`BagAdd`、`AddMember` 使用 `State.Lock/Unlock`，是为与独立刷盘 goroutine 的 `Capture` 快照协调，不是代替 actor。部分字段直接赋值并未统一走该锁，不能宣称现有实体已全面消除快照竞态。
- 不要持有 `State.Lock()` 再调用 `MarkDirty`、`Dirty`、`BeginSave`、`Capture` 或 `EndSave`；这些方法都再次获取同一把非重入锁。`Capture` 的回调也不能调用这些锁方法。
- 修改字段与后续 `MarkDirty` 不是本包提供的原子事务。代数机制防止已记录的新修改被旧保存清脏，但不等于跨字段事务或数据库并发写入版本控制。
- `State` 使用后不要复制；快照应复制业务字段而非 mutex。代数为 `uint64`，没有溢出检测。
- `WithServer` 要求 `serverID > 0` 且 `1 <= seq < 100000`，否则返回 `0, ErrIDRange`。每个区服每种计数器最多可编码 99999 个正序号；计数器本身不受这里约束，也不会因编码失败回退。
- 本包不生成 ObjectID、不分配计数器、不自动调用 `MarkDirty`，也不检查路由归属或缓存键唯一性。

## 相关测试

- [id_test.go](../pkg/entity/id_test.go)：`TestWithServer` 验证 `3,1 -> 300001`，以及序号等于 `IDSpan` 时返回 `ErrIDRange`。
- [缓存测试](../pkg/cache/cache_test.go)：通过嵌入 `State` 和 `Capture` 验证一次成功刷盘后清脏。
- [服务测试](../example/demo/service/guild_test.go) 的 `TestLoginUsesObjectIDAndIncrement` 验证 ObjectID 与带区服业务编号；`TestGuildRemoteCreateJoinOnlyOwnerCaches` 检查主人实体的路由键、脏标记与返回快照隔离。这里描述测试内容，不声明运行通过。

没有专门覆盖 `State` 并发修改、代数竞态、锁重入或全部编号非法输入的单测；本文未运行测试。
