# store：存储契约、memory 与 Mongo

## 职责与源码入口

- [Store 接口与 ErrNotFound](../pkg/store/store.go)、[包说明](../pkg/store/doc.go)。
- [memory 实现](../pkg/store/memory/memory.go)、[Mongo 实现](../pkg/store/mongo/store.go)。
- 调用链见 [缓存](cache.md)、[实体与编号](entity.md)；实际集成见 [PlayerService](../example/demo/service/player.go)。

存储层按区服、集合、主键读写 BSON 文档，并提供字段查询和计数器。包说明中的“业务包看不到它”是设计意图，不是 Go 可见性限制：接口已导出，当前 service 注入 Store，并调用 `Find`、`NextID`；实体正文加载仍通过缓存 `Get`，不要在玩法中直接 `Load`。

## 接口与执行流程

`Store` 包含四个方法：

- `Load(ctx context.Context, serverID int32, collection, key string, dest any) error`：按主键解码到 dest。
- `Save(ctx context.Context, serverID int32, collection, key string, doc any) error`：覆盖文档。
- `Find(ctx context.Context, serverID int32, collection, field string, value any, dest any) error`：按一个字段找一条文档。
- `NextID(ctx context.Context, serverID int32, name string) (int64, error)`：返回区服内、指定名字计数器的下一序号，不是已编码的业务编号。

`ErrNotFound = errors.New("store: not found")` 用于查询无结果；推荐 `errors.Is(err, store.ErrNotFound)`。Save 不操作 Entity 的脏状态，清脏由缓存成功保存后执行。Store 不提供删除、事务、批量写、索引管理、自动重试策略或实体路由。

### memory：测试用 BSON 存储

`memory.New()` 初始化文档表与计数器表。文档以 `collection|serverID|key` 定位，存储的是 BSON 字节而不是传入对象指针；Load 再反序列化。因此可以验证 BSON tag 和落库快照，不等同于 Mongo 服务端行为。

- Save 先在锁外 `bson.Marshal`，成功后锁内覆盖文档；不能靠存储锁保护调用方正在变化的对象。
- Load 在锁内增加调用计数，再查记录并解码；不存在也会计数。`Loads()` 返回所有 Load 的累计次数，Find 不计入。
- Find 锁内扫描指定区服/集合，解码成 `bson.M`，用 `probe[field] == value` 比较第一个命中；map 遍历没有稳定顺序。
- NextID 在同一把 mutex 下自增，每个 `(serverID, name)` 从 **1** 开始；仅在这个 Store 实例中唯一，不持久化、不跨进程共享。
- 所有方法忽略传入 context，包括已取消的上下文。

### Mongo：按区服分库

`mongo.Open(ctx, uri, prefix)` 使用官方驱动 `Connect`。prefix 为空时默认 **`game_`**；区服 3 对应 `game_3`，集合使用传入名字。Open 没有显式 Ping，不保证返回时已验证远端可用；连接、池和超时没有本包自定义默认值，取 URI/驱动配置与 context 的行为。

- Load：`FindOne({_id: idValue(key)})` 并 Decode。
- Save：`ReplaceOne({_id: idValue(key)}, doc, upsert=true)`，是整体替换而不是局部 `$set`。旧文档中本次 doc 缺失的字段不会自动保留。
- Find：`FindOne({field: value})`，无排序；多条匹配时不承诺哪条。Load/Find 将驱动 `ErrNoDocuments` 转成公共 `ErrNotFound`，其他错误原样返回。
- `idValue` 将合法的 **24 位十六进制 ObjectID 字符串**转换为 BSON ObjectID，转换失败才保留字符串。纯字符串主键如果恰好符合该格式，也会被转换；memory 没有这一步。
- `Close(ctx)` 调用驱动 Disconnect；它不属于 Store 接口，也不替调用方 Flush。

Mongo NextID 使用当前区服数据库的 `uuid` 集合，`_id=name`、`count` 为 int64：先执行一次不带 upsert 的 `FindOneAndUpdate($inc count:1)`，默认读更新前的 count，返回 `count+1`。**任意首次错误**都会进入 upsert 路径，而不仅是不存在：最多再调用 **3 次**带 upsert 的 FindOneAndUpdate；成功返回旧 count+1，upsert 新插入而返回 `ErrNoDocuments` 时视为序号 1。其余失败记录 last error，三次后返回；无退避、也不只针对重复键重试。最坏合计 **4 次数据库调用**。

## 使用示例

下面是存储适配层/测试调用片段，不是在业务中绕过缓存读实体：

```go
func inspectCounter(ctx context.Context) (int64, error) {
    st := memory.New()
    seq, err := st.NextID(ctx, 3, "player")
    if err != nil {
        return 0, err
    }
    // 自增序号与带区服业务编号是两层概念。
    return entity.WithServer(3, seq)
}
```

导入 `context`、`tuserv/pkg/store/memory`、`tuserv/pkg/entity`。Mongo 运行时通过 `mongo.Open(ctx, uri, "")` 创建后注入 `cache.New(st)`，退出时先协调停止修改并 Flush，再 Close；当前 [进程代码](../cmd/logic/main.go) 的停写协调尚不完整，详见缓存文档。

角色按名字/业务编号查找时，service 可以先用 Find 定位文档的 ObjectID，再 `cache.Get` 获取唯一实体，注册内存别名。不能把业务编号直接当成 Mongo `_id`，也不能把 Find 解出的临时对象直接当作缓存里的唯一可变实体。

## 错误、并发与实现差异

- BSON 编解码、Mongo 网络、权限、超时、不可变 `_id` 等错误直接上交；缓存决定失败后保留脏状态。
- memory Find 是 Go 等值比较，不支持 Mongo 的查询操作符、嵌套字段语义或类型转换；BSON 解码后的整数类型与 value 类型不一致可能不命中，缺失字段与 nil 也可能被视为相等。若比较到相同动态类型的不可比较值，可能 panic。
- memory 不校验文档 `_id` 和存储 key 是否一致；Mongo 则受主键与替换写规则约束。memory 单测不能证明 Mongo 唯一索引、网络失败、原子 upsert 行为正确。
- 两实现不验证 serverID、集合名、计数器名，也不限制序号到 99999；调用方必须用 `entity.WithServer` 校验编码范围。序号被获取后失败不会退回，不能要求连续无空洞。
- Mongo `$inc` 保证服务端单次操作的原子性，但当前 NextID 的通用错误重试不能承诺端到端恰好一次；网络结果不确定时可能耗掉序号。
- 没有自动创建名字或业务编号唯一索引；并发业务创建和幂等性需要上层处理。Store 也不提供防止旧快照覆盖新快照的版本条件。
- memory 字符串组合键没有转义分隔符 `|`，应限制集合/键/计数器命名，避免歧义。

## 相关测试

当前 `pkg/store`、`pkg/store/memory`、`pkg/store/mongo` 都没有专门测试文件，也没有 Mongo 集成测试。

- [缓存测试](../pkg/cache/cache_test.go) 间接覆盖 memory Save/Load、BSON 持久化与 Loads。
- [服务测试](../example/demo/service/guild_test.go) 的 `TestLoginUsesObjectIDAndIncrement` 覆盖 memory NextID、业务编号、Find 与重新加载；公会相关用例还使用共享 memory 存储检查主人缓存与重新加载。这里描述测试内容，不声明运行通过。

本文未运行测试；Mongo 连接、并发 upsert、真实 ObjectID 查询、错误重试与驱动超时均未因本文写作得到运行验证。
