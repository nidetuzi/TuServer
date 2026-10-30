# cache：实体内存表与延迟刷盘

## 职责与源码入口

- [实现](../pkg/cache/cache.go)、[包说明](../pkg/cache/doc.go)、[单测](../pkg/cache/cache_test.go)。
- 上游是 service，下游是 [Store](store.md)；缓存只操作 [Entity](entity.md) 接口，不认识具体玩法。
- [Logic 进程装配](../cmd/logic/main.go)、[刷盘配置](../pkg/config/config.go)、[角色服务](../example/demo/service/player.go)。

业务读取应走 `Get`，未命中时由缓存内部调用存储 `Load`；业务修改实体并 `MarkDirty`，由节点定时 Flush，而不是每次请求直接写库。

## 实际架构与执行流程

缓存持有一个互斥锁、`map[string]entity.Entity` 和一个 `store.Store`。键格式是 `serverID|collection|key`，一个实体可以有主键和多个别名，值是同一个对象引用。

### Get

1. 锁内查内存，命中直接返回原指针。
2. 未命中且存储或 factory 为 nil，返回 `store.ErrNotFound`。
3. 锁外调用 factory，并用调用方传入的区服、集合、键执行 `Store.Load`；任何错误都直接返回，不缓存失败对象，也不自动创建实体。
4. 加锁再次检查请求键。若并发请求已写入，返回先写入者；否则同时登记请求键与实体自身的规范键。

这不是 singleflight：并发冷读可能重复 factory 和 Load，但针对同一个请求键，最终返回已登记的对象。不验证加载结果的区服、集合、主键是否和请求一致；也不统一处理不同别名并发冷读的对象身份。

### Put、Alias 与 Flush

- `Put` 在锁内先调用 `MarkDirty`，再以实体规范键覆盖登记；不会立即写库。
- `Alias` 只添加/覆盖同区服同集合下的另一个键，不自动放入规范键，也不打脏标记。
- `Flush` 锁内枚举当前脏对象，按 `map[entity.Entity]struct{}` 去重，然后解锁逐个保存。别名不会让同一对象重复写入。
- 实现了 `Persist() (uint64, any)` 的对象优先提供快照；否则使用 `BeginSave()` 和对象本身。保存键始终来自实体的 `ServerID/Collection/CacheKey`，不是别名。
- 每次 Save 成功后 `EndSave(gen)`；中间又被 `MarkDirty` 的对象继续保持脏状态。

包本身没有定时器。`cmd/logic/main.go` 用 ticker 调用 Flush，配置 `flush_interval` 为空、解析失败或非正数时回退 **1 秒**；[默认配置](../config/logic.yaml) 也是 `1s`。退出路径另 Flush 一次。当前退出顺序是先最终 Flush、后停止 actor/RPC，且 ticker 未显式停止，因此不是已完成协调的停写排空流程。

## 主要 API 与使用示例

- `New(st store.Store) *Cache`：初始化内存表；传 nil 可做纯内存缓存。
- `Get(ctx, serverID, collection, key, factory) (entity.Entity, error)`：factory 类型为 `func() entity.Entity`。
- `Put(e entity.Entity)`、`Alias(e entity.Entity, key string)`：无返回错误，调用方负责实体有效性和唯一性。
- `Flush(ctx context.Context) error`：由运行时调用，不应当作业务提交事务。

使用真实 Demo 实体的初始化与读取示例：

```go
func example(ctx context.Context) error {
    st := memory.New()
    c := cache.New(st)
    p := demoentity.NewPlayer(1, "ann")
    c.Put(p)
    c.Alias(p, "name:ann")
    if _, err := c.Get(ctx, 1, "player", "name:ann", nil); err != nil {
        return err
    }
    // 示例模拟节点刷盘，实际业务不应每次请求自行刷盘。
    if err := c.Flush(ctx); err != nil {
        return err
    }
    cold := cache.New(st)
    _, err := cold.Get(ctx, 1, "player", p.CacheKey(), func() core.Entity {
        return &demoentity.Player{}
    })
    return err
}
```

导入 `context`、`tuserv/pkg/cache`、`tuserv/pkg/store/memory`，`core` 为 `tuserv/pkg/entity`，`demoentity` 为 `tuserv/example/demo/entity`。该示例仅演示缓存；真实角色的 `PlayerID` 仍应通过服务分配，不能以该片段替代登录创建流程。

## 并发、错误与边界

- 缓存锁保护映射，不保护返回实体的业务字段；业务操作仍须进所属 [actor](actor.md)，刷盘使用独立快照协调。
- Save 第一次出错就返回；此前成功对象已经清脏，失败对象和后续对象保留脏标记。枚举 map 的顺序不稳定，所以没有固定的失败前缀，也没有全量事务、内部重试或回滚。
- 存储为 nil 时，Flush 返回 nil，但不保存也不清脏；不能把这个 nil 理解为已经持久化。
- Get 内存命中不检查上下文取消；Load/Save 的取消行为取决于存储实现。Flush 在开始前和每个实体前检查 `ctx.Err()`。
- 同一缓存的 Flush / FlushEntity 共用一把刷盘锁。带 `DocVersion` 的快照会在代数或任期落后时返回 `ErrStale`，缓存丢掉这份内存，不再覆盖库里更新的文档。其他保存错误仍会中断本轮剩余实体。
- 没有 TTL、容量上限或按时间淘汰。`Forget` 只在交出租约时摘掉内存副本。Put/Alias 可覆盖已有键，不报告冲突。别名只在当前内存生效，不自动按 BSON 字段查询。
- 去重使用实体作 map 键，实际实体应使用可比较的指针类型；不可比较的动态值会 panic。nil 实体、返回 nil 的 factory 等也没有防御校验。
- 键未转义 `|`，集合名/键含分隔符可能产生组合碰撞，调用方应限制命名。
- `Placement`/`RouteKey` 不参与 Get/Put/Flush；非主人节点不能指望缓存自动转发。缺少 `Persist` 时传给 Save 的还是活实体，存在并发序列化风险。

## 相关测试

[TestGetLoadsOnceAndFlushClearsDirty](../pkg/cache/cache_test.go) 验证顺序两次 Get 返回同一对象、`memory.Loads()==1`、修改并 MarkDirty 后 Flush 落库且清脏。测试记录实现 `Persist` 并通过 Capture 生成快照。

当前没有覆盖并发冷读、别名冲突、保存失败、刷盘期间新修改、nil 存储、并发 Flush 或路由归属的专门用例。本文未运行测试；也不据单次顺序读测试推断并发只 Load 一次。
