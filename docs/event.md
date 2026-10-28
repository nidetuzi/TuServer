# event：同步、按动态类型匹配的进程内事件

## 职责与源码入口

- [总线实现](../pkg/event/bus.go)、[包说明](../pkg/event/doc.go)、[单测](../pkg/event/bus_test.go)。
- [Demo 事件与监听者](../example/demo/event/levelup.go)、[注册装配](../example/demo/register.go)、[发布入口](../example/demo/service/player.go)。
- 配套：[actor](actor.md)、[实体](entity.md)、[缓存](cache.md)。

本包是进程内同步事件总线，不是跨节点通知、异步任务队列或持久化消息系统。目标是让 service 发布事件后，监听者在当前玩家 actor 内立即修改同一份实体，随后才继续完成请求。

## 实际架构与执行流程

Bus 内部用 `map[reflect.Type][]func(context.Context, any)` 保存监听者，并用 `sync.RWMutex` 保护注册表。没有配置参数、工作 goroutine、队列容量或超时默认值。

1. `New()` 初始化空注册表。
2. 泛型函数 `On[T](bus, handler)` 用 T 的精确反射类型作为键，锁内追加监听者。
3. `Publish(ctx, ev)` 对无类型 nil 直接返回，否则取 `reflect.TypeOf(ev)`。
4. 读锁内复制该类型的监听者切片，然后释放锁。
5. 在发布者当前 goroutine 按复制出来的顺序依次调用；全部正常返回后 Publish 才返回。

复制切片意味着本次发布开始遍历后新注册的监听者不会加入这一轮；监听者执行时不持有总线锁，可以注册新监听者或再次发布。精确匹配意味着 `T`、`*T` 是两个不同事件，注册某个接口类型也不会自动收到实现该接口的具体类型事件。

## 主要 API 与使用示例

- `New() *Bus`：构造总线；不要用零值 Bus 注册监听者，内部 map 尚未初始化。
- `On[T any](b *Bus, h func(context.Context, T))`：包级泛型函数，不是 Bus 方法；不返回订阅令牌。
- `(*Bus).Publish(ctx context.Context, ev any)`：无返回值，不包含监听者错误聚合或重试。

使用现有 Demo 类型的示例：

```go
func reward(ctx context.Context, p *demoentity.Player) {
    bus := event.New()
    event.On(bus, demoevent.OnLevelUpReward)
    // 监听者同步修改这份角色，Publish 返回前奖励已经写入内存。
    bus.Publish(ctx, demoevent.PlayerLevelUp{Player: p, Level: 2})
}
```

导入 `context`、`tuserv/pkg/event`，`demoentity` 为 `tuserv/example/demo/entity`，`demoevent` 为 `tuserv/example/demo/event`。该片段演示同步通知，不会自动把角色升到 2 级；真实逻辑由 PlayerService.AddExp 修改经验、遍历实际升级等级后逐次发布。

实际装配应复用节点级 Bus，并在启动阶段注册一次；不能每个请求重复注册。`OnLevelUpReward` 遇到 `Player == nil` 返回，否则调用 `BagAdd(9001, 1)` 和 `MarkDirty()`。奖励会在后续统一 Flush 时落库，而不是 Publish 时直接保存。

## 并发、错误与边界

- **同步不等于总线全局串行**：两个 goroutine 可以同时 Publish，监听者也会并发执行。总线锁只保护订阅表，不能保护角色字段；必须让相关业务发布运行在所属 actor 内。
- 注册顺序决定同类型事件的调用顺序；并发注册的实际锁获取顺序不保证与外部调用开始时间一致。重复注册会重复调用，无去重、注销、订阅优先级、单次订阅或过滤器。
- 无监听者时直接返回，没有“未处理”错误。Publish 不检查 `ctx.Done()`，不替换 nil context，只把原 ctx 透传；监听者自行决定是否响应取消。
- 监听者 panic **不会被总线恢复**，会中断当前发布，后续监听者不再运行。在 actor 回调内发布时，外层 actor 会捕获 panic 并记录日志，但不会回滚已完成的修改，也不会自动把 panic 转成业务错误。
- 注册 nil handler 没有校验，实际调用时会 panic；nil Bus 使用不受支持。零值 Bus 的 Publish 在 map 未初始化时无订阅可执行，但 On 会因向 nil map 写入而 panic。
- 有类型 nil 指针不是 nil interface：例如 `(*SomeEvent)(nil)` 仍按指针类型发布，指针监听者可能收到 nil，需自行防御。
- 嵌套 Publish 立即递归执行，不是排到下一轮；相同类型互相发布可能无限递归。同步耗时监听者会阻塞整个当前 actor，不能在里面同步 RPC 回自己。
- 事件值不会深拷贝。含指针、map、slice 的事件可以让后续监听者看到前序监听者对共享内容的修改；没有不可变快照或事务隔离。
- 进程退出或崩溃后事件没有重放保证；本包没有持久化、跨节点投递、背压、确认、失败重试或恰好一次语义。

## 相关测试

- [TestPublishRunsListener](../pkg/event/bus_test.go)：注册值类型 `levelUp`，发布 `Level:3`，在 Publish 返回后直接断言监听者已写入结果；覆盖基本同步调用。
- [TestDemoFlows](../example/demo/demo_test.go) 在 Logic 邮箱内调用升级协议，随后检查背包奖励数量，覆盖同步事件在实际玩法流程中的使用；测试已显式装配本地主人运行时。这里描述用例内容，不声明运行通过。

当前没有专门覆盖多监听者顺序、值/指针错配、并发注册/发布、嵌套发布、上下文取消或 panic 传播的测试。本文未运行测试。
