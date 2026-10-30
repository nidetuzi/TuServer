# actor：进程内串行邮箱

## 职责与源码入口

`pkg/actor` 按字符串编号维护邮箱，让同一编号的任务串行执行；它不负责会话绑定、实体路由或分布式 actor。

- [实现](../pkg/actor/actor.go)、[包说明](../pkg/actor/doc.go)。
- [Logic 调用入口](../pkg/logic/app.go)：普通协议以 `session/<SessionID>` 为邮箱键；空会话编号使用固定键 `session/player`。cmd 0 在会话邮箱外交给 [owner 运行时](owner.md)，再进入 `owner/...` 实体邮箱；两种邮箱共享同一个 actor System，但键命名空间隔离。断线通知只删除会话表记录，不删除邮箱。
- 相关：[实体](entity.md)、[事件](event.md)、[缓存](cache.md)。

## 实际架构与执行流程

1. `NewSystem()` 建立空映射；第一次 `Do` 某个编号时，在全局互斥锁内创建 `Actor`。
2. 每个 actor 有一个容量固定为 **64** 的 `chan func()` 和一个常驻 goroutine；容量不含正在执行的任务，没有配置项。
3. `Do` 将包装后的任务送入邮箱，然后等待任务结束或上下文取消。队列满时入队也会等待。
4. 邮箱 goroutine 顺序调用任务；不同编号可以并行，同编号的并发提交以实际进入 channel 的顺序执行，不保证调用开始时间的先后。
5. 包装层捕获任务 panic，使用 `tuserv/pkg/log` 记录 `actor panic`，并关闭完成信号；邮箱继续处理后续任务。

邮箱表只增不减，没有空闲回收、单个 actor 停止、队列长度查询或持久化。`Actor` 虽导出，但没有对外构造函数和调度方法，正常入口是 `System`。

## 主要 API 与示例

- `NewSystem() *System`：必须使用构造函数初始化映射。
- `(*System).Do(ctx context.Context, id string, fn func()) error`：提交并同步等待。`nil` 上下文被替换成 `context.Background()`；空字符串编号也可用。
- `(*System).Stop()`：幂等关闭全部邮箱，不等待 goroutine 退出。

下面是调用片段；业务错误需要单独接收，因为 `Do` 的返回值只表示调度/等待结果。

```go
func apply(ctx context.Context, actors *actor.System) error {
    var businessErr error
    err := actors.Do(ctx, "session:s1", func() {
        // 业务修改与同步事件应在这次邮箱任务内完成。
        businessErr = changePlayer()
    })
    if err != nil {
        return err
    }
    return businessErr
}
```

片段中的 `changePlayer()` 代表调用方自己的业务函数；导入 `context` 和 `tuserv/pkg/actor`。示例假定上下文不会在任务执行中提前结束；若允许提前返回，不能继续无保护地读取任务正在写入的变量。

## 约束、失败与边界

- **不能同步重入同一邮箱**：任务内再次 `Do` 同一个编号会等待自己。跨 actor 相互同步等待也可能形成环；跨节点通知应另起协程或采用单向调用，不能同步 RPC 回自己。
- actor 串行仅约束经过该邮箱的操作。它不自动保护其他 goroutine 或定时刷盘对同一实体字段的访问；刷盘仍要靠实体自己的锁和快照代数。已登录请求在 Logic 里按角色编号进入同一个邮箱。
- **取消不撤销任务**：入队后 `Do` 可以先返回 `ctx.Err()`，任务仍可能执行或继续执行。回调不接收额外的任务上下文，只能自行检查闭包中的 `ctx`。
- 两个 `select` 都可能在取消与入队/完成同时就绪时随机选中分支，因此已取消上下文不严格保证不执行，完成与取消同时发生也不保证返回 `nil`。
- `fn == nil` 会在任务中触发 panic，被包装层记录并吞掉；普通任务 panic 也不转成返回错误，等待成功通常返回 `nil`。这不是业务回滚机制。
- 停止后新调用 `Do` 返回 `context.Canceled`，与调用方取消使用同一错误。`Stop` / `Shutdown` 会先拒绝新任务，再把已排队的任务跑完，然后返回；入队和关闭共用邮箱锁，不会向已关闭的 channel 发送。
- `Shutdown` 在上下文到期时会放弃等待，已经在跑的任务不会被取消。

## 相关测试

[`actor_test.go`](../pkg/actor/actor_test.go) 覆盖停止时等待正在执行的任务，以及停止与发送并发时不会 panic。取消后任务仍可能执行完，这一点没有单独的回滚测试。

[Demo 集成测试](../example/demo/demo_test.go) 的 `TestDemoFlows` 经 `logic.App.Handle` 间接使用邮箱，覆盖登录、升级和同步奖励等流程，并使用同一 actor System 装配本地主人运行时。它不是专门的邮箱并发测试；本文未运行测试，不声明运行结果。
