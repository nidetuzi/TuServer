# Log：统一结构化日志与滚动文件

## 架构与职责

[`pkg/log/log.go`](../pkg/log/log.go) 提供进程级日志入口，内部使用结构化 handler；[`rotate.go`](../pkg/log/rotate.go) 管理文件轮转和过期清理；[`kitex.go`](../pkg/log/kitex.go) 将 Kitex klog 接入同一输出链路。业务只使用 `tuserv/pkg/log`，不要直接调用标准库 `log` 或 `log/slog`。

进程在监听前调用 `Init`，再调用 `BindKitex`。日志写入是同步的，文件 writer 使用互斥锁保护写入与轮转；它不是异步日志队列。未初始化时默认输出到标准输出。

## 配置与默认值

`log.Config` 对应 YAML `log` 节点：

- `Level` / `level`：默认 `info`；支持小写 `debug`、`warn`、`error`，其他值回退 info。
- `Format` / `format`：默认 `text`；只有精确的 `json` 使用 JSON，其他值使用文本。
- `Dir` / `dir`：默认 `logs`。
- `Name` / `name`：默认 `app`；三个进程示例分别使用 gate、logic、cross。
- `MaxSizeMB` / `max_size_mb`：非正数默认 100，按 MiB 换算。
- `MaxAgeDays` / `max_age_days`：非正数默认 7，不能通过 0 表达无限保留。
- `AlsoStdout` / `also_stdout`：默认 false；仓库三个 YAML 均开启。

目录创建或文件打开失败时 `Init` 返回错误，应在启动阶段处理。相对目录基于工作目录。

## 使用方法

```go
if err := log.Init(log.Config{
    Level: "info", Format: "text", Dir: "logs", Name: "logic",
    MaxSizeMB: 100, MaxAgeDays: 7, AlsoStdout: true,
}); err != nil {
    return err
}
log.BindKitex()
log.Info("logic start", "addr", "127.0.0.1:9101")
logger := log.With("server_id", int32(3), "player_id", int64(300001))
logger.Warn("request failed", "cmd_id", int32(1003), "err", err)
```

片段在返回 `error` 的函数中使用，导入 `tuserv/pkg/log`。字段使用键值对，避免拼接长字符串。`With` 返回的日志器已绑定当前 handler；重新 Init 不会让先前取得的日志器自动改用新 handler，因此不要把反复初始化当作热更新接口。

级别约定：

- `Debug`：高频诊断，默认不会输出。
- `Info`：启动、退出、连接绑定等关键状态；普通协议成功不逐包记录 Info。
- `Warn`：可恢复失败，例如一次 RPC 转发失败。
- `Error`：刷盘失败等需要处理的异常。

不要记录密码、token、完整私密协议载荷；日志不是业务流水或可靠审计存储。

## 轮转与保留

当天主文件是 `logic-2026-10-28.log`。即将写入的新日志超过大小限制时，将主文件重命名为当天下一个序号，例如 `logic-2026-10-28.1.log`，再打开新的主文件。跨天在**下一次写入**时换文件，空闲时不依赖后台定时器。

大小限制不是硬上限：单条记录大于限制时仍整条写入。文件以追加模式打开，重启不会自动清空当天文件。

每次成功写入后，按文件名里的日期清理同 Name 前缀的过期日志；不是按文件修改时间，也不是独立清理服务。清理目录读取/删除错误不会上抛。保留判断按日期零点和 `MaxAgeDays * 24h` 比较，不是精确的逐条日志寿命。

## Kitex 接入与生命周期边界

`BindKitex()` 安装统一适配器：Trace/Debug 映射 Debug，Notice 映射 Info，Fatal 映射 Error。Fatal **不会退出进程**。适配器忽略 Kitex 的 SetLevel 和 SetOutput，输出和级别仍由本模块配置控制。

Kitex 的 `Ctx...` 方法当前不从 context 自动提取追踪字段；业务要显式带上 session、区服、角色等信息。

- `Handler()`：提供当前 handler，通常由框架适配使用。
- `Sync()`：请求当前文件同步到磁盘，可返回错误；不是后台定时保证。
- `Close()`：关闭当前文件，不能把它当成全局停止所有日志调用的屏障。应先协调停止日志生产者，再同步和关闭。
- `SetNow()`：仅测试换时钟，不用于业务时间控制。

重复 Init 会关闭旧 writer；不要在业务请求中调用。日志方法不返回底层写入错误，磁盘满等问题不能依赖调用方收到错误发现。多进程使用同目录、同 Name 会竞争同一批文件，部署时应隔离名称或目录。当前进程入口没有显式调用 `Sync/Close` 的退出收尾。

## 测试

[`log_test.go`](../pkg/log/log_test.go) 的 `TestRotateByDaySizeAndAge` 验证结构化字段、大小分片、跨天切换及过期文件清理。它不覆盖磁盘满、多进程同名、并发重新 Init、所有 Kitex 映射或崩溃持久化。

```powershell
go test ./pkg/log
```

相关：[配置](config.md)、[进程启动与运维](processes.md)。
