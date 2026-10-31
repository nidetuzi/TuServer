# Config：配置加载与进程参数

## 架构与职责

[`pkg/config/config.go`](../pkg/config/config.go) 使用 `gopkg.in/yaml.v3` 将 YAML 映射为 `config.File`，供三个进程入口使用。它只负责读取和转换，不建立连接、不监听端口，也不做热更新。配置不应由业务 handler 单独读取。

- `Load(path) (File, error)`：读取指定文件并解析；文件不存在、无权限或 YAML 无法解析时返回错误。
- `File.LogConfig()`：将日志字段转换为 `pkg/log.Config`，默认值由日志模块补齐。
- `File.FlushEvery()`：解析刷盘间隔；空值、非法值、零或负数都返回 **1 秒**。

## 字段使用

进程身份与网络：

- `service_name`：Kitex 服务名。仓库示例分别为 `Gate`、`Logic`、`CrossNode`，发现和注册必须使用匹配的名字。
- `weight`：节点注册权重；Logic 新会话选址使用它，不代表区服归属，也不参与共享实体主人的哈希选择。
- `listen_addr`：本机 Kitex 监听地址，例如 `:9101`。
- `advertise_addr`：其他节点实际可访问的 Kitex 地址，例如 `127.0.0.1:9101`；跨机器部署不能使用回环地址。
- `tcp_addr`、`ws_addr`：仅 Gate 读取，留空禁用对应入口。WS 路径固定 `/ws`，不是配置字段。
- `etcd.endpoints`：etcd 地址列表，用于节点发现和注册。

持久化：

- `mongo.uri`：Logic 使用的 Mongo URI。
- `mongo.db_prefix`：按 `前缀 + serverID` 选库，例如 `game_3`。
- `flush_interval`：Logic 定时调用缓存 Flush 的间隔，遵循 Go duration 格式，例如 `1s`、`500ms`。
- `cache.max_items`：内存中不同实体的上限，超过后按最久未访问淘汰。`0` 或负数表示不按容量淘汰。
- `cache.idle`：距上次访问超过该时长就可以淘汰，例如 `10m`。空字符串、非法值或负数表示不按空闲淘汰。

日志：`log.level`、`format`、`dir`、`name`、`max_size_mb`、`max_age_days`、`also_stdout`，详细默认值见 [日志文档](log.md)。

## 使用方法

三个进程均将**第一个命令行参数**作为配置路径，不传则分别使用 `config/gate.yaml`、`config/logic.yaml`、`config/cross.yaml`。这是位置参数，不是 `--config` 参数解析。

```powershell
go run ./cmd/logic config/logic.yaml
```

可从 [`config/logic.yaml`](../config/logic.yaml) 复制多节点配置，再修改监听、对外地址及权重。Gate 和 Cross 的配置分别见 [`gate.yaml`](../config/gate.yaml)、[`cross.yaml`](../config/cross.yaml)。所有相对路径以**进程工作目录**为基准，不以 YAML 所在目录为基准。

在自定义入口中，先加载配置，再初始化日志和依赖：

```go
cfg, err := config.Load("config/logic.yaml")
if err != nil {
    return err
}
if err := log.Init(cfg.LogConfig()); err != nil {
    return err
}
log.BindKitex()
interval := cfg.FlushEvery()
_ = interval
```

片段需在返回 `error` 的函数中使用，导入 `tuserv/pkg/config` 与 `tuserv/pkg/log`。

## 约束与失败边界

- 当前使用 `yaml.Unmarshal`，未启用严格未知字段校验；拼错字段可能被忽略。
- `Load` 不验证必填项、服务名、地址可达性、Mongo 可用性或权重合理性。解析成功不代表能启动或完成请求。
- 没有环境变量插值、配置合并、命令行字段覆盖和动态重载。修改 YAML 后需重启进程。
- 错误的 `flush_interval` 静默退回 1 秒，不会导致启动失败，也不会禁用刷盘。
- 库名和刷盘属于 Logic，客户端监听属于 Gate；共享配置结构不意味着所有进程使用所有字段。

## 测试与关联文档

当前 `pkg/config` 没有独立测试文件。建议扩展时覆盖非法 YAML、未知字段策略、duration 回退和工作目录行为。本文仅说明现有实现，不表示这些检查已自动完成。

相关：[进程启动](processes.md)、[集群](cluster.md)、[缓存](cache.md)、[日志](log.md)。
