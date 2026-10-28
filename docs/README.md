# TuServer 架构与使用文档

这里按模块维护架构和使用说明。每篇同时说明职责、源码入口、实际执行流程、API/配置用法、失败边界和测试依据；文档描述当前实现，不把设计目标当作已经具备的保证。

## 建议阅读顺序

1. [进程启动与运维](processes.md)：依赖、启动命令、端口、多节点部署、退出与生成代码。
2. [Gate](gate.md) → [Logic](logic.md) → [Router / Handler](router-handler.md)：一条请求从连接到玩法的完整路径。
3. [Demo 玩法](demo.md)：登录、升级奖励、背包、聊天、公会及新增玩法步骤。
4. [Actor](actor.md)、[实体](entity.md)、[缓存](cache.md)、[存储](store.md)：执行和数据生命周期。
5. [主人运行时](owner.md)：共享实体的唯一路由、键邮箱、类型化操作与节点转发。

## 模块索引

### 连接与集群

- [Protocol](protocol.md)：`pkg/protocol`、`tcp`、`ws`；帧格式、编解码、TCP 缓冲与 WebSocket 接入。
- [Gate](gate.md)：`pkg/gate`；连接会话、Logic 地址绑定、请求转发、回包和推送。
- [Cluster](cluster.md)：`pkg/cluster`；etcd 注册发现、加权选址、主人哈希与 Kitex RPC。
- [Logic](logic.md)：`pkg/logic`；请求调度、身份回传、在线会话、广播和进程装配。
- [Owner](owner.md)：`pkg/owner`；共享实体本地/远端统一执行、DTO 隔离、错误码与重入约束。

### 执行与数据

- [Actor](actor.md)：`pkg/actor`；串行邮箱、同步等待、上下文取消及停止语义。
- [Router / Handler](router-handler.md)：`pkg/router` 与 `pkg/handler`；泛型路由、protobuf 编解码、请求上下文和角色读取。
- [Event](event.md)：`pkg/event`；按类型匹配的同步进程内事件。
- [Entity](entity.md)：`pkg/entity`；ObjectID、业务编号、放置声明、脏代数和快照契约。
- [Cache](cache.md)：`pkg/cache`；按区服缓存、别名、Get 未命中加载与 Flush。
- [Store](store.md)：`pkg/store`、`memory`、`mongo`；存储接口、分库、编号分配和测试替身。

### 配置、日志与业务

- [Config](config.md)：`pkg/config`；YAML 字段、位置参数、刷盘间隔及配置边界。
- [Log](log.md)：`pkg/log`；结构化日志、滚动清理和 Kitex 适配。
- [Demo](demo.md)：`example/demo`；handler/service/entity 分层、六个请求协议和聊天推送。
- [Processes](processes.md)：`cmd/gate`、`cmd/logic`、`cmd/cross`；启动、部署、停机及生成流程。

## 核心约定

- **执行围绕 actor**：同一会话的 handler、service 和进程内事件串行执行。共享实体操作显式进入主人运行时的实体键邮箱，不用角色互斥锁代替调度。
- **数据围绕实体**：业务通过 Get 取得实体，修改内存后 MarkDirty；加载和批量刷盘由框架完成。protobuf 只在网络边界使用，不作为存档。
- **连接围绕会话**：新会话按权重选择一次 Logic 并钉住地址，推送按会话记录的 Gate 地址返回。
- **共享实体围绕主人**：公会等实体通过显式主人操作修改，不在各节点各存一份可写副本；actor 内禁止同步 RPC 回自己。
- **区服是数据和广播范围**：不是节点归属，一台 Logic 可同时服务多个区服。
- **协议边界固定**：客户端使用自研帧和 protobuf，节点之间使用 Kitex；不要将玩法协议号变成独立 Kitex 方法。

## 阅读边界

各文档的“约束”“错误”“限制”和“测试”章节区分已实现行为与待补保证。尤其注意：取消不代表任务撤销；客户端没有统一业务错误回包；缓存刷盘不构成跨实体事务；当前拓扑变化与进程退出不提供完整无损保证。

本文档整理不改变框架行为。运行与测试总入口也可参阅[仓库 README](../README.md)。
