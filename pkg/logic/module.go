package logic

import (
	"context"
	"time"

	"tuserv/pkg/cache"
	"tuserv/pkg/event"
	"tuserv/pkg/owner"
	"tuserv/pkg/router"
	"tuserv/pkg/store"
)

// Broadcaster 是区服广播的最小依赖，避免 Host 直接依赖 cluster。
type Broadcaster interface {
	NotifyServer(ctx context.Context, serverID, cmdID int32, body []byte) error
}

// Host 是逻辑进程提供给业务模块的装配面。
// 进程入口只构造 Host 并调用 Module.Wire，避免 main 散落具体玩法类型。
type Host struct {
	App       *App
	Router    *router.Router[int32]
	Cache     *cache.Cache
	Store     store.Store
	Owners    *owner.Runtime
	Bus       *event.Bus
	BodyCodec router.BodyCodec
	// Self 是本节点对外地址，供钩子与租约使用。
	Self string
	// DayLocation 与 Now 供跨天检查与日常逻辑共用。
	DayLocation *time.Location
	Now         func() time.Time
	// Broadcast 区服广播；聊天等玩法需要时注入。
	Broadcast Broadcaster
}

// Module 把玩法服务、路由、主人操作和生命周期挂到 Host 上。
type Module interface {
	Wire(h *Host) error
}
