package demo

import (
	"context"
	"time"

	"tuserv/example/demo/entity"
	"tuserv/example/demo/service"
	core "tuserv/pkg/entity"
	"tuserv/pkg/logic"
	routepb "tuserv/pkg/router/protobuf"
)

// Module 把演示玩法挂到逻辑宿主上。
type Module struct{}

// Wire 创建服务、注册路由与主人操作，并装配玩家生命周期钩子。
func (Module) Wire(h *logic.Host) error {
	if h == nil || h.App == nil || h.Router == nil || h.Cache == nil || h.Store == nil || h.Owners == nil {
		return errModuleHost
	}
	codec := h.BodyCodec
	if codec == nil {
		codec = routepb.Body()
	}
	now := h.Now
	if now == nil {
		now = time.Now
	}
	players := service.NewPlayerService(h.Cache, h.Store, h.Bus)
	if h.DayLocation != nil {
		players = players.WithDayClock(h.DayLocation, now)
	}
	chat := service.NewChatService(h.Broadcast, pushCodec{})
	guilds := service.NewGuildService(h.Cache, h.Store, h.Owners)
	if err := guilds.RegisterOwners(); err != nil {
		return err
	}
	h.App.Owners = h.Owners
	h.App.KeyLoad = players.GetByKey
	logic.BindLocalCache(h, "player", func(ctx context.Context, serverID int32, key string, term uint64) {
		v, err := players.GetByKey(ctx, serverID, key)
		if err != nil {
			return
		}
		if p, ok := v.(*entity.Player); ok {
			p.SetFence(term)
		}
	})
	logic.BindPlayerStale(h, func(e core.Entity) (int32, int64, bool) {
		p, ok := e.(*entity.Player)
		if !ok {
			return 0, 0, false
		}
		return p.ServerID(), p.Snapshot().PlayerID, true
	})
	BindPlayerLifecycle(h.App, players)
	Register(h.Router, Deps{
		Players: players,
		Chat:    chat,
		Guilds:  guilds,
		Bus:     h.Bus,
		Codec:   codec,
	})
	return nil
}

type hostError string

func (e hostError) Error() string { return string(e) }

const errModuleHost hostError = "demo: logic host is incomplete"
