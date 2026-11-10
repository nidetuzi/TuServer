package demo

import (
	"context"
	"strconv"
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
	chat := service.NewChatService(h.Broadcast)
	guilds := service.NewGuildService(h.Cache, h.Store, h.Owners)
	if err := guilds.RegisterOwners(); err != nil {
		return err
	}
	h.App.Owners = h.Owners
	h.App.KeyLoad = players.GetByKey
	wirePlayerHooks(h, players)
	BindPlayerLifecycle(h.App, players)
	h.Cache.SetStaleHandler(func(e core.Entity, _, fence uint64) func() {
		p, ok := e.(*entity.Player)
		if !ok || h.App == nil {
			return nil
		}
		return h.App.PrepareStalePlayer(p.ServerID(), p.Snapshot().PlayerID, fence)
	})
	Register(h.Router, Deps{
		Players: players,
		Bags:    service.NewBagService(),
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

func wirePlayerHooks(h *logic.Host, players *service.PlayerService) {
	prev := h.App.Hooks
	h.App.Hooks.Self = prev.Self
	h.App.Hooks.Leases = prev.Leases
	h.App.Hooks.AskRelease = prev.AskRelease
	h.App.Hooks.Kick = prev.Kick
	h.App.Hooks.Guard = prev.Guard
	if h.App.Hooks.Guard == nil {
		h.App.Hooks.Guard = h.Cache.Guard
	}
	h.App.Hooks.TouchKey = func(ctx context.Context, serverID int32, key string, term uint64) {
		v, err := players.GetByKey(ctx, serverID, key)
		if err != nil {
			return
		}
		if p, ok := v.(*entity.Player); ok {
			p.SetFence(term)
		}
	}
	h.App.Hooks.FlushKey = func(ctx context.Context, serverID int32, key string) error {
		e, err := h.Cache.Get(ctx, serverID, "player", key, nil)
		if err != nil {
			return nil
		}
		err = h.Cache.FlushEntity(ctx, e)
		h.Cache.Forget(e)
		return err
	}
	h.App.Hooks.FlushID = func(ctx context.Context, serverID int32, collection string, id int64) error {
		e, err := h.Cache.Get(ctx, serverID, collection, strconv.FormatInt(id, 10), nil)
		if err != nil {
			return nil
		}
		err = h.Cache.FlushEntity(ctx, e)
		h.Cache.Forget(e)
		return err
	}
	h.App.Hooks.PinKey = func(serverID int32, key string) func() {
		return h.Cache.PinKey(serverID, "player", key)
	}
}
