package logic

import (
	"context"
	"strconv"

	"tuserv/pkg/entity"
)

// BindLocalCache 为会话本地集合装配 Flush/Pin/Touch 钩子，避免 Module 手写样板。
// touch 由玩法注入（通常 GetByKey 后 SetFence），logic 不依赖具体玩家类型。
func BindLocalCache(h *Host, collection string, touch func(ctx context.Context, serverID int32, key string, term uint64)) {
	if h == nil || h.App == nil || h.Cache == nil || collection == "" {
		return
	}
	prev := h.App.Hooks
	h.App.Hooks.Self = prev.Self
	h.App.Hooks.Leases = prev.Leases
	h.App.Hooks.AskRelease = prev.AskRelease
	h.App.Hooks.Kick = prev.Kick
	h.App.Hooks.Guard = prev.Guard
	if h.App.Hooks.Guard == nil {
		h.App.Hooks.Guard = h.Cache.Guard
	}
	h.App.Hooks.TouchKey = touch
	h.App.Hooks.FlushKey = func(ctx context.Context, serverID int32, key string) error {
		e, err := h.Cache.Get(ctx, serverID, collection, key, nil)
		if err != nil {
			return nil
		}
		if err := h.Cache.FlushEntity(ctx, e); err != nil {
			// 保存失败时留着脏内存，避免交出租约后对端加载到旧档。
			return err
		}
		h.Cache.Forget(e)
		return nil
	}
	h.App.Hooks.FlushID = func(ctx context.Context, serverID int32, coll string, id int64) error {
		e, err := h.Cache.Get(ctx, serverID, coll, strconv.FormatInt(id, 10), nil)
		if err != nil {
			return nil
		}
		if err := h.Cache.FlushEntity(ctx, e); err != nil {
			return err
		}
		h.Cache.Forget(e)
		return nil
	}
	h.App.Hooks.PinKey = func(serverID int32, key string) func() {
		return h.Cache.PinKey(serverID, collection, key)
	}
}

// BindPlayerStale 在条件写失败后按玩法解析出的区服与角色编号准备陈旧通知。
func BindPlayerStale(h *Host, resolve func(e entity.Entity) (serverID int32, playerID int64, ok bool)) {
	if h == nil || h.App == nil || h.Cache == nil || resolve == nil {
		return
	}
	app := h.App
	h.Cache.SetStaleHandler(func(e entity.Entity, _, fence uint64) func() {
		serverID, playerID, ok := resolve(e)
		if !ok || app == nil {
			return nil
		}
		return app.PrepareStalePlayer(serverID, playerID, fence)
	})
}
