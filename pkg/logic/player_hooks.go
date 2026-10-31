package logic

import (
	"context"

	"tuserv/pkg/handler"
	"tuserv/pkg/lease"
)

// playerKey 只从匹配的服务端会话恢复实体主键，不能采纳网关请求夹带的键。
func (a *App) playerKey(req Request) string {
	if b, ok := a.boundSession(req); ok {
		return b.PlayerKey
	}
	return ""
}

func (a *App) touchPlayer(ctx context.Context, req Request, term uint64) {
	if a.Hooks.TouchKey != nil {
		key := req.PlayerKey
		if key == "" {
			key = a.playerKey(req)
		}
		if key != "" {
			a.Hooks.TouchKey(ctx, req.ServerID, key, term)
		}
		return
	}
	if a.Hooks.Touch != nil {
		a.Hooks.Touch(ctx, req.ServerID, req.PlayerID, term)
	}
}

func (a *App) flushPlayer(ctx context.Context, s Session) error {
	if a.Hooks.FlushKey != nil {
		if s.PlayerKey == "" {
			return nil
		}
		return a.Hooks.FlushKey(ctx, s.ServerID, s.PlayerKey)
	}
	if a.Hooks.FlushID != nil {
		return a.Hooks.FlushID(ctx, s.ServerID, KindPlayer, s.PlayerID)
	}
	return nil
}

func (a *App) playerContext(ctx context.Context, req Request) *handler.Context {
	key := req.PlayerKey
	if key == "" {
		key = a.playerKey(req)
	}
	return handler.New(ctx, handler.Meta{ServerID: req.ServerID, PlayerID: req.PlayerID, PlayerKey: key, SessionID: req.SessionID, GateID: req.GateID}).WithLoader(a.Load).WithKeyLoader(a.KeyLoad)
}

// completeOnline 仅在角色邮箱和缓存保护内调用，保证登录检查与在线任务使用同一写入边界。
func (a *App) completeOnline(ctx context.Context, req Request, term uint64) error {
	a.mu.Lock()
	b, ok := a.bound[playerActorKey(req.ServerID, req.PlayerID)]
	changed := !ok || b.ID != req.SessionID || b.GateAddr != req.GateID || b.Term != term
	a.mu.Unlock()
	if changed && a.Hooks.OnPlayerOnline != nil {
		if err := a.Hooks.OnPlayerOnline(a.playerContext(ctx, req)); err != nil {
			return err
		}
	}
	a.bindOnline(ctx, req, req.PlayerID, term)
	return nil
}

func (a *App) beforePlayerOffline(ctx context.Context, req Request) error {
	if a.Hooks.OnPlayerOffline == nil {
		return nil
	}
	// 仅续原会话租约，不允许离线通知重新申领已经交出去的写权。
	term, err := a.enter(ctx, req)
	if err != nil {
		return err
	}
	return a.Hooks.OnPlayerOffline(a.playerContext(lease.WithTerm(ctx, term), req))
}
