package logic

import (
	"context"

	"tuserv/pkg/handler"
	"tuserv/pkg/lease"
)

func (a *App) playerContext(ctx context.Context, req Request) *handler.Context {
	return handler.New(ctx, handler.Meta{ServerID: req.ServerID, PlayerID: req.PlayerID, SessionID: req.SessionID, GateID: req.GateID}).WithLoader(a.Load)
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
