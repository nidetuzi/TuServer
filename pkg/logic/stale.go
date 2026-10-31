package logic

import (
	"context"

	"tuserv/pkg/lease"
	"tuserv/pkg/store"
)

// PrepareStalePlayer 捕获保存前的角色绑定，避免延迟通知误踢后来登录的会话。
// 返回的通知只投递任务，允许缓存持有刷盘锁时调用，也允许从角色 actor 内触发。
func (a *App) PrepareStalePlayer(serverID int32, playerID int64, term uint64) func() {
	key := playerActorKey(serverID, playerID)
	a.mu.Lock()
	b, ok := a.bound[key]
	a.mu.Unlock()
	if !ok || b.Term != term {
		return nil
	}
	return func() {
		a.mu.Lock()
		cur, still := a.bound[key]
		if still && cur.ID == b.ID && cur.Term == b.Term && cur.GateAddr == b.GateAddr {
			// 先封住后续协议，再等待异步清理，避免忘掉旧实体后又重新加载并继续修改。
			a.cancelPlayerTasksLocked(key)
			cur.invalid = true
			a.bound[key] = cur
		}
		a.mu.Unlock()
		if still && cur.ID == b.ID && cur.Term == b.Term && cur.GateAddr == b.GateAddr {
			a.queueStale(b)
		}
	}
}

func (a *App) queueStale(b binding) {
	// saveOne 已丢弃旧实体；清理不能再刷一遍，否则可能把新加载的实体一并 Forget。
	a.queueCleanup(b, store.ErrStale, true)
}

func (a *App) discardStaleInActor(ctx context.Context, b binding) {
	a.mu.Lock()
	key := playerActorKey(b.ServerID, b.PlayerID)
	cur, ok := a.bound[key]
	if !ok || cur.ID != b.ID || cur.Term != b.Term || cur.GateAddr != b.GateAddr {
		a.mu.Unlock()
		return
	}
	a.cancelPlayerTasksLocked(key)
	delete(a.bound, key)
	delete(a.sessions, b.ID)
	a.mu.Unlock()
	a.Actors.Retire("session/" + b.ID)
	if cur.unpin != nil {
		cur.unpin()
	}
	// 存档冲突不一定代表失租，按原任期条件释放，绝不改动新持有者的租约。
	if a.Hooks.Leases != nil {
		_ = lease.Release(ctx, a.Hooks.Leases, b.ServerID, playerLeaseKey(b.ServerID, b.PlayerID), a.Hooks.Self, b.Term)
	}
	a.kick(ctx, b.GateAddr, b.ID)
}
