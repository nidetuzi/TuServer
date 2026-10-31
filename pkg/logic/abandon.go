package logic

import (
	"context"
	"time"

	"tuserv/pkg/log"
)

type abandonTask struct {
	binding binding
	held    error
}

// RenewErrorCount 返回未能确认续租成功的累计条数，便于节点监控区分存储故障和失租。
func (a *App) RenewErrorCount() uint64 { return a.renewErrors.Load() }

func (a *App) queueAbandon(b binding, held error) {
	key := playerActorKey(b.ServerID, b.PlayerID)
	a.abandonMu.Lock()
	defer a.abandonMu.Unlock()
	if a.abandonPending == nil {
		a.abandonPending = make(map[string]*abandonTask)
	}
	// 同一角色只保留一份快照，慢邮箱不能让每轮续租再堆一份清理任务。
	if _, ok := a.abandonPending[key]; ok {
		return
	}
	a.abandonPending[key] = &abandonTask{binding: b, held: held}
	if a.abandonActive < DefaultReapParallel {
		a.abandonActive++
		go a.abandonWorker()
	}
}

func (a *App) abandonWorker() {
	for {
		a.abandonMu.Lock()
		var key string
		var selected *abandonTask
		for k, task := range a.abandonPending {
			if task != nil {
				key, selected = k, task
				// nil 保留正在处理的占位，直到完成前都不重复投递。
				a.abandonPending[k] = nil
				break
			}
		}
		if selected == nil {
			a.abandonActive--
			a.abandonMu.Unlock()
			return
		}
		task := *selected
		a.abandonMu.Unlock()

		// 排队不使用本轮续租的截止时间；IO 超时从邮箱真正执行时开始计算。
		err := a.Actors.Do(context.Background(), key, func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			a.withGuard(func() { a.abandonLostInActor(ctx, task.binding, task.held) })
		})
		if err != nil {
			log.Warn("abandon lease failed", "err", err, "player_id", task.binding.PlayerID, "server_id", task.binding.ServerID)
		}
		a.abandonMu.Lock()
		if a.abandonPending[key] == nil {
			delete(a.abandonPending, key)
		}
		a.abandonMu.Unlock()
	}
}
