package logic

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"tuserv/pkg/handler"
	"tuserv/pkg/lease"
	"tuserv/pkg/scheduler"
)

// ErrPlayerTaskSession 表示注册时的在线会话已失效，任务不能接管后来登录的角色。
var ErrPlayerTaskSession = errors.New("logic: scheduled player session expired")

// AddPlayerTask 绑定当前在线会话注册周期任务，允许在角色邮箱内调用但不会立即执行业务。
// fn 只能使用传入上下文读取实体，不应捕获请求上下文或实体指针。
func (a *App) AddPlayerTask(session Session, name, spec string, fn func(*handler.Context) error) (*scheduler.Task, error) {
	if a.Scheduler == nil || fn == nil || strings.TrimSpace(name) == "" {
		return nil, errors.New("logic: scheduler, name and callback required")
	}
	key := playerActorKey(session.ServerID, session.PlayerID)
	a.mu.Lock()
	defer a.mu.Unlock()
	b, ok := a.bound[key]
	if !ok || b.invalid || b.ID == "" || b.Session != session {
		return nil, ErrPlayerTaskSession
	}
	if a.playerTasks == nil {
		a.playerTasks = make(map[string]map[string]*scheduler.Task)
	}
	if a.playerTasks[key] == nil {
		a.playerTasks[key] = make(map[string]*scheduler.Task)
	}
	if prev := a.playerTasks[key][name]; prev != nil {
		select {
		case <-prev.Done():
			delete(a.playerTasks[key], name)
		default:
			return nil, fmt.Errorf("%w: %s", scheduler.ErrDuplicate, name)
		}
	}
	var task *scheduler.Task
	task, err := a.Scheduler.Add(fmt.Sprintf("player/%d/%d/%s", session.ServerID, session.PlayerID, name), spec, func(ctx context.Context) error {
		// 注册和执行通过 a.mu 发布句柄，避免 Start 已运行时读到尚未赋值的 task。
		a.mu.Lock()
		current := task
		a.mu.Unlock()
		return a.runPlayerTask(ctx, b, current, fn)
	})
	if err != nil {
		return nil, err
	}
	a.playerTasks[key][name] = task
	return task, nil
}

func (a *App) runPlayerTask(ctx context.Context, b binding, task *scheduler.Task, fn func(*handler.Context) error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var result error
	// Do 的等待上下文不能随业务超时退出，否则运行配额会在邮箱真实执行结束前被释放。
	// 排队中的任务仍会执行这个轻量包装，并在其中检查业务取消状态。
	err := a.Actors.Do(context.Background(), playerActorKey(b.ServerID, b.PlayerID), func() {
		defer func() {
			if rec := recover(); rec != nil {
				result = fmt.Errorf("player task panic: %v", rec)
			}
		}()
		if ctx.Err() != nil {
			result = ctx.Err()
			return
		}
		select {
		case <-task.Done():
			result = context.Canceled
			return
		default:
		}
		a.mu.Lock()
		cur, ok := a.bound[playerActorKey(b.ServerID, b.PlayerID)]
		valid := ok && !cur.invalid && cur.ID == b.ID && cur.Term == b.Term && cur.GateAddr == b.GateAddr
		a.mu.Unlock()
		if !valid {
			task.Cancel()
			result = ErrPlayerTaskSession
			return
		}
		a.withGuard(func() {
			req := Request{ServerID: b.ServerID, PlayerID: b.PlayerID, SessionID: b.ID, GateID: b.GateAddr}
			term, err := a.enter(ctx, req)
			if err != nil {
				result = err
				return
			}
			if ctx.Err() != nil {
				result = ctx.Err()
				return
			}
			hctx := handler.New(lease.WithTerm(ctx, term), handler.Meta{ServerID: b.ServerID, PlayerID: b.PlayerID, SessionID: b.ID, GateID: b.GateAddr}).WithLoader(a.Load)
			result = fn(hctx)
		})
	})
	if err != nil {
		return err
	}
	return result
}

// cancelPlayerTasksLocked 与绑定变更在同一把锁下执行，旧清理不能取消新会话注册的任务。
func (a *App) cancelPlayerTasksLocked(key string) {
	for _, task := range a.playerTasks[key] {
		task.Cancel()
	}
	delete(a.playerTasks, key)
}
