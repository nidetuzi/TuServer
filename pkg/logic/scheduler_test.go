package logic

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"tuserv/pkg/handler"
	"tuserv/pkg/lease"
	"tuserv/pkg/router"
	"tuserv/pkg/scheduler"
	"tuserv/pkg/store/memory"
)

func taskApp(t *testing.T) (*App, Session) {
	t.Helper()
	a := New(router.New[int32](), nil)
	s, err := scheduler.New(scheduler.Config{Timeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	a.Scheduler = s
	t.Cleanup(a.Stop)
	session := Session{ID: "online", ServerID: 1, PlayerID: 100001, GateAddr: "gate"}
	a.bindOnline(context.Background(), Request{SessionID: session.ID, GateID: session.GateAddr, ServerID: 1}, session.PlayerID, 0)
	return a, session
}

func addTestPlayerTask(t *testing.T, a *App, session Session) (*scheduler.Task, binding) {
	t.Helper()
	task, err := a.AddPlayerTask(session, "test", "@every 1s", func(*handler.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	a.mu.Lock()
	b := a.bound[playerActorKey(session.ServerID, session.PlayerID)]
	a.mu.Unlock()
	return task, b
}

func TestPlayerTaskLifecycle(t *testing.T) {
	for _, mode := range []string{"disconnect", "relogin", "lost", "stale", "release"} {
		t.Run(mode, func(t *testing.T) {
			a, session := taskApp(t)
			task, b := addTestPlayerTask(t, a, session)
			switch mode {
			case "disconnect":
				if err := a.onDisconnect(context.Background(), Request{ServerID: 1, PlayerID: session.PlayerID, SessionID: session.ID, GateID: session.GateAddr}); err != nil {
					t.Fatal(err)
				}
			case "relogin":
				a.bindOnline(context.Background(), Request{ServerID: 1, SessionID: "new", GateID: "gate"}, session.PlayerID, 0)
			case "lost":
				a.abandonLost(context.Background(), b, lease.ErrHeld)
			case "stale":
				a.discardStaleInActor(context.Background(), b)
			case "release":
				if err := a.relinquish(context.Background(), []byte(`{"kind":"player","server_id":1,"id":100001}`)); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-task.Done():
			default:
				t.Fatal("会话结束后任务未取消")
			}
			if len(a.Scheduler.Entries()) != 0 {
				t.Fatal("调度器残留旧会话任务")
			}
			var ran bool
			_ = a.runPlayerTask(context.Background(), b, task, func(*handler.Context) error { ran = true; return nil })
			if ran {
				t.Fatal("旧任务仍修改角色")
			}
		})
	}
}

func TestPlayerTaskGuardLeaseAndLoader(t *testing.T) {
	a, session := taskApp(t)
	st := memory.New()
	a.Hooks.Self, a.Hooks.Leases = "logic", st
	rec, err := lease.Claim(context.Background(), st, 1, playerLeaseKey(1, session.PlayerID), "logic", "gate", "online", time.Now(), lease.TTL)
	if err != nil {
		t.Fatal(err)
	}
	a.mu.Lock()
	b := a.bound[playerActorKey(1, session.PlayerID)]
	b.Term = rec.Term
	a.bound[playerActorKey(1, session.PlayerID)] = b
	a.mu.Unlock()
	guarded := false
	a.Hooks.Guard = func() func() { guarded = true; return func() { guarded = false } }
	a.Load = func(ctx context.Context, serverID int32, playerID int64) (any, error) {
		term, ok := lease.Term(ctx)
		if !guarded || !ok || term != rec.Term || serverID != 1 || playerID != session.PlayerID {
			t.Error("缺少缓存保护、身份或租约上下文")
		}
		return "player", nil
	}
	task, b := addTestPlayerTask(t, a, session)
	err = a.runPlayerTask(context.Background(), b, task, func(ctx *handler.Context) error {
		v, err := ctx.GetPlayer()
		if v != "player" || ctx.SessionID != session.ID {
			t.Error("加载器或会话错误")
		}
		return err
	})
	if err != nil || guarded {
		t.Fatalf("执行失败或未释放 Guard: %v", err)
	}
	if err := lease.Release(context.Background(), st, 1, playerLeaseKey(1, session.PlayerID), "logic", rec.Term); err != nil {
		t.Fatal(err)
	}
	var ran bool
	err = a.runPlayerTask(context.Background(), b, task, func(*handler.Context) error { ran = true; return nil })
	if !errors.Is(err, lease.ErrHeld) || ran {
		t.Fatalf("失租后执行了业务: %v", err)
	}
}

func TestPlayerTaskQueuedCancellationAndPanic(t *testing.T) {
	a, session := taskApp(t)
	task, b := addTestPlayerTask(t, a, session)
	started, release, blockerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		_ = a.Actors.Do(context.Background(), playerActorKey(1, session.PlayerID), func() { close(started); <-release })
		close(blockerDone)
	}()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var ran bool
	if err := a.runPlayerTask(ctx, b, task, func(*handler.Context) error { ran = true; return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(release)
	<-blockerDone
	if ran {
		t.Fatal("取消后仍执行")
	}
	if err := a.runPlayerTask(context.Background(), b, task, func(*handler.Context) error { panic("失败") }); err == nil {
		t.Fatal("panic 被误报成功")
	}
}

func TestQueuedPlayerTaskWaitsAndSkipsAfterCancel(t *testing.T) {
	a, session := taskApp(t)
	task, b := addTestPlayerTask(t, a, session)
	started, release, blockerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		_ = a.Actors.Do(context.Background(), playerActorKey(1, session.PlayerID), func() { close(started); <-release })
		close(blockerDone)
	}()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var ran atomic.Bool
	result := make(chan error, 1)
	go func() {
		result <- a.runPlayerTask(ctx, b, task, func(*handler.Context) error { ran.Store(true); return nil })
	}()
	// 先确认包装尚未返回，再取消已投递或即将投递的业务；释放邮箱后两种时序都必须跳过业务。
	select {
	case err := <-result:
		close(release)
		t.Fatalf("邮箱占用时提前返回: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	task.Cancel()
	close(release)
	<-blockerDone
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) || ran.Load() {
			t.Fatalf("已取消队列业务仍执行: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("取消包装未完成")
	}
}

func TestCronPlayerTaskWaitsForActualCompletion(t *testing.T) {
	a, session := taskApp(t)
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	var runs atomic.Int32
	_, err := a.AddPlayerTask(session, "slow", "@every 1s", func(ctx *handler.Context) error {
		runs.Add(1)
		close(started)
		<-release
		return ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Scheduler.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("任务未触发")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if err := a.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("邮箱仍执行时不应成功停机: %v", err)
	}
	entries := a.Scheduler.Entries()
	if len(entries) != 1 || !entries[0].Running || runs.Load() != 1 {
		t.Fatal("协作超时提前释放了玩家任务状态")
	}
}

func TestOnlineHookRegistersOnlyForNewBinding(t *testing.T) {
	a, session := taskApp(t)
	calls := 0
	a.Hooks.OnOnline = func(s Session) {
		calls++
		if _, err := a.AddPlayerTask(s, "online", "@every 1s", func(*handler.Context) error { return nil }); err != nil {
			t.Error(err)
		}
	}
	a.bindOnline(context.Background(), Request{ServerID: 1, GateID: "gate", SessionID: "new"}, session.PlayerID, 0)
	a.bindOnline(context.Background(), Request{ServerID: 1, GateID: "gate", SessionID: "new"}, session.PlayerID, 0)
	if calls != 1 || len(a.Scheduler.Entries()) != 1 {
		t.Fatal("每条协议重复注册了会话任务")
	}
}
