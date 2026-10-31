package logic

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"tuserv/pkg/lease"
	"tuserv/pkg/store"
	"tuserv/pkg/store/memory"
)

func waitAbandons(t *testing.T, app *App) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		app.abandonMu.Lock()
		done := app.abandonActive == 0 && len(app.abandonPending) == 0
		app.abandonMu.Unlock()
		if done {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("失租清理未完成")
}

func TestSlowAbandonDoesNotBlockRenew(t *testing.T) {
	mem := memory.New()
	now := time.Now()
	app := newLeaseApp(t, "node-a", mem, func() time.Time { return now })
	playerID := mustPlayerID(t, 1, 9)
	mustOnline(t, app, "gate", "session", 1, playerID)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	busy := make(chan struct{})
	go func() {
		defer close(busy)
		_ = app.Actors.Do(context.Background(), playerActorKey(1, playerID), func() {
			close(entered)
			<-release
		})
	}()
	<-entered
	_, err := lease.Claim(context.Background(), mem, 1, playerLeaseKey(1, playerID), "node-b", "gate-b", "session-b", now.Add(lease.TTL+time.Second), lease.TTL)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 20; i++ {
			app.RenewLeases(context.Background())
		}
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("慢邮箱阻塞了后续续租")
	}
	app.abandonMu.Lock()
	n := len(app.abandonPending)
	app.abandonMu.Unlock()
	if n != 1 {
		t.Fatalf("重复失租任务没有合并: %d", n)
	}
	once.Do(func() { close(release) })
	<-busy
	waitAbandons(t, app)
	if len(app.Sessions(1)) != 0 {
		t.Fatal("邮箱恢复后未清理失租会话")
	}
}

type failedRenewLocker struct{ store.Locker }

func (l failedRenewLocker) RenewMany(_ context.Context, items []store.LeaseTouch) ([]store.LeaseMiss, error) {
	misses := make([]store.LeaseMiss, 0, len(items))
	for _, item := range items {
		misses = append(misses, store.LeaseMiss{ServerID: item.ServerID, Key: item.Key, Err: errors.New("写入失败")})
	}
	return misses, nil
}

func TestRenewWriteFailureKeepsSession(t *testing.T) {
	app := newLeaseApp(t, "node-a", memory.New(), time.Now)
	mustOnline(t, app, "gate", "session", 1, mustPlayerID(t, 1, 9))
	app.Hooks.Leases = failedRenewLocker{Locker: app.Hooks.Leases}
	app.RenewLeases(context.Background())
	if app.RenewErrorCount() != 1 || len(app.Sessions(1)) != 1 {
		t.Fatal("普通写错误应计数并保留会话")
	}
}

type timeoutLocker struct{ store.Locker }

func (l timeoutLocker) RenewMany(ctx context.Context, _ []store.LeaseTouch) ([]store.LeaseMiss, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestRenewLoopBoundsBlockedLocker(t *testing.T) {
	app := newLeaseApp(t, "node-a", memory.New(), time.Now)
	mustOnline(t, app, "gate", "session", 1, mustPlayerID(t, 1, 9))
	app.Hooks.Leases = timeoutLocker{Locker: app.Hooks.Leases}
	stop, done := make(chan struct{}), make(chan struct{})
	rounds := make(chan struct{}, 8)
	go func() {
		defer close(done)
		Loops{RenewEvery: 15 * time.Millisecond, FlushEvery: time.Hour, ProbeEvery: time.Hour,
			Renew: func(ctx context.Context) { app.RenewLeases(ctx); rounds <- struct{}{} },
		}.Run(stop)
	}()
	for i := 0; i < 3; i++ {
		select {
		case <-rounds:
		case <-time.After(time.Second):
			close(stop)
			t.Fatal("租约存储阻塞后续租未继续")
		}
	}
	close(stop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("续租超时后未能退出")
	}
	if app.RenewErrorCount() < 3 {
		t.Fatal("续租失败未计数")
	}
}
