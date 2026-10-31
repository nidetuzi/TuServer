package logic

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"tuserv/pkg/handler"
	"tuserv/pkg/lease"
	"tuserv/pkg/router"
	"tuserv/pkg/store/memory"
)

func TestInitialLoginHookUsesPlayerActorAndLease(t *testing.T) {
	r := router.New[int32]()
	a := New(r, nil)
	defer a.Stop()
	st := memory.New()
	a.Hooks.Self, a.Hooks.Leases = "logic", st
	r.Handle(1, func(ctx *handler.Context, _ []byte) ([]byte, error) { ctx.BindPlayer(100001); return nil, nil })
	var online atomic.Int32
	a.Hooks.OnPlayerOnline = func(ctx *handler.Context) error {
		if term, ok := lease.Term(ctx); !ok || term == 0 || ctx.PlayerID != 100001 {
			t.Error("上线业务缺少租约或角色身份")
		}
		online.Add(1)
		return nil
	}
	blocked, release, drained := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		_ = a.Actors.Do(context.Background(), playerActorKey(1, 100001), func() { close(blocked); <-release })
		close(drained)
	}()
	<-blocked
	result := make(chan error, 1)
	go func() {
		_, err := a.Handle(context.Background(), Request{ServerID: 1, SessionID: "one", GateID: "gate", CmdID: 1})
		result <- err
	}()
	select {
	case err := <-result:
		close(release)
		t.Fatalf("角色邮箱占用时登录提前完成: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if online.Load() != 0 {
		t.Error("上线业务在会话邮箱中提前执行")
	}
	close(release)
	<-drained
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if online.Load() != 1 {
		t.Fatal("上线业务未执行")
	}
	var offline atomic.Int32
	a.Hooks.OnPlayerOffline = func(ctx *handler.Context) error {
		if term, ok := lease.Term(ctx); !ok || term == 0 {
			t.Error("离线业务缺少租约")
		}
		offline.Add(1)
		return nil
	}
	a.Hooks.FlushID = func(context.Context, int32, string, int64) error {
		if offline.Load() != 1 {
			t.Error("在记录离线时间前就刷盘")
		}
		return nil
	}
	if err := a.onDisconnect(context.Background(), Request{ServerID: 1, PlayerID: 100001, SessionID: "one", GateID: "gate"}); err != nil {
		t.Fatal(err)
	}
	if err := a.onDisconnect(context.Background(), Request{ServerID: 1, PlayerID: 100001, SessionID: "one", GateID: "gate"}); err != nil {
		t.Fatal(err)
	}
	if offline.Load() != 1 {
		t.Fatal("重复断线重复执行离线业务")
	}
}
