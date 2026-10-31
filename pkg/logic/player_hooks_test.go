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

func TestSessionCarriesPlayerKeyThroughLifecycle(t *testing.T) {
	ctx := context.Background()
	r := router.New[int32]()
	a := New(r, nil)
	defer a.Stop()
	const key = "507f1f77bcf86cd799439011"
	a.KeyLoad = func(_ context.Context, serverID int32, playerKey string) (any, error) {
		if serverID != 1 || playerKey != key {
			t.Fatalf("角色加载未使用会话主键: %d %s", serverID, playerKey)
		}
		return key, nil
	}
	r.Handle(1, func(c *handler.Context, _ []byte) ([]byte, error) {
		c.BindPlayer(100001, key)
		return nil, nil
	})
	r.Handle(2, func(c *handler.Context, _ []byte) ([]byte, error) {
		_, err := c.GetPlayer()
		return nil, err
	})
	check := func(c *handler.Context) error {
		_, err := c.GetPlayer()
		return err
	}
	a.Hooks.OnPlayerOnline, a.Hooks.OnPlayerOffline = check, check
	pins, flushes, touches := 0, 0, 0
	a.Hooks.PinKey = func(_ int32, got string) func() {
		if got != key {
			t.Error("钉住未使用 ObjectID")
		}
		pins++
		return func() { pins-- }
	}
	a.Hooks.FlushKey = func(_ context.Context, _ int32, got string) error {
		if got != key || pins != 1 {
			t.Error("离线应在钉住期间按 ObjectID 刷盘")
		}
		flushes++
		return nil
	}
	a.Hooks.TouchKey = func(_ context.Context, _ int32, got string, _ uint64) {
		if got != key {
			t.Error("任期更新未使用 ObjectID")
		}
		touches++
	}
	a.Hooks.Self, a.Hooks.Leases = "logic", memory.New()
	req := Request{ServerID: 1, SessionID: "s", GateID: "g", CmdID: 1}
	resp, err := a.Handle(ctx, req)
	if err != nil || resp.PlayerKey != key {
		t.Fatalf("登录未解析实体主键: %+v %v", resp, err)
	}
	if sessions := a.Sessions(1); len(sessions) != 1 || sessions[0].PlayerKey != key {
		t.Fatalf("会话未保存实体主键: %+v", sessions)
	}
	req.PlayerID, req.CmdID, req.PlayerKey = 100001, 2, "untrusted-key"
	resp, err = a.Handle(ctx, req)
	if err != nil || resp.CmdID != 2 || resp.PlayerKey != key {
		t.Fatalf("后续请求应恢复会话主键: %+v %v", resp, err)
	}
	req.CmdID, req.Internal = CmdDisconnect, true
	if _, err := a.Handle(ctx, req); err != nil {
		t.Fatal(err)
	}
	if pins != 0 || flushes != 1 || touches == 0 {
		t.Fatalf("生命周期未完整使用主键: pins=%d flushes=%d touches=%d", pins, flushes, touches)
	}
}

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
