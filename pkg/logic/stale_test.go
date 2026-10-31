package logic

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"tuserv/pkg/cache"
	"tuserv/pkg/entity"
	"tuserv/pkg/store/memory"
)

func TestStaleFlushDropsBoundSession(t *testing.T) {
	mem := memory.New()
	app := newLeaseApp(t, "node-a", mem, time.Now)
	id := mustPlayerID(t, 1, 9)
	var pins, kicks atomic.Int32
	app.Hooks.PinPlayer = func(int32, int64) func() {
		pins.Add(1)
		return func() { pins.Add(-1) }
	}
	app.Hooks.Kick = func(context.Context, string, string) { kicks.Add(1) }
	mustOnline(t, app, "gate", "session", 1, id)
	c := cache.New(mem, cache.WithStaleHandler(func(_ entity.Entity, _, term uint64) func() {
		return app.PrepareStalePlayer(1, id, term)
	}))
	old := &fenceEnt{ID: "oid", Server: 1, Name: "old", Fence: 1}
	old.InitRev(1)
	old.MarkDirty()
	c.Adopt(old)
	if err := mem.Save(context.Background(), 1, "player", "oid", fenceDoc{ID: "oid", Name: "new", Rev: 9, Fence: 2}); err != nil {
		t.Fatal(err)
	}
	// 在角色邮箱内触发，回调也不能同步 Do 回自己或在持有 flushMu 时再次刷盘。
	if err := app.Actors.Do(context.Background(), playerActorKey(1, id), func() {
		if err := c.Flush(context.Background()); err != nil {
			t.Error(err)
		}
		app.bindOnline(context.Background(), Request{GateID: "gate", SessionID: "session", ServerID: 1}, id, 1)
		if !app.stale(Request{ServerID: 1, PlayerID: id, SessionID: "session"}) {
			t.Error("在途协议回写绑定不应清除存档冲突标记")
		}
	}); err != nil {
		t.Fatal(err)
	}
	waitAbandons(t, app)
	if c.Len() != 0 || pins.Load() != 0 || kicks.Load() != 1 || len(app.Sessions(1)) != 0 {
		t.Fatalf("冲突后应丢弃缓存并清理会话: cache=%d pins=%d kicks=%d", c.Len(), pins.Load(), kicks.Load())
	}
}

func TestDelayedStaleNotificationPreservesNewSession(t *testing.T) {
	app := newLeaseApp(t, "node-a", memory.New(), time.Now)
	id := mustPlayerID(t, 1, 9)
	mustOnline(t, app, "gate", "old", 1, id)
	notify := app.PrepareStalePlayer(1, id, 1)
	if notify == nil {
		t.Fatal("缺少存档冲突通知")
	}
	app.bindOnline(context.Background(), Request{GateID: "gate", SessionID: "new", ServerID: 1}, id, 1)
	var kicks atomic.Int32
	app.Hooks.Kick = func(context.Context, string, string) { kicks.Add(1) }
	notify()
	waitAbandons(t, app)
	got := app.Sessions(1)
	if len(got) != 1 || got[0].ID != "new" || kicks.Load() != 0 {
		t.Fatalf("延迟冲突误伤新会话: %+v", got)
	}
}
