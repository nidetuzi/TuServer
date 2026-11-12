package logic

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"tuserv/pkg/lease"
	"tuserv/pkg/owner"
	"tuserv/pkg/store"
	"tuserv/pkg/store/memory"
)

// TestRelinquishStaleTermKeepsNewSession 覆盖迟到的旧交接：目标已持有更新任期时不能清会话或放租约。
func TestRelinquishStaleTermKeepsNewSession(t *testing.T) {
	mem := memory.New()
	now := time.Unix(1_700_000_000, 0)
	playerID := mustPlayerID(t, 1, 9)
	app := newLeaseApp(t, "node-a", mem, func() time.Time { return now })
	var kicks int
	app.Hooks.Kick = func(context.Context, string, string) { kicks++ }

	mustOnline(t, app, "gate-a", "sess-new", 1, playerID)
	rec := peekLease(t, mem, 1, playerLeaseKey(1, playerID))
	if rec.Term == 0 {
		t.Fatal("登录后应持有任期")
	}
	stale := rec.Term
	_, err := mem.MutateLease(context.Background(), 1, playerLeaseKey(1, playerID), func(cur store.LeaseRec) (store.LeaseRec, error) {
		cur.Term = stale + 1
		return cur, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	app.mu.Lock()
	b := app.bound[playerActorKey(1, playerID)]
	b.Term = stale + 1
	app.bound[playerActorKey(1, playerID)] = b
	app.mu.Unlock()

	body, err := json.Marshal(Release{Kind: KindPlayer, ServerID: 1, ID: playerID, Term: stale, Gate: "gate-b", Session: "sess-old"})
	if err != nil {
		t.Fatal(err)
	}
	if err := app.relinquish(context.Background(), body); err != nil {
		t.Fatal(err)
	}
	if got := app.Sessions(1); len(got) != 1 || got[0].ID != "sess-new" {
		t.Fatalf("旧交接不应清掉新会话: %+v", got)
	}
	after := peekLease(t, mem, 1, playerLeaseKey(1, playerID))
	if after.Owner != "node-a" || after.Term != stale+1 {
		t.Fatalf("旧交接不应释放新任期: %+v", after)
	}
	if kicks != 0 {
		t.Fatalf("旧交接不应踢新会话, kicks=%d", kicks)
	}
}

// TestRelinquishFlushFailureKeepsLease 刷盘失败时必须保留租约与会话，避免对端加载旧档。
func TestRelinquishFlushFailureKeepsLease(t *testing.T) {
	mem := memory.New()
	now := time.Unix(1_700_000_000, 0)
	playerID := mustPlayerID(t, 1, 9)
	app := newLeaseApp(t, "node-a", mem, func() time.Time { return now })
	flushErr := errors.New("flush boom")
	app.Hooks.FlushID = func(context.Context, int32, string, int64) error {
		return flushErr
	}
	var kicks int
	app.Hooks.Kick = func(context.Context, string, string) { kicks++ }

	mustOnline(t, app, "gate-a", "sess-a", 1, playerID)
	before := peekLease(t, mem, 1, playerLeaseKey(1, playerID))
	body, err := json.Marshal(Release{
		Kind: KindPlayer, ServerID: 1, ID: playerID, Term: before.Term, Gate: "gate-b", Session: "sess-b",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := app.relinquish(context.Background(), body); !errors.Is(err, flushErr) {
		t.Fatalf("刷盘失败应返回错误: %v", err)
	}
	if got := app.Sessions(1); len(got) != 1 || got[0].ID != "sess-a" {
		t.Fatalf("刷盘失败应保留会话: %+v", got)
	}
	after := peekLease(t, mem, 1, playerLeaseKey(1, playerID))
	if after != before {
		t.Fatalf("刷盘失败不应释放租约: before=%+v after=%+v", before, after)
	}
	if kicks != 0 {
		t.Fatalf("刷盘失败不应踢线, kicks=%d", kicks)
	}
}

// TestDisconnectFlushFailureKeepsLease 断线刷盘失败时不能交出租约。
func TestDisconnectFlushFailureKeepsLease(t *testing.T) {
	mem := memory.New()
	now := time.Unix(1_700_000_000, 0)
	playerID := mustPlayerID(t, 1, 9)
	app := newLeaseApp(t, "node-a", mem, func() time.Time { return now })
	flushErr := errors.New("flush boom")
	app.Hooks.FlushID = func(context.Context, int32, string, int64) error {
		return flushErr
	}

	mustOnline(t, app, "gate-a", "sess-a", 1, playerID)
	before := peekLease(t, mem, 1, playerLeaseKey(1, playerID))
	err := app.onDisconnect(context.Background(), Request{
		Internal: true, CmdID: CmdDisconnect, GateID: "gate-a", SessionID: "sess-a", ServerID: 1, PlayerID: playerID,
	})
	if !errors.Is(err, flushErr) {
		t.Fatalf("刷盘失败应返回错误: %v", err)
	}
	if got := app.Sessions(1); len(got) != 1 || got[0].ID != "sess-a" {
		t.Fatalf("刷盘失败应保留绑定以便续租: %+v", got)
	}
	after := peekLease(t, mem, 1, playerLeaseKey(1, playerID))
	if after != before {
		t.Fatalf("刷盘失败不应释放租约: before=%+v after=%+v", before, after)
	}
}

// TestRelinquishOwnerUsesReleaseKind 共享实体交接必须按 Release.Kind 刷盘和释放，不能写死 guild。
func TestRelinquishOwnerUsesReleaseKind(t *testing.T) {
	mem := memory.New()
	now := time.Unix(1_700_000_000, 0)
	app := newLeaseApp(t, "node-a", mem, func() time.Time { return now })
	var flushed string
	app.Hooks.FlushID = func(_ context.Context, _ int32, collection string, id int64) error {
		flushed = collection
		if id != 42 {
			t.Fatalf("id=%d", id)
		}
		return nil
	}
	key := owner.Key(1, "account", 42)
	_, err := lease.Claim(context.Background(), mem, 1, key.String(), "node-a", "", "", now, lease.TTL)
	if err != nil {
		t.Fatal(err)
	}
	before := peekLease(t, mem, 1, key.String())
	body, err := json.Marshal(Release{Kind: "account", ServerID: 1, ID: 42, Term: before.Term})
	if err != nil {
		t.Fatal(err)
	}
	if err := app.relinquish(context.Background(), body); err != nil {
		t.Fatal(err)
	}
	if flushed != "account" {
		t.Fatalf("应按 kind 刷盘, got=%q", flushed)
	}
	after := peekLease(t, mem, 1, key.String())
	if after.Owner != "" || after.Until != 0 {
		t.Fatalf("应释放 account 租约: %+v", after)
	}
	guildKey := owner.Key(1, KindGuild, 42)
	guild := peekLease(t, mem, 1, guildKey.String())
	if guild.Found && guild.Owner != "" {
		t.Fatalf("不应误动 guild 租约: %+v", guild)
	}
}
