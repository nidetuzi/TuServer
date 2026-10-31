package logic

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"tuserv/pkg/cache"
	"tuserv/pkg/entity"
	"tuserv/pkg/handler"
	"tuserv/pkg/lease"
	"tuserv/pkg/router"
	"tuserv/pkg/store"
	"tuserv/pkg/store/memory"
)

// TestRenewLeasesExtendsOwnLease 确认仍由本节点持有时只延长到期时间，不发 AskRelease，也不放开缓存钉住。
func TestRenewLeasesExtendsOwnLease(t *testing.T) {
	mem := memory.New()
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	playerID := mustPlayerID(t, 1, 9)
	var asks atomic.Int32
	var pins atomic.Int32
	app := newLeaseApp(t, "node-a", mem, func() time.Time { return now })
	app.Hooks.AskRelease = func(context.Context, string, []byte) error {
		asks.Add(1)
		return nil
	}
	app.Hooks.PinPlayer = func(int32, int64) func() {
		pins.Add(1)
		return func() { pins.Add(-1) }
	}
	mustOnline(t, app, "gate-a", "sess-a", 1, playerID)
	if pins.Load() != 1 {
		t.Fatalf("在线应钉住缓存, n=%d", pins.Load())
	}

	now = now.Add(time.Second)
	app.RenewLeases(ctx)

	if asks.Load() != 0 {
		t.Fatalf("续自己的租约不应 AskRelease，次数=%d", asks.Load())
	}
	if pins.Load() != 1 {
		t.Fatalf("续约不应放开钉住, n=%d", pins.Load())
	}
	rec := peekLease(t, mem, 1, playerLeaseKey(1, playerID))
	if rec.Owner != "node-a" || rec.Term != 1 {
		t.Fatalf("续约不应改持有者或任期: %+v", rec)
	}
	wantUntil := now.Add(lease.TTL).UnixMilli()
	if rec.Until != wantUntil {
		t.Fatalf("到期时间=%d，想要 %d", rec.Until, wantUntil)
	}
	if got := app.Sessions(1); len(got) != 1 || got[0].ID != "sess-a" {
		t.Fatalf("续约后会话应还在: %+v", got)
	}
}

// TestRenewLeasesConcurrentWithRelinquish 复现续约快照和交接重叠。
// 旧实现会在锁外 enter，向已经接走角色的节点发 AskRelease，新会话被踢、租约又被旧节点抢走。
func TestRenewLeasesConcurrentWithRelinquish(t *testing.T) {
	mem := memory.New()
	// 时钟停住，B 不能靠过期直接接手，必须走 Relinquish。
	now := time.Unix(1_700_000_000, 0)
	playerID := mustPlayerID(t, 1, 9)
	hold := &holdLocker{inner: mem, started: make(chan struct{}), resume: make(chan struct{})}
	appA := newLeaseApp(t, "node-a", hold, func() time.Time { return now })
	appB := newLeaseApp(t, "node-b", hold, func() time.Time { return now })
	var asks atomic.Int32
	var mu sync.Mutex
	kicks := map[string][]string{}
	var pinsA, pinsB atomic.Int32
	pins := map[string]*atomic.Int32{"node-a": &pinsA, "node-b": &pinsB}
	recordKick := func(node string) func(context.Context, string, string) {
		return func(_ context.Context, _, sessionID string) {
			mu.Lock()
			kicks[node] = append(kicks[node], sessionID)
			mu.Unlock()
		}
	}
	pin := func(node string) func(int32, int64) func() {
		return func(int32, int64) func() {
			pins[node].Add(1)
			return func() { pins[node].Add(-1) }
		}
	}
	appA.Hooks.Kick = recordKick("node-a")
	appB.Hooks.Kick = recordKick("node-b")
	appA.Hooks.PinPlayer = pin("node-a")
	appB.Hooks.PinPlayer = pin("node-b")
	apps := map[string]*App{"node-a": appA, "node-b": appB}
	ask := func(ctx context.Context, host string, body []byte) error {
		asks.Add(1)
		target := apps[host]
		if target == nil {
			return errors.New("logic: test host missing")
		}
		// 内部交接必须带 Internal。只写 GateID 会被当成客户端非法帧，租约交不出去。
		_, err := target.Handle(ctx, Request{GateID: InternalCaller, CmdID: CmdRelinquish, Internal: true, Body: body})
		return err
	}
	appA.Hooks.AskRelease = ask
	appB.Hooks.AskRelease = ask

	mustOnline(t, appA, "gate-a", "sess-a", 1, playerID)
	hold.Arm()
	done := make(chan struct{})
	go func() {
		defer close(done)
		appA.RenewLeases(context.Background())
	}()
	select {
	case <-hold.started:
	case <-time.After(2 * time.Second):
		t.Fatal("续约没有进入租约写入")
	}

	loginErr := make(chan error, 1)
	go func() {
		_, err := appB.Handle(context.Background(), Request{
			GateID: "gate-b", SessionID: "sess-b", ServerID: 1, PlayerID: playerID, CmdID: 1,
		})
		loginErr <- err
	}()
	select {
	case err := <-loginErr:
		if err != nil {
			close(hold.resume)
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		close(hold.resume)
		t.Fatal("交接卡住")
	}
	if asks.Load() != 1 {
		close(hold.resume)
		t.Fatalf("接手时应只通知旧节点一次，次数=%d", asks.Load())
	}
	before := peekLease(t, mem, 1, playerLeaseKey(1, playerID))
	if before.Owner != "node-b" || before.Term != 2 {
		close(hold.resume)
		t.Fatalf("交接后租约应在 B: %+v", before)
	}
	close(hold.resume)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("续约没有结束")
	}

	if asks.Load() != 1 {
		t.Fatalf("续约失败后又发了 AskRelease，次数=%d", asks.Load())
	}
	after := peekLease(t, mem, 1, playerLeaseKey(1, playerID))
	if after != before {
		t.Fatalf("旧节点续约改了新持有者的租约: before=%+v after=%+v", before, after)
	}
	if got := appB.Sessions(1); len(got) != 1 || got[0].ID != "sess-b" {
		t.Fatalf("新会话应保持在线: %+v", got)
	}
	if pins["node-b"].Load() != 1 {
		t.Fatalf("新节点的缓存钉住应还在, n=%d", pins["node-b"].Load())
	}
	if pins["node-a"].Load() != 0 {
		t.Fatalf("旧节点交接后应放开钉住, n=%d", pins["node-a"].Load())
	}
	appA.mu.Lock()
	_, still := appA.bound[playerActorKey(1, playerID)]
	appA.mu.Unlock()
	if still {
		t.Fatal("旧节点不应再绑着这个角色")
	}
	mu.Lock()
	defer mu.Unlock()
	for _, id := range kicks["node-b"] {
		if id == "sess-b" {
			t.Fatal("续约不应踢掉新会话")
		}
	}
}

// TestRenewLeasesDropsLocalWhenHeldElsewhere 覆盖绑定还在、租约已被别人接走的放弃路径。
func TestRenewLeasesDropsLocalWhenHeldElsewhere(t *testing.T) {
	mem := memory.New()
	c := cache.New(mem)
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	const serverID int32 = 1
	playerID := mustPlayerID(t, serverID, 9)
	idKey := strconv.FormatInt(playerID, 10)
	if err := mem.Save(ctx, serverID, "player", idKey, fenceDoc{ID: idKey, Name: "new", Rev: 3, Fence: 4}); err != nil {
		t.Fatal(err)
	}
	app := newLeaseApp(t, "node-a", mem, func() time.Time { return now })
	var asks atomic.Int32
	var pins atomic.Int32
	var guardDepth atomic.Int32
	var flushOutside atomic.Bool
	var mu sync.Mutex
	var kicks []string
	var order []string
	note := func(step string) {
		mu.Lock()
		order = append(order, step)
		mu.Unlock()
	}
	app.Hooks.Guard = func() func() {
		guardDepth.Add(1)
		release := c.Guard()
		return func() {
			release()
			guardDepth.Add(-1)
		}
	}
	app.Hooks.PinPlayer = func(serverID int32, id int64) func() {
		pins.Add(1)
		release := c.PinKey(serverID, "player", strconv.FormatInt(id, 10))
		return func() {
			release()
			pins.Add(-1)
			note("unpin")
		}
	}
	app.Hooks.AskRelease = func(ctx context.Context, host string, body []byte) error {
		asks.Add(1)
		var rel Release
		if err := json.Unmarshal(body, &rel); err != nil {
			return err
		}
		// 模拟新节点答应交出。若续约误走 enter，旧节点会接着把租约 Claim 回去。
		return lease.Release(ctx, mem, rel.ServerID, playerLeaseKey(rel.ServerID, rel.ID), host, rel.Term)
	}
	app.Hooks.FlushID = func(ctx context.Context, serverID int32, collection string, id int64) error {
		if guardDepth.Load() == 0 {
			flushOutside.Store(true)
		}
		note("flush")
		e, err := c.Get(ctx, serverID, collection, strconv.FormatInt(id, 10), nil)
		if err != nil {
			return nil
		}
		err = c.FlushEntity(ctx, e)
		c.Forget(e)
		return err
	}
	app.Hooks.Kick = func(_ context.Context, _, sessionID string) {
		mu.Lock()
		kicks = append(kicks, sessionID)
		mu.Unlock()
	}
	mustOnline(t, app, "gate-a", "sess-a", serverID, playerID)
	stale := &fenceEnt{ID: idKey, Server: serverID, Name: "old", Fence: 1}
	stale.InitRev(1)
	stale.MarkDirty()
	c.Adopt(stale)

	later := now.Add(lease.TTL + time.Second)
	got, err := lease.Claim(ctx, mem, serverID, playerLeaseKey(serverID, playerID), "node-b", "gate-b", "sess-b", later, lease.TTL)
	if err != nil || got.Owner != "node-b" || got.Term != 2 {
		t.Fatalf("过期后应由 B 接手: %+v %v", got, err)
	}
	app.RenewLeases(ctx)

	if asks.Load() != 0 {
		t.Fatalf("续约失败不能 AskRelease，次数=%d", asks.Load())
	}
	if flushOutside.Load() {
		t.Fatal("放弃角色时刷盘必须在 Guard 内")
	}
	rec := peekLease(t, mem, serverID, playerLeaseKey(serverID, playerID))
	if rec.Owner != "node-b" || rec.Term != got.Term || rec.Until != got.Until {
		t.Fatalf("续约不得抢回或改写租约: %+v", rec)
	}
	app.mu.Lock()
	_, still := app.bound[playerActorKey(serverID, playerID)]
	app.mu.Unlock()
	if still {
		t.Fatal("失去租约后应清掉本地绑定")
	}
	if sessions := app.Sessions(serverID); len(sessions) != 0 {
		t.Fatalf("本地会话应被清掉: %+v", sessions)
	}
	if pins.Load() != 0 {
		t.Fatalf("放弃后应放开缓存钉住, n=%d", pins.Load())
	}
	mu.Lock()
	if len(kicks) != 1 || kicks[0] != "sess-a" {
		t.Fatalf("应踢掉本地会话: %v", kicks)
	}
	flushed, unpinned := false, false
	for _, step := range order {
		if step == "flush" {
			flushed = true
		}
		if step == "unpin" {
			if !flushed {
				t.Fatal("应先刷盘再放开钉住")
			}
			unpinned = true
		}
	}
	mu.Unlock()
	if !flushed || !unpinned {
		t.Fatalf("放弃时应刷盘并放开钉住: %v", order)
	}
	var loaded fenceDoc
	if err := mem.Load(ctx, serverID, "player", idKey, &loaded); err != nil {
		t.Fatal(err)
	}
	if loaded.Name != "new" || loaded.Fence != 4 {
		t.Fatalf("旧节点刷盘盖掉了新任期存档: %+v", loaded)
	}
	if _, err := c.Get(ctx, serverID, "player", idKey, nil); err == nil {
		t.Fatal("放弃后应忘掉本地实体")
	}
}

// TestRenewLeasesReleasesOwnedLeaseAfterBindingGone 确认会话已经消失时，仍由自己持有的续约会按原任期还回去。
func TestRenewLeasesReleasesOwnedLeaseAfterBindingGone(t *testing.T) {
	mem := memory.New()
	now := time.Unix(1_700_000_000, 0)
	hold := &holdLocker{inner: mem, started: make(chan struct{}), resume: make(chan struct{})}
	playerID := mustPlayerID(t, 1, 9)
	var asks atomic.Int32
	app := newLeaseApp(t, "node-a", hold, func() time.Time { return now })
	app.Hooks.AskRelease = func(context.Context, string, []byte) error {
		asks.Add(1)
		return nil
	}
	mustOnline(t, app, "gate-a", "sess-a", 1, playerID)
	hold.Arm()
	done := make(chan struct{})
	go func() {
		defer close(done)
		app.RenewLeases(context.Background())
	}()
	select {
	case <-hold.started:
	case <-time.After(2 * time.Second):
		t.Fatal("续约没有进入租约写入")
	}
	key := playerActorKey(1, playerID)
	app.mu.Lock()
	b := app.bound[key]
	delete(app.bound, key)
	delete(app.sessions, b.ID)
	unpin := b.unpin
	app.mu.Unlock()
	if unpin != nil {
		unpin()
	}
	close(hold.resume)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("续约没有结束")
	}
	if asks.Load() != 0 {
		t.Fatalf("绑定消失不应 AskRelease，次数=%d", asks.Load())
	}
	rec := peekLease(t, mem, 1, playerLeaseKey(1, playerID))
	if rec.Owner != "" || rec.Until != 0 {
		t.Fatalf("没有会话的租约应按原任期还回去: %+v", rec)
	}
}

// TestRenewLeasesKeepsSessionWhenTermChangesLocally 避免把仍由本节点持有、只是任期对不上的会话踢掉。
func TestRenewLeasesKeepsSessionWhenTermChangesLocally(t *testing.T) {
	mem := memory.New()
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	playerID := mustPlayerID(t, 1, 9)
	var asks atomic.Int32
	var kicks atomic.Int32
	var pins atomic.Int32
	app := newLeaseApp(t, "node-a", mem, func() time.Time { return now })
	app.Hooks.AskRelease = func(context.Context, string, []byte) error {
		asks.Add(1)
		return nil
	}
	app.Hooks.Kick = func(context.Context, string, string) { kicks.Add(1) }
	app.Hooks.PinPlayer = func(int32, int64) func() {
		pins.Add(1)
		return func() { pins.Add(-1) }
	}
	mustOnline(t, app, "gate-a", "sess-a", 1, playerID)
	key := playerLeaseKey(1, playerID)
	before := peekLease(t, mem, 1, key)
	if _, err := mem.MutateLease(ctx, 1, key, func(cur store.LeaseRec) (store.LeaseRec, error) {
		cur.Term++
		return cur, nil
	}); err != nil {
		t.Fatal(err)
	}
	app.RenewLeases(ctx)
	if asks.Load() != 0 || kicks.Load() != 0 || pins.Load() != 1 {
		t.Fatalf("任期变化但持有者仍是自己时不应交接或踢线: ask=%d kick=%d pin=%d", asks.Load(), kicks.Load(), pins.Load())
	}
	after := peekLease(t, mem, 1, key)
	if after.Owner != "node-a" || after.Term != before.Term+1 || after.Until != before.Until {
		t.Fatalf("不应改写仍属于自己的租约: %+v", after)
	}
	if got := app.Sessions(1); len(got) != 1 || got[0].ID != "sess-a" {
		t.Fatalf("本地会话应保留: %+v", got)
	}
}

func newLeaseApp(t *testing.T, self string, leases store.Locker, now func() time.Time) *App {
	t.Helper()
	r := router.New[int32]()
	r.Handle(1, func(*handler.Context, []byte) ([]byte, error) {
		return []byte("ok"), nil
	})
	app := New(r, nil)
	t.Cleanup(app.Stop)
	app.Hooks.Self = self
	app.Hooks.Leases = leases
	app.Hooks.Now = now
	return app
}

func mustPlayerID(t *testing.T, serverID int32, seq int64) int64 {
	t.Helper()
	id, err := entity.WithServer(serverID, seq)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func mustOnline(t *testing.T, app *App, gate, session string, serverID int32, playerID int64) {
	t.Helper()
	_, err := app.Handle(context.Background(), Request{
		GateID: gate, SessionID: session, ServerID: serverID, PlayerID: playerID, CmdID: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
}

var errPeekLease = errors.New("peek lease")

func peekLease(t *testing.T, backend store.Locker, serverID int32, key string) store.LeaseRec {
	t.Helper()
	rec, err := backend.MutateLease(context.Background(), serverID, key, func(cur store.LeaseRec) (store.LeaseRec, error) {
		return cur, errPeekLease
	})
	if !errors.Is(err, errPeekLease) {
		t.Fatalf("读取租约: %v", err)
	}
	return rec
}

// holdLocker 让下一次写租约停在落库之前，用来把 Relinquish 插进续约窗口。
type holdLocker struct {
	inner   store.Locker
	mu      sync.Mutex
	armed   bool
	started chan struct{}
	resume  chan struct{}
}

func (h *holdLocker) Arm() {
	h.mu.Lock()
	h.armed = true
	h.mu.Unlock()
}

func (h *holdLocker) MutateLease(ctx context.Context, serverID int32, key string, fn func(store.LeaseRec) (store.LeaseRec, error)) (store.LeaseRec, error) {
	if h.blockOnce() {
		close(h.started)
		<-h.resume
	}
	return h.inner.MutateLease(ctx, serverID, key, fn)
}

// RenewMany 是续租现在的写入入口。并发用例要停在批量写之前，交接才插得进窗口。
func (h *holdLocker) RenewMany(ctx context.Context, items []store.LeaseTouch) ([]store.LeaseMiss, error) {
	if h.blockOnce() {
		close(h.started)
		<-h.resume
	}
	return h.inner.RenewMany(ctx, items)
}

func (h *holdLocker) blockOnce() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.armed {
		return false
	}
	h.armed = false
	return true
}

type fenceDoc struct {
	ID    string `bson:"_id"`
	Name  string `bson:"name"`
	Rev   uint64 `bson:"rev"`
	Fence uint64 `bson:"fence"`
}

func (d fenceDoc) DocVersion() (uint64, uint64) { return d.Rev, d.Fence }

type fenceEnt struct {
	entity.State `bson:"-"`
	ID           string
	Server       int32
	Name         string
	Fence        uint64
}

func (e *fenceEnt) CacheKey() string            { return e.ID }
func (e *fenceEnt) ServerID() int32             { return e.Server }
func (e *fenceEnt) Collection() string          { return "player" }
func (e *fenceEnt) RouteKey() string            { return e.ID }
func (e *fenceEnt) Placement() entity.Placement { return entity.PlacementLocal }

func (e *fenceEnt) Persist() (uint64, any) {
	var doc fenceDoc
	gen := e.Capture(func() {
		doc.ID = e.ID
		doc.Name = e.Name
		doc.Fence = e.Fence
	})
	doc.Rev = gen
	return gen, doc
}
