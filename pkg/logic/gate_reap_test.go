package logic

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"tuserv/pkg/handler"
	"tuserv/pkg/lease"
	"tuserv/pkg/router"
	"tuserv/pkg/store"
	"tuserv/pkg/store/memory"
)

func TestReapGatesFlushesBeforeRelease(t *testing.T) {
	app, st, flushed, now := onlineApp(t)
	defer app.Stop()
	var pins atomic.Int32
	app.Hooks.PinPlayer = func(int32, int64) func() {
		pins.Add(1)
		return func() { pins.Add(-1) }
	}
	login(t, app, "gate-a", "s1", 7)
	if pins.Load() != 1 {
		t.Fatalf("在线角色应钉住缓存: %d", pins.Load())
	}
	if !held(st, 7, now) {
		t.Fatal("登录后租约应在本节点")
	}
	app.ReapGates(context.Background(), []string{"gate-a"}, 2)
	if len(app.Sessions(1)) != 1 {
		t.Fatal("第一次缺席不应清理")
	}
	app.ReapGates(context.Background(), []string{"gate-b"}, 2)
	if len(app.Sessions(1)) != 1 || len(*flushed) != 0 {
		t.Fatal("缺席次数不够，不应刷盘")
	}
	app.ReapGates(context.Background(), nil, 2)
	if len(app.Sessions(1)) != 0 {
		t.Fatal("连续缺席后应清掉会话")
	}
	if len(*flushed) != 1 || (*flushed)[0] != "player:7" {
		t.Fatalf("应先刷该角色: %v", *flushed)
	}
	if held(st, 7, now) {
		t.Fatal("清理后其他节点应能立刻接手")
	}
	if pins.Load() != 0 {
		t.Fatalf("刷盘后再放开缓存钉住: %d", pins.Load())
	}
}

func TestReapGatesResetsWhenGateReturns(t *testing.T) {
	app, _, _, _ := onlineApp(t)
	defer app.Stop()
	login(t, app, "gate-a", "s1", 7)
	app.ReapGates(context.Background(), nil, 2)
	app.ReapGates(context.Background(), []string{"gate-a"}, 2)
	app.ReapGates(context.Background(), nil, 2)
	if len(app.Sessions(1)) != 1 {
		t.Fatal("网关重新出现后，缺席次数应重新计算")
	}
}

func TestReapGatesLeavesOtherGate(t *testing.T) {
	app, st, flushed, now := onlineApp(t)
	defer app.Stop()
	login(t, app, "gate-a", "s1", 7)
	login(t, app, "gate-b", "s2", 8)
	app.ReapGates(context.Background(), []string{"gate-b"}, 1)
	got := app.Sessions(1)
	if len(got) != 1 || got[0].ID != "s2" || got[0].GateAddr != "gate-b" {
		t.Fatalf("只应留下还活着的网关: %+v", got)
	}
	if len(*flushed) != 1 || (*flushed)[0] != "player:7" {
		t.Fatalf("只应刷掉失联网关上的角色: %v", *flushed)
	}
	if !held(st, 8, now) {
		t.Fatal("活着的网关上的角色租约不能放")
	}
	if held(st, 7, now) {
		t.Fatal("失联网关上的角色租约应已释放")
	}
}

func TestReapDoesNotFlushMovedPlayer(t *testing.T) {
	app, st, flushed, now := onlineApp(t)
	defer app.Stop()
	var pins atomic.Int32
	app.Hooks.PinPlayer = func(int32, int64) func() {
		pins.Add(1)
		return func() { pins.Add(-1) }
	}
	login(t, app, "gate-a", "s1", 7)
	login(t, app, "gate-b", "s2", 7)
	before := len(*flushed)
	if _, err := app.Handle(context.Background(), Request{
		Internal: true, CmdID: CmdDisconnect,
		GateID: "gate-a", SessionID: "s1", ServerID: 1, PlayerID: 7,
	}); err != nil {
		t.Fatal(err)
	}
	if len(*flushed) != before {
		t.Fatal("角色已经换到活着的网关，不应再刷盘")
	}
	got := app.Sessions(1)
	if len(got) != 1 || got[0].ID != "s2" {
		t.Fatalf("新会话应保留: %+v", got)
	}
	if !held(st, 7, now) {
		t.Fatal("新会话的租约应还在")
	}
	if pins.Load() != 1 {
		t.Fatalf("换到活网关后钉住应留着: %d", pins.Load())
	}
}

func TestClientDisconnectStillFlushes(t *testing.T) {
	app, st, flushed, now := onlineApp(t)
	defer app.Stop()
	login(t, app, "gate-a", "s1", 7)
	if _, err := app.Handle(context.Background(), Request{
		Internal: true, CmdID: CmdDisconnect,
		GateID: "gate-a", SessionID: "s1", ServerID: 1, PlayerID: 7,
	}); err != nil {
		t.Fatal(err)
	}
	if len(app.Sessions(1)) != 0 || len(*flushed) != 1 {
		t.Fatalf("客户端断线应刷盘并删会话: sessions=%d flush=%v", len(app.Sessions(1)), *flushed)
	}
	if held(st, 7, now) {
		t.Fatal("客户端断线后租约应释放")
	}
}

func TestRenewReleasesWhenBindingGone(t *testing.T) {
	app, st, _, now := onlineApp(t)
	defer app.Stop()
	login(t, app, "gate-a", "s1", 7)
	app.Hooks.Leases = &dropOnce{Locker: st, app: app, player: 7}
	app.RenewLeases(context.Background())
	if len(app.Sessions(1)) != 0 {
		t.Fatal("绑定已经不在，不应被续约写回去")
	}
	if held(st, 7, now) {
		t.Fatal("续约发生在断线之后，租约应被还回去")
	}
}

type dropOnce struct {
	store.Locker
	app    *App
	player int64
	once   sync.Once
}

func (d *dropOnce) MutateLease(ctx context.Context, serverID int32, key string, fn func(store.LeaseRec) (store.LeaseRec, error)) (store.LeaseRec, error) {
	rec, err := d.Locker.MutateLease(ctx, serverID, key, fn)
	d.forget()
	return rec, err
}

func (d *dropOnce) RenewMany(ctx context.Context, items []store.LeaseTouch) ([]store.LeaseMiss, error) {
	misses, err := d.Locker.RenewMany(ctx, items)
	d.forget()
	return misses, err
}

func (d *dropOnce) forget() {
	d.once.Do(func() {
		d.app.mu.Lock()
		for id, s := range d.app.sessions {
			if s.PlayerID == d.player {
				delete(d.app.sessions, id)
			}
		}
		for k, b := range d.app.bound {
			if b.PlayerID == d.player {
				delete(d.app.bound, k)
			}
		}
		d.app.mu.Unlock()
	})
}

func onlineApp(t *testing.T) (*App, *memory.Store, *[]string, time.Time) {
	t.Helper()
	r := router.New[int32]()
	app := New(r, nil)
	st := memory.New()
	flushed := []string{}
	now := time.UnixMilli(1_700_000_000_000)
	app.Hooks = Hooks{
		Self:   "logic-a",
		Leases: st,
		Now:    func() time.Time { return now },
		FlushID: func(_ context.Context, serverID int32, collection string, id int64) error {
			if _, err := lease.Claim(context.Background(), st, serverID, playerLeaseKey(serverID, id), "other", "x", "y", now, time.Second); err == nil {
				t.Errorf("刷盘时租约应仍由本节点持有")
			}
			flushed = append(flushed, collection+":"+strconv.FormatInt(id, 10))
			return nil
		},
	}
	r.Handle(1, func(ctx *handler.Context, body []byte) ([]byte, error) {
		id, err := strconv.ParseInt(string(body), 10, 64)
		if err != nil {
			return nil, err
		}
		ctx.BindPlayer(id)
		return []byte("ok"), nil
	})
	return app, st, &flushed, now
}

func login(t *testing.T, app *App, gate, sid string, player int64) {
	t.Helper()
	_, err := app.Handle(context.Background(), Request{
		GateID: gate, SessionID: sid, ServerID: 1, CmdID: 1,
		Body: []byte(strconv.FormatInt(player, 10)),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRenewMissDoesNotAskRelease(t *testing.T) {
	app, st, _, now := onlineApp(t)
	defer app.Stop()
	login(t, app, "gate-a", "s1", 7)
	var asked atomic.Int32
	app.Hooks.AskRelease = func(context.Context, string, []byte) error {
		asked.Add(1)
		return nil
	}
	key := playerLeaseKey(1, 7)
	if err := lease.Release(context.Background(), st, 1, key, "logic-a", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := lease.Claim(context.Background(), st, 1, key, "logic-b", "g", "s", now, time.Minute); err != nil {
		t.Fatal(err)
	}
	app.RenewLeases(context.Background())
	waitAbandons(t, app)
	if asked.Load() != 0 {
		t.Fatalf("续租失败不应要求对方交出租约: %d", asked.Load())
	}
	if _, err := lease.Claim(context.Background(), st, 1, key, "logic-c", "g", "s2", now, time.Second); err == nil {
		t.Fatal("失败的续租不应把租约抢回来")
	}
	if len(app.Sessions(1)) != 0 {
		t.Fatal("租约已被别人持有时应清掉本地会话")
	}
}

func TestRenewExtendsOwnLease(t *testing.T) {
	app, st, _, start := onlineApp(t)
	defer app.Stop()
	clock := start
	app.Hooks.Now = func() time.Time { return clock }
	login(t, app, "gate-a", "s1", 7)
	clock = start.Add(10 * time.Second)
	app.RenewLeases(context.Background())
	key := playerLeaseKey(1, 7)
	// 登录时的到期点是 start+TTL。若没续上，start+20s 时别的节点已经可以接手。
	if _, err := lease.Claim(context.Background(), st, 1, key, "other", "g", "s", start.Add(20*time.Second), time.Second); err == nil {
		t.Fatal("续租后原到期点之后仍应拒绝别的节点")
	}
}

func TestDropGateCapsConcurrency(t *testing.T) {
	app, _, _, _ := onlineApp(t)
	defer app.Stop()
	const n = 6
	for i := 0; i < n; i++ {
		login(t, app, "gate-a", "s"+strconv.Itoa(i), int64(100+i))
	}
	app.ReapParallel = 2
	var cur, max atomic.Int32
	started := make(chan struct{})
	var once sync.Once
	release := make(chan struct{})
	var releaseOnce sync.Once
	letGo := func() { releaseOnce.Do(func() { close(release) }) }
	defer letGo()
	app.Hooks.FlushID = func(context.Context, int32, string, int64) error {
		c := cur.Add(1)
		for {
			m := max.Load()
			if c <= m || max.CompareAndSwap(m, c) {
				break
			}
		}
		if c == 2 {
			once.Do(func() { close(started) })
		}
		<-release
		cur.Add(-1)
		return nil
	}
	done := make(chan struct{})
	go func() {
		app.ReapGates(context.Background(), nil, 1)
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("清理没有并发起来")
	}
	time.Sleep(30 * time.Millisecond)
	if max.Load() > 2 {
		t.Fatalf("并发超过上限: %d", max.Load())
	}
	letGo()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("清理没有结束")
	}
	if max.Load() != 2 {
		t.Fatalf("应达到并发上限: %d", max.Load())
	}
	if len(app.Sessions(1)) != 0 {
		t.Fatal("会话应清完")
	}
}

func held(st *memory.Store, player int64, now time.Time) bool {
	_, err := lease.Claim(context.Background(), st, 1, playerLeaseKey(1, player), "other", "g", "s", now, time.Second)
	return err != nil
}
