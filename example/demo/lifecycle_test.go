package demo

import (
	"context"
	"strconv"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"tuserv/example/demo/entity"
	"tuserv/example/demo/gen/game"
	demohandler "tuserv/example/demo/handler"
	"tuserv/example/demo/service"
	"tuserv/pkg/cache"
	"tuserv/pkg/event"
	"tuserv/pkg/handler"
	"tuserv/pkg/logic"
	"tuserv/pkg/router"
	"tuserv/pkg/scheduler"
	"tuserv/pkg/store/memory"
)

func TestPlayerDailyLifecycle(t *testing.T) {
	ctx := context.Background()
	loc := time.FixedZone("Asia/Shanghai", 8*60*60)
	now := time.Date(2026, 10, 31, 23, 59, 0, 0, loc)
	mem := memory.New()
	c := cache.New(mem)
	players := service.NewPlayerService(c, mem, event.New()).WithDayClock(loc, func() time.Time { return now })
	r := router.New[int32]()
	router.Register(r, demohandler.CmdLogin, demohandler.OnLogin(players))
	app := logic.New(r, players.GetByID)
	app.Scheduler, _ = scheduler.New(scheduler.Config{})
	app.Hooks.Guard = c.Guard
	app.Hooks.FlushID = func(ctx context.Context, serverID int32, collection string, id int64) error {
		e, err := c.Get(ctx, serverID, collection, strconv.FormatInt(id, 10), nil)
		if err != nil {
			return err
		}
		if err := c.FlushEntity(ctx, e); err != nil {
			return err
		}
		c.Forget(e)
		return nil
	}
	BindPlayerLifecycle(app, players)
	defer app.Stop()
	body, _ := proto.Marshal(&game.LoginReq{Name: "daily"})
	login := func(session string) int64 {
		t.Helper()
		resp, err := app.Handle(ctx, logic.Request{GateID: "gate", SessionID: session, ServerID: 1, CmdID: demohandler.CmdLogin, Body: body})
		if err != nil {
			t.Fatal(err)
		}
		return resp.PlayerID
	}
	id := login("one")
	get := func() *entity.Player {
		t.Helper()
		v, err := players.GetByID(ctx, 1, id)
		if err != nil {
			t.Fatal(err)
		}
		return v.(*entity.Player)
	}
	firstLogin := now.Unix()
	if get().Snapshot().LastLoginAt != firstLogin {
		t.Fatal("首次登录未记录登录时间")
	}
	if entries := app.Scheduler.Entries(); len(entries) != 1 || entries[0].Spec != "@every 30s" {
		t.Fatalf("在线检查未注册: %+v", entries)
	}
	hctx := handler.New(ctx, handler.Meta{ServerID: 1, PlayerID: id}).WithLoader(players.GetByID)
	if _, err := players.AddExp(hctx, 10); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	// 与在线任务使用完全相同的业务回调；固定时钟不必等待真实午夜。
	if err := app.Actors.Do(ctx, "player/1/"+strconv.FormatInt(id, 10), func() {
		crossed, err := players.CheckNewDay(hctx)
		if err != nil || !crossed {
			t.Errorf("在线跨天失败: %v %v", crossed, err)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if view := get().Snapshot(); view.LastLoginAt != firstLogin || view.Exp != 10 {
		t.Fatal("在线跨天不应改写登录时间或经验")
	}
	if crossed, err := players.CheckNewDay(hctx); err != nil || crossed {
		t.Fatalf("同日在线检查重复触发: %v %v", crossed, err)
	}
	if _, err := players.AddExp(hctx, 7); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Handle(ctx, logic.Request{Internal: true, CmdID: logic.CmdDisconnect, GateID: "gate", SessionID: "one", ServerID: 1, PlayerID: id}); err != nil {
		t.Fatal(err)
	}
	if len(app.Scheduler.Entries()) != 0 {
		t.Fatal("离线任务残留")
	}
	if view := get().Snapshot(); view.LastOfflineAt != now.Unix() {
		t.Fatalf("离线时间未保存: %+v", view)
	}
	login("two")
	if view := get().Snapshot(); view.LastLoginAt != now.Unix() || view.Exp != 17 {
		t.Fatal("重登未更新登录时间或错误修改经验")
	}
	if _, err := app.Handle(ctx, logic.Request{Internal: true, CmdID: logic.CmdDisconnect, GateID: "gate", SessionID: "two", ServerID: 1, PlayerID: id}); err != nil {
		t.Fatal(err)
	}
	now = now.AddDate(0, 0, 2)
	var loginCrossed bool
	app.Hooks.OnPlayerOnline = func(ctx *handler.Context) error {
		var err error
		loginCrossed, err = players.CheckNewDay(ctx)
		if err != nil {
			return err
		}
		return players.OnOnline(ctx)
	}
	login("three")
	if view := get().Snapshot(); !loginCrossed || view.LastLoginAt != now.Unix() || view.Exp != 17 {
		t.Fatalf("离线跨天或登录时间错误: %+v crossed=%v", view, loginCrossed)
	}
}
