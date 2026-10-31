package logic

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"

	"tuserv/pkg/entity"
)

func TestDisconnectZeroPlayerIDReleasesBinding(t *testing.T) {
	app, st, flushed, now := onlineApp(t)
	defer app.Stop()
	var pins atomic.Int32
	app.Hooks.PinPlayer = func(int32, int64) func() {
		pins.Add(1)
		return func() { pins.Add(-1) }
	}
	playerID := playerOnServer(t, 1, 7)
	login(t, app, "gate-a", "s1", playerID)
	if pins.Load() != 1 || !held(st, playerID, now) {
		t.Fatalf("登录后应钉住并持有租约: pins=%d", pins.Load())
	}
	// 网关还没写回 playerID，区服号也可能是旧帧上的值。清理必须跟绑定走，不能跟请求里的 0。
	if _, err := app.Handle(context.Background(), Request{
		Internal: true, CmdID: CmdDisconnect,
		GateID: "gate-a", SessionID: "s1", ServerID: 9, PlayerID: 0,
	}); err != nil {
		t.Fatal(err)
	}
	if len(app.Sessions(1)) != 0 {
		t.Fatal("断线后会话应删除")
	}
	if len(*flushed) != 1 || (*flushed)[0] != "player:"+strconv.FormatInt(playerID, 10) {
		t.Fatalf("应按绑定角色刷盘: %v", *flushed)
	}
	if held(st, playerID, now) {
		t.Fatal("租约应释放")
	}
	if pins.Load() != 0 {
		t.Fatalf("刷盘后应放开缓存钉住: %d", pins.Load())
	}
	app.RenewLeases(context.Background())
	if held(st, playerID, now) {
		t.Fatal("绑定已清掉，续租不应再把角色占住")
	}
	if _, err := app.Handle(context.Background(), Request{
		Internal: true, CmdID: CmdDisconnect,
		GateID: "gate-a", SessionID: "s1", PlayerID: 0,
	}); err != nil {
		t.Fatal(err)
	}
	if len(*flushed) != 1 {
		t.Fatalf("重复断线不应再刷盘: %v", *flushed)
	}
}

func TestDisconnectZeroLeavesUnknownAndMoved(t *testing.T) {
	app, st, flushed, now := onlineApp(t)
	defer app.Stop()
	var pins atomic.Int32
	app.Hooks.PinPlayer = func(int32, int64) func() {
		pins.Add(1)
		return func() { pins.Add(-1) }
	}
	if _, err := app.Handle(context.Background(), Request{
		Internal: true, CmdID: CmdDisconnect,
		GateID: "gate-a", SessionID: "missing", ServerID: 1, PlayerID: 0,
	}); err != nil {
		t.Fatal(err)
	}
	if len(*flushed) != 0 {
		t.Fatalf("没有绑定不应刷盘: %v", *flushed)
	}
	playerID := playerOnServer(t, 1, 7)
	login(t, app, "gate-a", "s1", playerID)
	login(t, app, "gate-b", "s2", playerID)
	before := len(*flushed)
	if _, err := app.Handle(context.Background(), Request{
		Internal: true, CmdID: CmdDisconnect,
		GateID: "gate-a", SessionID: "s1", ServerID: 1, PlayerID: 0,
	}); err != nil {
		t.Fatal(err)
	}
	if len(*flushed) != before {
		t.Fatal("角色已经换到新会话，旧连接的空编号断线不应刷盘")
	}
	got := app.Sessions(1)
	if len(got) != 1 || got[0].ID != "s2" || got[0].PlayerID != playerID {
		t.Fatalf("新会话应保留: %+v", got)
	}
	if !held(st, playerID, now) {
		t.Fatal("新会话的租约应还在")
	}
	if pins.Load() != 1 {
		t.Fatalf("换会话后钉住应留着: %d", pins.Load())
	}
	if _, err := app.Handle(context.Background(), Request{
		Internal: true, CmdID: CmdDisconnect,
		GateID: "gate-x", SessionID: "s2", ServerID: 1, PlayerID: 0,
	}); err != nil {
		t.Fatal(err)
	}
	if len(*flushed) != before || len(app.Sessions(1)) != 1 || !held(st, playerID, now) || pins.Load() != 1 {
		t.Fatal("别的网关送来的空编号断线不能清当前绑定")
	}
}

func playerOnServer(t *testing.T, serverID int32, seq int64) int64 {
	t.Helper()
	id, err := entity.WithServer(serverID, seq)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
