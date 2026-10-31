package logic

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"tuserv/pkg/errcode"
	"tuserv/pkg/handler"
	"tuserv/pkg/owner"
	"tuserv/pkg/protocol"
	"tuserv/pkg/router"
)

func TestClientCmdCannotReachInternalPaths(t *testing.T) {
	app := New(router.New[int32](), nil)
	defer app.Stop()
	var flushed int
	var routed int
	app.Hooks.FlushID = func(context.Context, int32, string, int64) error {
		flushed++
		return nil
	}
	app.Owners = owner.NewLocal(app.Actors)
	if err := owner.Register[int, int](app.Owners, "counter.add", func(context.Context, owner.EntityKey, int) (int, error) {
		return 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	app.Router.Handle(CmdOwner, func(*handler.Context, []byte) ([]byte, error) {
		routed++
		return nil, nil
	})
	body, err := json.Marshal(map[string]any{
		"operation": "counter.add",
		"key":       map[string]any{"server_id": 1, "kind": "counter", "id": 1},
		"args":      1,
		"hops":      1,
	})
	if err != nil {
		t.Fatal(err)
	}
	cases := []Request{
		{CmdID: CmdOwner, Body: body, SessionID: "s", ServerID: 1, PlayerID: 7},
		{CmdID: CmdDisconnect, SessionID: "s", ServerID: 1, PlayerID: 7},
		{CmdID: CmdRelinquish, GateID: InternalCaller, Body: []byte(`{"kind":"player","server_id":1,"id":7}`)},
		{CmdID: -9, SessionID: "s", ServerID: 1, PlayerID: 7},
	}
	for _, req := range cases {
		resp, err := app.Handle(context.Background(), req)
		if err != nil {
			t.Fatalf("cmd %d: 客户端协议错误不应变成节点故障: %v", req.CmdID, err)
		}
		body, uerr := protocol.UnmarshalError(resp.Body)
		if resp.CmdID != protocol.CmdError || uerr != nil || body.Code != errcode.BadArg || body.Detail != ErrClientCmd.Error() || body.ReqCmd != req.CmdID {
			t.Fatalf("cmd %d: 应回错误帧: resp=%+v body=%+v err=%v", req.CmdID, resp, body, uerr)
		}
	}
	if flushed != 0 || routed != 0 || len(app.Sessions(1)) != 0 {
		t.Fatalf("客户端帧碰到了内部路径: flush=%d route=%d sessions=%d", flushed, routed, len(app.Sessions(1)))
	}
}

func TestInternalDisconnectAndRelinquish(t *testing.T) {
	app := New(router.New[int32](), nil)
	defer app.Stop()
	var flushed []string
	app.Hooks.FlushID = func(_ context.Context, _ int32, collection string, id int64) error {
		flushed = append(flushed, collection)
		return nil
	}
	app.Router.Handle(1, func(ctx *handler.Context, _ []byte) ([]byte, error) {
		ctx.BindPlayer(7)
		return []byte("ok"), nil
	})
	if _, err := app.Handle(context.Background(), Request{
		SessionID: "s", ServerID: 1, CmdID: 1, GateID: "g",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Handle(context.Background(), Request{
		Internal: true, CmdID: CmdDisconnect, SessionID: "s", ServerID: 1, PlayerID: 7,
	}); err != nil {
		t.Fatal(err)
	}
	if len(app.Sessions(1)) != 0 {
		t.Fatal("内部断线后会话应消失")
	}
	body, err := json.Marshal(Release{Kind: KindPlayer, ServerID: 1, ID: 7, Gate: "g", Session: "other"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := app.Handle(context.Background(), Request{
		SessionID: "s", ServerID: 1, CmdID: 1, GateID: "g",
	}); err != nil {
		t.Fatal(err)
	}
	before := len(flushed)
	if _, err := app.Handle(context.Background(), Request{
		Internal: true, CmdID: CmdRelinquish, GateID: "not-a-credential", Body: body,
	}); err != nil {
		t.Fatal(err)
	}
	if len(flushed) == before {
		t.Fatal("内部交接应刷盘")
	}
	if _, err := app.Handle(context.Background(), Request{Internal: true, CmdID: 9}); !errors.Is(err, ErrInternalCmd) {
		t.Fatalf("未知内部协议应拒绝: %v", err)
	}
}
