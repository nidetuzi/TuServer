package logic

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"tuserv/pkg/errcode"
	"tuserv/pkg/handler"
	"tuserv/pkg/owner"
	"tuserv/pkg/protocol"
	"tuserv/pkg/router"
	"tuserv/pkg/store"
)

func TestBusinessErrorBecomesFrame(t *testing.T) {
	r := router.New[int32]()
	app := New(r, nil)
	defer app.Stop()
	r.Handle(1, func(ctx *handler.Context, _ []byte) ([]byte, error) {
		ctx.BindPlayer(7)
		return []byte("ok"), nil
	})
	resp, err := app.Handle(context.Background(), Request{SessionID: "s", ServerID: 1, CmdID: 1, GateID: "g"})
	if err != nil || resp.PlayerID != 7 || string(resp.Body) != "ok" {
		t.Fatalf("登录应仍是成功包: %+v %v", resp, err)
	}
	if len(app.Sessions(1)) != 1 {
		t.Fatal("登录成功应登记会话")
	}

	r.Handle(2, func(*handler.Context, []byte) ([]byte, error) {
		return nil, errcode.New(errcode.NotEnough, "service: not enough items")
	})
	resp, err = app.Handle(context.Background(), Request{SessionID: "s", ServerID: 1, PlayerID: 7, CmdID: 2, GateID: "g"})
	if err != nil {
		t.Fatal(err)
	}
	fault := mustFault(t, resp)
	if fault.Code != errcode.NotEnough || fault.ReqCmd != 2 || fault.Detail != "service: not enough items" {
		t.Fatalf("道具不足应回错误码: %+v", fault)
	}
	if len(app.Sessions(1)) != 1 {
		t.Fatal("业务错误不应摘掉在线会话")
	}

	resp, err = app.Handle(context.Background(), Request{SessionID: "other", ServerID: 1, CmdID: 404, GateID: "g"})
	if err != nil {
		t.Fatal(err)
	}
	fault = mustFault(t, resp)
	if fault.Code != errcode.NoHandler || fault.Detail != "router.no_handler" || fault.ReqCmd != 404 {
		t.Fatalf("找不到路由应回错误码: %+v", fault)
	}
	if len(app.Sessions(1)) != 1 {
		t.Fatal("失败请求不应登记新会话")
	}

	router.Register(r, int32(3), func(*handler.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return &wrapperspb.StringValue{Value: "x"}, nil
	})
	resp, err = app.Handle(context.Background(), Request{SessionID: "s", ServerID: 1, PlayerID: 7, CmdID: 3, Body: []byte{0xff}, GateID: "g"})
	if err != nil {
		t.Fatal(err)
	}
	if fault = mustFault(t, resp); fault.Code != errcode.BadArg || fault.Detail != "router.bad_message" {
		t.Fatalf("坏 protobuf 应回参数错误: %+v", fault)
	}

	r.Handle(4, func(ctx *handler.Context, _ []byte) ([]byte, error) {
		_, err := ctx.GetPlayer()
		return nil, err
	})
	resp, err = app.Handle(context.Background(), Request{SessionID: "fresh", ServerID: 1, CmdID: 4, GateID: "g"})
	if err != nil {
		t.Fatal(err)
	}
	if fault = mustFault(t, resp); fault.Code != errcode.NotLoggedIn {
		t.Fatalf("未登录应回错误码: %+v", fault)
	}

	r.Handle(5, func(*handler.Context, []byte) ([]byte, error) {
		return nil, owner.NewError("guild.not_found", "missing guild 123")
	})
	resp, err = app.Handle(context.Background(), Request{SessionID: "s", ServerID: 1, PlayerID: 7, CmdID: 5, GateID: "g"})
	if err != nil {
		t.Fatal(err)
	}
	if fault = mustFault(t, resp); fault.Code != errcode.NotFound || fault.Detail != "guild.not_found" {
		t.Fatalf("主人业务错误应回错误码而不是诊断原文: %+v", fault)
	}

	r.Handle(6, func(*handler.Context, []byte) ([]byte, error) {
		return nil, owner.NewError("guild.full", "full")
	})
	resp, err = app.Handle(context.Background(), Request{SessionID: "s", ServerID: 1, PlayerID: 7, CmdID: 6, GateID: "g"})
	if err != nil {
		t.Fatal(err)
	}
	if fault = mustFault(t, resp); fault.Code != errcode.Rejected || fault.Detail != "guild.full" {
		t.Fatalf("未约定的主人错误应回 Rejected: %+v", fault)
	}

	r.Handle(7, func(*handler.Context, []byte) ([]byte, error) {
		return nil, owner.NewError("owner.panic", "boom")
	})
	resp, err = app.Handle(context.Background(), Request{SessionID: "s", ServerID: 1, PlayerID: 7, CmdID: 7, GateID: "g"})
	if err != nil {
		t.Fatal(err)
	}
	if fault = mustFault(t, resp); fault.Code != errcode.Internal || fault.Detail != "owner.panic" {
		t.Fatalf("主人内部错误应回内部码: %+v", fault)
	}

	r.Handle(8, func(*handler.Context, []byte) ([]byte, error) {
		return nil, errors.New("mongo timeout secret-host")
	})
	resp, err = app.Handle(context.Background(), Request{SessionID: "s", ServerID: 1, PlayerID: 7, CmdID: 8, GateID: "g"})
	if err != nil {
		t.Fatal(err)
	}
	if fault = mustFault(t, resp); fault.Code != errcode.Internal || fault.Detail != "logic.internal" {
		t.Fatalf("未分类错误不应把原文回给客户端: %+v", fault)
	}
}

func TestHandlerPanicReturnsInternalCode(t *testing.T) {
	r := router.New[int32]()
	app := New(r, nil)
	defer app.Stop()
	r.Handle(1, func(*handler.Context, []byte) ([]byte, error) {
		panic("secret boom")
	})
	resp, err := app.Handle(context.Background(), Request{SessionID: "s", ServerID: 1, CmdID: 1, GateID: "g"})
	if err != nil {
		t.Fatal(err)
	}
	fault := mustFault(t, resp)
	if fault.Code != errcode.Internal || fault.Detail != "logic.panic" || fault.ReqCmd != 1 {
		t.Fatalf("panic 应回内部错误码: %+v", fault)
	}
	r.Handle(2, func(*handler.Context, []byte) ([]byte, error) {
		return []byte("ok"), nil
	})
	resp, err = app.Handle(context.Background(), Request{SessionID: "s", ServerID: 1, CmdID: 2, GateID: "g"})
	if err != nil || resp.CmdID != 2 || string(resp.Body) != "ok" {
		t.Fatalf("panic 之后邮箱应还能处理下一条: %+v %v", resp, err)
	}
}

func TestNodeFailureStaysRPCError(t *testing.T) {
	r := router.New[int32]()
	app := New(r, nil)
	defer app.Stop()
	r.Handle(1, func(*handler.Context, []byte) ([]byte, error) {
		t.Fatal("租约失败不应进入玩法")
		return nil, nil
	})
	app.Hooks.Self = "logic-a"
	app.Hooks.Leases = failLock{}
	resp, err := app.Handle(context.Background(), Request{SessionID: "s", ServerID: 1, PlayerID: 9, CmdID: 1, GateID: "g"})
	if err == nil || resp.CmdID == protocol.CmdError {
		t.Fatalf("租约故障应作为节点错误返回: %+v %v", resp, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	app.Hooks.Leases = nil
	resp, err = app.Handle(ctx, Request{SessionID: "s", ServerID: 1, CmdID: 1, GateID: "g"})
	if err == nil || resp.CmdID == protocol.CmdError {
		t.Fatalf("取消的调用应作为节点错误返回: %+v %v", resp, err)
	}

	app.Stop()
	resp, err = app.Handle(context.Background(), Request{SessionID: "s", ServerID: 1, CmdID: 1, GateID: "g"})
	if err == nil || resp.CmdID == protocol.CmdError {
		t.Fatalf("节点停机应作为节点错误返回: %+v %v", resp, err)
	}
}

type failLock struct{}

func (failLock) MutateLease(context.Context, int32, string, func(store.LeaseRec) (store.LeaseRec, error)) (store.LeaseRec, error) {
	return store.LeaseRec{}, errors.New("lease down")
}

func mustFault(t *testing.T, resp Response) protocol.ErrorBody {
	t.Helper()
	if resp.CmdID != protocol.CmdError {
		t.Fatalf("应回统一错误包: %+v", resp)
	}
	body, err := protocol.UnmarshalError(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if body.Code == 0 {
		t.Fatalf("错误包的错误码不能是 0: %+v", body)
	}
	return body
}
