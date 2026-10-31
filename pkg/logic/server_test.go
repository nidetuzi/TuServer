package logic

import (
	"context"
	"testing"

	"tuserv/pkg/entity"
	"tuserv/pkg/errcode"
	"tuserv/pkg/handler"
	"tuserv/pkg/protocol"
	"tuserv/pkg/router"
	"tuserv/pkg/store"
	"tuserv/pkg/store/memory"
)

type countingLocker struct {
	store.Locker
	n int
}

func (c *countingLocker) MutateLease(ctx context.Context, serverID int32, key string, fn func(store.LeaseRec) (store.LeaseRec, error)) (store.LeaseRec, error) {
	c.n++
	return c.Locker.MutateLease(ctx, serverID, key, fn)
}

// TestRejectMismatchedServer 保证已登录请求不能凭帧头换区服去申领租约。
func TestRejectMismatchedServer(t *testing.T) {
	leases := &countingLocker{Locker: memory.New()}
	r := router.New[int32]()
	var hits int
	r.Handle(7, func(*handler.Context, []byte) ([]byte, error) {
		hits++
		return []byte("ok"), nil
	})
	app := New(r, nil)
	defer app.Stop()
	app.Hooks.Leases = leases
	app.Hooks.Self = "logic-a"

	playerID, err := entity.WithServer(1, 7)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := app.Handle(context.Background(), Request{
		SessionID: "s", ServerID: 4, PlayerID: playerID, CmdID: 7, GateID: "g",
	})
	if err != nil {
		t.Fatal(err)
	}
	fault := mustFault(t, resp)
	if fault.Code != errcode.BadArg || fault.ReqCmd != 7 || fault.Detail != ErrServerMismatch.Error() || resp.PlayerID != 0 {
		t.Fatalf("应回错误帧且不绑定角色: %+v resp=%+v", fault, resp)
	}
	if hits != 0 || leases.n != 0 || len(app.Sessions(4)) != 0 || len(app.Sessions(1)) != 0 {
		t.Fatalf("不应进入玩法或申领租约: hits=%d claims=%d", hits, leases.n)
	}

	resp, err = app.Handle(context.Background(), Request{
		SessionID: "s", ServerID: 1, PlayerID: playerID, CmdID: 7, GateID: "g",
	})
	if err != nil || resp.CmdID == protocol.CmdError || string(resp.Body) != "ok" || resp.PlayerID != playerID {
		t.Fatalf("服号一致时应正常执行: %+v %v", resp, err)
	}
	if hits != 1 || leases.n == 0 || len(app.Sessions(1)) != 1 {
		t.Fatalf("一致的请求应申领租约并挂上会话: hits=%d claims=%d sessions=%d", hits, leases.n, len(app.Sessions(1)))
	}
}
