package main

import (
	"context"
	"testing"

	node "tuserv/kitex_gen/node"
	"tuserv/pkg/cluster"
	"tuserv/pkg/logic"
	"tuserv/pkg/owner"
	"tuserv/pkg/protocol"
	"tuserv/pkg/router"
)

func TestDispatchCopiesInternalFlag(t *testing.T) {
	app := logic.New(router.New[int32](), nil)
	defer app.Stop()
	app.Owners = owner.NewLocal(app.Actors)
	var ran bool
	if err := owner.Register[int, int](app.Owners, "counter.add", func(context.Context, owner.EntityKey, int) (int, error) {
		ran = true
		return 2, nil
	}); err != nil {
		t.Fatal(err)
	}
	api := &logicAPI{app: app}
	body := []byte(`{"operation":"counter.add","key":{"server_id":1,"kind":"counter","id":1},"args":1,"hops":1}`)
	resp, err := api.Dispatch(context.Background(), &node.DispatchReq{CmdId: 0, Body: body})
	if err != nil || resp.GetCmdId() != protocol.CmdError || ran {
		t.Fatalf("未验签的 cmd 0 应回错误帧且不进主人调用: resp=%+v err=%v ran=%v", resp, err, ran)
	}
	ctx := cluster.WithInternalCall(context.Background(), true)
	resp, err = api.Dispatch(ctx, &node.DispatchReq{CmdId: logic.CmdOwner, Body: body})
	if err != nil || !ran || len(resp.GetBody()) == 0 {
		t.Fatalf("验签后的内部调用应执行: resp=%+v err=%v ran=%v", resp, err, ran)
	}
}
