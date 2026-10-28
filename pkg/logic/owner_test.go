package logic

import (
	"context"
	"testing"
	"time"

	"tuserv/pkg/actor"
	"tuserv/pkg/owner"
	"tuserv/pkg/router"
)

func TestInternalOwnerDispatch(t *testing.T) {
	app := New(router.New[int32](), nil)
	defer app.Stop()
	resolve := func(context.Context, owner.EntityKey) (string, error) { return "destination", nil }
	app.Owners = owner.New("destination", app.Actors, resolve, nil)
	if err := owner.Register[int, int](app.Owners, "counter.add", func(_ context.Context, _ owner.EntityKey, n int) (int, error) {
		return n + 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	sourceActors := actor.NewSystem()
	defer sourceActors.Stop()
	source := owner.New("source", sourceActors, resolve, func(ctx context.Context, _ string, body []byte) ([]byte, error) {
		resp, err := app.Handle(ctx, Request{CmdID: 0, Body: body})
		return resp.Body, err
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := owner.Call[int](ctx, source, owner.Key(1, "counter", 1), "counter.add", 4)
	if err != nil || got != 5 {
		t.Fatalf("内部调用应由实体邮箱执行: got=%d err=%v", got, err)
	}
	if len(app.Sessions(1)) != 0 {
		t.Fatal("实体内部调用不应创建玩家会话")
	}
}

func TestInternalWithoutOwnersFails(t *testing.T) {
	app := New(router.New[int32](), nil)
	defer app.Stop()
	if _, err := app.Handle(context.Background(), Request{CmdID: 0}); err == nil {
		t.Fatal("没有主人调用器时不能把内部消息当作客户端协议")
	}
}
