package logic

import (
	"context"
	"sync/atomic"
	"testing"

	"tuserv/pkg/handler"
	"tuserv/pkg/router"
)

func TestPinPlayerOnceUntilDisconnect(t *testing.T) {
	r := router.New[int32]()
	app := New(r, nil)
	defer app.Stop()
	var n atomic.Int32
	app.Hooks.PinPlayer = func(int32, int64) func() {
		n.Add(1)
		return func() { n.Add(-1) }
	}
	r.Handle(1, func(ctx *handler.Context, _ []byte) ([]byte, error) {
		ctx.BindPlayer(7)
		return []byte("ok"), nil
	})
	ctx := context.Background()
	req := Request{SessionID: "s", ServerID: 1, PlayerID: 7, CmdID: 1, GateID: "g"}
	for i := 0; i < 3; i++ {
		if _, err := app.Handle(ctx, req); err != nil {
			t.Fatal(err)
		}
	}
	if n.Load() != 1 {
		t.Fatalf("在线期间应只钉一次, n=%d", n.Load())
	}
	req.CmdID = -1
	if _, err := app.Handle(ctx, req); err != nil {
		t.Fatal(err)
	}
	if n.Load() != 0 {
		t.Fatalf("断线后应放开, n=%d", n.Load())
	}
}
