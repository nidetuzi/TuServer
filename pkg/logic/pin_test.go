package logic

import (
	"context"
	"sync/atomic"
	"testing"

	"tuserv/pkg/entity"
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
	playerID, err := entity.WithServer(1, 7)
	if err != nil {
		t.Fatal(err)
	}
	r.Handle(1, func(ctx *handler.Context, _ []byte) ([]byte, error) {
		ctx.BindPlayer(playerID)
		return []byte("ok"), nil
	})
	ctx := context.Background()
	req := Request{SessionID: "s", ServerID: 1, PlayerID: playerID, CmdID: 1, GateID: "g"}
	for i := 0; i < 3; i++ {
		if _, err := app.Handle(ctx, req); err != nil {
			t.Fatal(err)
		}
	}
	if n.Load() != 1 {
		t.Fatalf("在线期间应只钉一次, n=%d", n.Load())
	}
	req.CmdID = CmdDisconnect
	req.Internal = true
	if _, err := app.Handle(ctx, req); err != nil {
		t.Fatal(err)
	}
	if n.Load() != 0 {
		t.Fatalf("断线后应放开, n=%d", n.Load())
	}
}
