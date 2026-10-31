package logic

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"tuserv/pkg/entity"
	"tuserv/pkg/handler"
	"tuserv/pkg/router"
)

func TestPlayerActorSerializesAndKicksOldSession(t *testing.T) {
	r := router.New[int32]()
	app := New(r, nil)
	defer app.Stop()
	var kicks []string
	var mu sync.Mutex
	app.Hooks.Kick = func(_ context.Context, _, sessionID string) {
		mu.Lock()
		kicks = append(kicks, sessionID)
		mu.Unlock()
	}
	playerID, err := entity.WithServer(1, 42)
	if err != nil {
		t.Fatal(err)
	}
	r.Handle(1, func(ctx *handler.Context, _ []byte) ([]byte, error) {
		ctx.BindPlayer(playerID)
		return []byte("ok"), nil
	})
	if _, err := app.Handle(context.Background(), Request{SessionID: "a", ServerID: 1, CmdID: 1, GateID: "g"}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Handle(context.Background(), Request{SessionID: "b", ServerID: 1, CmdID: 1, GateID: "g"}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if len(kicks) != 1 || kicks[0] != "a" {
		t.Fatalf("新登录应踢掉旧会话: %v", kicks)
	}
	mu.Unlock()

	var ran atomic.Bool
	r.Handle(2, func(*handler.Context, []byte) ([]byte, error) {
		ran.Store(true)
		return []byte("no"), nil
	})
	if _, err := app.Handle(context.Background(), Request{SessionID: "a", ServerID: 1, PlayerID: playerID, CmdID: 2, GateID: "g"}); err != nil {
		t.Fatal(err)
	}
	if ran.Load() {
		t.Fatal("旧会话不应再改这个角色")
	}

	var current atomic.Int32
	r.Handle(3, func(*handler.Context, []byte) ([]byte, error) {
		if current.Add(1) != 1 {
			t.Error("同一角色的两个请求重叠执行了")
		}
		time.Sleep(20 * time.Millisecond)
		current.Add(-1)
		return []byte("ok"), nil
	})
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = app.Handle(context.Background(), Request{SessionID: "b", ServerID: 1, PlayerID: playerID, CmdID: 3, GateID: "g"})
		}()
	}
	wg.Wait()
}
