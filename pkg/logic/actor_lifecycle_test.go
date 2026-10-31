package logic

import (
	"context"
	"fmt"
	"testing"
	"time"

	"tuserv/pkg/actor"
	"tuserv/pkg/router"
)

func TestDisconnectRetiresSessionActors(t *testing.T) {
	app := New(router.New[int32](), nil)
	app.Actors.Stop()
	app.Actors = actor.NewSystem(actor.WithIdleTimeout(0))
	defer app.Stop()
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("session-%d", i)
		if err := app.Actors.Do(context.Background(), "session/"+id, func() {}); err != nil {
			t.Fatal(err)
		}
		if err := app.onDisconnect(context.Background(), Request{SessionID: id}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for app.Actors.Len() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if n := app.Actors.Len(); n != 0 {
		t.Fatalf("断线后会话邮箱未回收: %d", n)
	}
}
