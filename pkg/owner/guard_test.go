package owner

import (
	"context"
	"sync/atomic"
	"testing"

	"tuserv/pkg/actor"
)

func TestGuardWrapsOwnerTurn(t *testing.T) {
	actors := actor.NewSystem()
	defer actors.Stop()
	rt := NewLocal(actors)
	var depth atomic.Int32
	rt.Guard = func() func() {
		depth.Add(1)
		return func() { depth.Add(-1) }
	}
	if err := Register(rt, "nop", func(ctx context.Context, _ EntityKey, _ struct{}) (struct{}, error) {
		if depth.Load() != 1 {
			t.Errorf("主人操作应在 Guard 内, depth=%d", depth.Load())
		}
		return struct{}{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := Call[struct{}](context.Background(), rt, Key(1, "guild", 1), "nop", struct{}{}); err != nil {
		t.Fatal(err)
	}
	if depth.Load() != 0 {
		t.Fatalf("操作结束后应放开 Guard, depth=%d", depth.Load())
	}
}
