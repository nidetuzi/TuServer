package owner

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"tuserv/pkg/actor"
	"tuserv/pkg/lease"
	"tuserv/pkg/store/memory"
)

func TestSplitBrainClaimAllowsOneWriter(t *testing.T) {
	mem := memory.New()
	var runs atomic.Int32
	mk := func(self string) *Runtime {
		rt := New(self, actor.NewSystem(), func(context.Context, EntityKey) (string, error) { return self, nil }, nil)
		rt.Claim = func(ctx context.Context, key EntityKey) (uint64, error) {
			rec, err := lease.Claim(ctx, mem, key.ServerID, key.String(), self, "", "", time.Now(), time.Minute)
			if err != nil {
				return 0, NewError("owner.lease", err.Error())
			}
			return rec.Term, nil
		}
		if err := Register(rt, "op", func(context.Context, EntityKey, int) (int, error) {
			runs.Add(1)
			return 1, nil
		}); err != nil {
			t.Fatal(err)
		}
		return rt
	}
	a := mk("node-a")
	b := mk("node-b")
	defer a.actors.Stop()
	defer b.actors.Stop()
	if _, err := Call[int](context.Background(), a, Key(1, "guild", 9), "op", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := Call[int](context.Background(), b, Key(1, "guild", 9), "op", 1); err == nil {
		t.Fatal("发现结果不一致时第二个节点不应写入")
	}
	if runs.Load() != 1 {
		t.Fatalf("操作应只执行一次: %d", runs.Load())
	}
}
