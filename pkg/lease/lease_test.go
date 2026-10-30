package lease

import (
	"context"
	"errors"
	"testing"
	"time"

	"tuserv/pkg/store/memory"
)

func TestClaimSingleHolder(t *testing.T) {
	mem := memory.New()
	ctx := context.Background()
	now := time.Unix(1000, 0)
	first, err := Claim(ctx, mem, 1, "player/1/9", "node-a", "g", "s1", now, time.Minute)
	if err != nil || first.Term != 1 || first.Owner != "node-a" {
		t.Fatalf("首次申请失败: %+v %v", first, err)
	}
	renew, err := Claim(ctx, mem, 1, "player/1/9", "node-a", "g", "s1", now.Add(time.Second), time.Minute)
	if err != nil || renew.Term != 1 {
		t.Fatalf("持有者续约不应增加任期: %+v %v", renew, err)
	}
	_, err = Claim(ctx, mem, 1, "player/1/9", "node-b", "g", "s2", now.Add(time.Second), time.Minute)
	if !errors.Is(err, ErrHeld) {
		t.Fatalf("未过期的租约应拒绝第二个节点: %v", err)
	}
	if err := Release(ctx, mem, 1, "player/1/9", "node-a", 1); err != nil {
		t.Fatal(err)
	}
	next, err := Claim(ctx, mem, 1, "player/1/9", "node-b", "g", "s2", now.Add(2*time.Second), time.Minute)
	if err != nil || next.Term != 2 || next.Owner != "node-b" {
		t.Fatalf("释放后应由新节点接手并增加任期: %+v %v", next, err)
	}
}
