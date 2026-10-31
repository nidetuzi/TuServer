package lease

import (
	"context"
	"errors"
	"testing"
	"time"

	"tuserv/pkg/store"
	"tuserv/pkg/store/memory"
)

func TestRenewOwnReportsMisses(t *testing.T) {
	mem := memory.New()
	ctx := context.Background()
	now := time.Unix(2000, 0)
	if _, err := Claim(ctx, mem, 1, "player/1/1", "node-a", "g", "s1", now, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := Claim(ctx, mem, 1, "player/1/2", "node-b", "g", "s2", now, time.Minute); err != nil {
		t.Fatal(err)
	}
	until := now.Add(2 * time.Minute).UnixMilli()
	misses, err := RenewOwn(ctx, mem, []store.LeaseTouch{
		{ServerID: 1, Key: "player/1/1", Owner: "node-a", Term: 1, Until: until},
		{ServerID: 1, Key: "player/1/2", Owner: "node-a", Term: 1, Until: until},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(misses) != 1 || misses[0].Key != "player/1/2" || !errors.Is(misses[0].Err, ErrHeld) {
		t.Fatalf("只应报告不属于自己的那条: %+v", misses)
	}
	var held *HeldError
	if !errors.As(misses[0].Err, &held) || held.Owner != "node-b" {
		t.Fatalf("应带回当前持有者: %+v", misses[0].Err)
	}
	_, err = Claim(ctx, mem, 1, "player/1/2", "node-c", "g", "s3", now.Add(time.Second), time.Minute)
	if !errors.Is(err, ErrHeld) {
		t.Fatalf("失败的续期不应抢走 node-b 的租约: %v", err)
	}
	if _, err := mem.MutateLease(ctx, 1, "player/1/1", func(cur store.LeaseRec) (store.LeaseRec, error) {
		cur.Term++
		return cur, nil
	}); err != nil {
		t.Fatal(err)
	}
	termMiss, err := RenewOwn(ctx, mem, []store.LeaseTouch{
		{ServerID: 1, Key: "player/1/1", Owner: "node-a", Term: 1, Until: until + 1},
	})
	if err != nil || len(termMiss) != 1 || !errors.Is(termMiss[0].Err, errTermChanged) || errors.Is(termMiss[0].Err, ErrHeld) {
		t.Fatalf("任期变化不能当成被别人持有: %+v %v", termMiss, err)
	}
}

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

// TestRenewDoesNotPreempt 保证续约只延长本节点当前任期，空闲和别人的租约都保持原样。
func TestRenewDoesNotPreempt(t *testing.T) {
	mem := memory.New()
	ctx := context.Background()
	now := time.Unix(1000, 0)
	key := "player/1/9"

	missing, err := Renew(ctx, mem, 1, "missing", "node-a", 1, now, time.Minute)
	if !errors.Is(err, ErrHeld) || missing.Found {
		t.Fatalf("没有的租约不能被续出来: %+v %v", missing, err)
	}

	first, err := Claim(ctx, mem, 1, key, "node-a", "g", "s1", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	renewed, err := Renew(ctx, mem, 1, key, "node-a", first.Term, now.Add(time.Second), time.Minute)
	if err != nil || renewed.Term != first.Term || renewed.Owner != "node-a" {
		t.Fatalf("持有者续约不应改任期: %+v %v", renewed, err)
	}
	wantUntil := now.Add(time.Second).Add(time.Minute).UnixMilli()
	if renewed.Until != wantUntil {
		t.Fatalf("续约没有延长到期时间: %d", renewed.Until)
	}

	held, err := Renew(ctx, mem, 1, key, "node-b", first.Term, now.Add(time.Second), time.Minute)
	if !errors.Is(err, ErrHeld) || held.Owner != "node-a" || held.Until != wantUntil {
		t.Fatalf("别人不能靠续约抢占: %+v %v", held, err)
	}

	mismatch, err := Renew(ctx, mem, 1, key, "node-a", first.Term+1, now.Add(2*time.Second), time.Minute)
	if err == nil || errors.Is(err, ErrHeld) || mismatch.Until != wantUntil || mismatch.Term != first.Term {
		t.Fatalf("任期不符应拒绝且不改库: %+v %v", mismatch, err)
	}

	// 已经过期但还没被别人接走时，持有者和任期都没变，仍然只续期。
	expiredNow := now.Add(time.Hour)
	again, err := Renew(ctx, mem, 1, key, "node-a", first.Term, expiredNow, time.Minute)
	if err != nil || again.Term != first.Term || again.Owner != "node-a" {
		t.Fatalf("自己的过期租约仍应续上: %+v %v", again, err)
	}

	if err := Release(ctx, mem, 1, key, "node-a", first.Term); err != nil {
		t.Fatal(err)
	}
	released, err := Renew(ctx, mem, 1, key, "node-a", first.Term, expiredNow, time.Minute)
	if !errors.Is(err, ErrHeld) || released.Owner != "" {
		t.Fatalf("释放后不能靠续约接手: %+v %v", released, err)
	}
	next, err := Claim(ctx, mem, 1, key, "node-b", "g", "s2", expiredNow, time.Minute)
	if err != nil || next.Owner != "node-b" || next.Term != first.Term+1 {
		t.Fatalf("释放后应由别人接手并增加任期: %+v %v", next, err)
	}
	stolen, err := Renew(ctx, mem, 1, key, "node-a", first.Term, expiredNow, time.Minute)
	if !errors.Is(err, ErrHeld) || stolen.Owner != "node-b" || stolen.Term != next.Term || stolen.Until != next.Until {
		t.Fatalf("旧任期不能把别人的租约续回来: %+v %v", stolen, err)
	}
}
