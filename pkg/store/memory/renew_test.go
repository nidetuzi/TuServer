package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"tuserv/pkg/lease"
	"tuserv/pkg/store"
)

func TestRenewManySplitsHitAndMiss(t *testing.T) {
	s := New()
	ctx := context.Background()
	now := time.UnixMilli(5_000)
	const (
		own  = "player/1/7"
		gone = "player/1/8"
		miss = "player/1/9"
	)
	if _, err := lease.Claim(ctx, s, 1, own, "node-a", "gate-a", "s1", now, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := lease.Claim(ctx, s, 1, gone, "node-b", "gate-b", "s2", now, time.Minute); err != nil {
		t.Fatal(err)
	}
	before := peekLease(t, s, 1, gone)
	until := now.Add(2 * time.Minute).UnixMilli()
	misses, err := s.RenewMany(ctx, []store.LeaseTouch{
		{ServerID: 1, Key: own, Owner: "node-a", Term: 1, Until: until},
		{ServerID: 1, Key: gone, Owner: "node-a", Term: 1, Until: until},
		{ServerID: 1, Key: own, Owner: "node-a", Term: 9, Until: until},
		{ServerID: 1, Key: miss, Owner: "node-a", Term: 1, Until: until},
		{ServerID: 2, Key: own, Owner: "node-a", Term: 1, Until: until},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(misses) != 4 {
		t.Fatalf("应有 4 条没续上: %+v", misses)
	}
	kind := map[string]error{}
	for _, m := range misses {
		kind[m.Key+"@"+itoa(m.ServerID)] = m.Err
		if m.Key == gone && m.Owner != "node-b" {
			t.Fatalf("被别人持有时应带回当前持有者: %+v", m)
		}
		if m.Key == miss && m.Owner != "" {
			t.Fatalf("不存在的租约没有持有者: %+v", m)
		}
	}
	if !errors.Is(kind[own+"@1"], store.ErrLeaseTerm) {
		t.Fatalf("同一持有者任期不同不能当成被抢走: %v", kind[own+"@1"])
	}
	if !errors.Is(kind[gone+"@1"], store.ErrLeaseHeld) || !errors.Is(kind[miss+"@1"], store.ErrLeaseHeld) || !errors.Is(kind[own+"@2"], store.ErrLeaseHeld) {
		t.Fatalf("别人的、没有的、别的区服应是 ErrLeaseHeld: %+v", misses)
	}
	held := peekLease(t, s, 1, own)
	if held.Owner != "node-a" || held.Term != 1 || held.Until != until || held.Gate != "gate-a" || held.Session != "s1" {
		t.Fatalf("成功的续期只应延长到期时间: %+v", held)
	}
	after := peekLease(t, s, 1, gone)
	if after != before {
		t.Fatalf("别人的租约不应被改写: before=%+v after=%+v", before, after)
	}
	if _, ok := s.leases[itoa(1)+"|"+miss]; ok {
		t.Fatal("不存在的租约不应被插入")
	}
}

func peekLease(t *testing.T, s *Store, serverID int32, key string) store.LeaseRec {
	t.Helper()
	var cur store.LeaseRec
	_, err := s.MutateLease(context.Background(), serverID, key, func(got store.LeaseRec) (store.LeaseRec, error) {
		cur = got
		return got, errors.New("peek")
	})
	if err == nil || err.Error() != "peek" {
		t.Fatalf("窥视租约失败: %v", err)
	}
	return cur
}
