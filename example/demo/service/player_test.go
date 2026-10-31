package service

import (
	"context"
	"errors"
	"sync"
	"testing"

	"tuserv/example/demo/entity"
	"tuserv/pkg/cache"
	"tuserv/pkg/store"
	"tuserv/pkg/store/memory"
)

func TestLoginUsesChannelUsernameIndependentlyOfNickname(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	c := cache.New(mem)
	svc := NewPlayerService(c, mem, nil)
	username := "channel-account-longer-than-16-bytes"
	p, err := svc.Login(ctx, 3, username)
	if err != nil {
		t.Fatal(err)
	}
	if p.Username != username || p.Name == username {
		t.Fatalf("渠道用户名应独立于昵称: %+v", p)
	}
	// 昵称修改和缓存重建后仍应通过同一渠道用户名找到原角色。
	p.Name = "新昵称"
	p.MarkDirty()
	if err := c.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	restarted := NewPlayerService(cache.New(mem), mem, nil)
	again, err := restarted.Login(ctx, 3, username)
	if err != nil {
		t.Fatal(err)
	}
	if again.PlayerID != p.PlayerID || again.Username != username || again.Name != "新昵称" {
		t.Fatalf("重新登录应保留身份和昵称: %+v", again)
	}
	var found entity.Player
	if err := mem.Find(ctx, 3, "player", "username", username, &found); err != nil {
		t.Fatal(err)
	}
	if found.Username != username {
		t.Fatalf("渠道用户名未持久化: %+v", found)
	}
}

func TestPlayerCacheOnlyUsesObjectID(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	c := cache.New(mem)
	svc := NewPlayerService(c, mem, nil)
	p, err := svc.Login(ctx, 3, "account")
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"username:account", idKey(p.PlayerID)} {
		if _, err := c.Get(ctx, 3, "player", key, nil); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("角色不应创建缓存别名 %s: %v", key, err)
		}
	}
	v, err := svc.GetByKey(ctx, 3, p.CacheKey())
	if err != nil || v != p {
		t.Fatalf("ObjectID 应命中同一份实体: %v", err)
	}
	c.Forget(p)
	v, err = svc.GetByKey(ctx, 3, p.CacheKey())
	if err != nil {
		t.Fatal(err)
	}
	if loaded := v.(*entity.Player); loaded.OID != p.OID || loaded.PlayerID != p.PlayerID {
		t.Fatalf("缓存未命中应按 ObjectID 加载原角色: %+v", loaded)
	}
	other, err := svc.Login(ctx, 4, "account")
	if err != nil || other.OID == p.OID {
		t.Fatalf("不同区服的相同用户名应对应不同角色: %v", err)
	}
}

func TestConcurrentLoginSameUsername(t *testing.T) {
	mem := memory.New()
	svc := NewPlayerService(cache.New(mem), mem, nil)
	const n = 8
	ids := make([]int64, n)
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p, err := svc.Login(context.Background(), 3, "ann")
			if err != nil {
				errCh <- err
				return
			}
			ids[i] = p.PlayerID
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	for i := 1; i < n; i++ {
		if ids[i] == 0 || ids[i] != ids[0] {
			t.Fatalf("相同用户名并发登录应得到同一个角色: %v", ids)
		}
	}
}
