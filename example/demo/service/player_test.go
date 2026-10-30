package service

import (
	"context"
	"sync"
	"testing"

	"tuserv/pkg/cache"
	"tuserv/pkg/store/memory"
)

func TestConcurrentLoginSameName(t *testing.T) {
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
			t.Fatalf("同名并发登录应得到同一个角色: %v", ids)
		}
	}
}
