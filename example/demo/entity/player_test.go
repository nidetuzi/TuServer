package entity

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestTryUseIsAtomic(t *testing.T) {
	p := NewPlayer(1, "ann")
	p.BagAdd(7, 1)
	var won atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok := p.TryUse(7, 1); ok {
				won.Add(1)
			}
		}()
	}
	wg.Wait()
	if won.Load() != 1 {
		t.Fatalf("只剩 1 个道具时不能被扣两次: %d", won.Load())
	}
	if p.BagCount(7) != 0 {
		t.Fatalf("扣减后数量应为 0: %d", p.BagCount(7))
	}
}
