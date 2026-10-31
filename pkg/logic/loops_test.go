package logic

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRenewRunsWhileFlushBlocked(t *testing.T) {
	var flushHeld atomic.Bool
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	letGo := func() { releaseOnce.Do(func() { close(release) }) }
	defer letGo()
	var during atomic.Int32
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		Loops{
			RenewEvery: 15 * time.Millisecond,
			FlushEvery: 15 * time.Millisecond,
			ProbeEvery: time.Hour,
			Renew: func(context.Context) {
				if flushHeld.Load() {
					during.Add(1)
				}
			},
			Flush: func(context.Context) {
				if flushHeld.CompareAndSwap(false, true) {
					close(entered)
				}
				<-release
				flushHeld.Store(false)
			},
		}.Run(stop)
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("刷盘没有开始")
	}
	deadline := time.Now().Add(2 * time.Second)
	for during.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if during.Load() == 0 {
		t.Fatal("刷盘堵住时续租应继续")
	}
	close(stop)
	select {
	case <-done:
		t.Fatal("刷盘还在跑时循环不应结束")
	case <-time.After(80 * time.Millisecond):
	}
	letGo()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("放开刷盘后循环应退出")
	}
}
