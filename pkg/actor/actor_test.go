package actor

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStopWaitsForRunningJob(t *testing.T) {
	s := NewSystem()
	started := make(chan struct{})
	release := make(chan struct{})
	var finished atomic.Bool
	go func() {
		_ = s.Do(context.Background(), "p", func() {
			close(started)
			<-release
			finished.Store(true)
		})
	}()
	<-started
	stopped := make(chan struct{})
	go func() {
		s.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("正在执行的任务还没结束，Stop 就返回了")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop 没有在任务结束后返回")
	}
	if !finished.Load() {
		t.Fatal("任务没有执行完")
	}
	if err := s.Do(context.Background(), "p", func() {}); err == nil {
		t.Fatal("停止后不应再接收任务")
	}
}

func TestStopConcurrentWithSendDoesNotPanic(t *testing.T) {
	s := NewSystem()
	hold := make(chan struct{})
	started := make(chan struct{})
	go func() {
		_ = s.Do(context.Background(), "p", func() {
			close(started)
			<-hold
		})
	}()
	<-started
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 32; i++ {
			_ = s.Do(context.Background(), "p", func() {})
		}
	}()
	stopped := make(chan struct{})
	go func() {
		s.Stop()
		close(stopped)
	}()
	time.Sleep(20 * time.Millisecond)
	close(hold)
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop 没有返回")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("停止后仍在发送的 Do 没有返回")
	}
}

func TestConcurrentDoNeverRunsSameActorConcurrently(t *testing.T) {
	s := NewSystem(WithIdleTimeout(0))
	const count = 128
	var current, maximum atomic.Int32
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			if err := s.Do(context.Background(), "same", func() {
				n := current.Add(1)
				for {
					old := maximum.Load()
					if n <= old || maximum.CompareAndSwap(old, n) {
						break
					}
				}
				time.Sleep(time.Microsecond)
				current.Add(-1)
			}); err != nil {
				t.Errorf("Do 返回错误: %v", err)
			}
		}()
	}
	wg.Wait()
	if got := maximum.Load(); got != 1 {
		t.Fatalf("同一 actor 同时执行了 %d 个任务", got)
	}
}

func TestRetireDrainsQueueAndOldPostRetries(t *testing.T) {
	s := NewSystem(WithIdleTimeout(0))
	started := make(chan struct{})
	release := make(chan struct{})
	var ran atomic.Int32
	go func() {
		_ = s.Do(context.Background(), "p", func() {
			close(started)
			<-release
			ran.Add(1)
		})
	}()
	<-started
	queued := make(chan struct{})
	go func() {
		_ = s.Do(context.Background(), "p", func() {
			ran.Add(1)
			close(queued)
		})
	}()
	if err := s.Retire("p"); err != nil {
		t.Fatal(err)
	}
	newPost := make(chan struct{})
	go func() {
		_ = s.Do(context.Background(), "p", func() {
			ran.Add(1)
			close(newPost)
		})
	}()
	close(release)
	select {
	case <-queued:
	case <-time.After(2 * time.Second):
		t.Fatal("退休前已入队任务没有排空")
	}
	select {
	case <-newPost:
	case <-time.After(2 * time.Second):
		t.Fatal("旧引用投递没有在 actor 退出后重试")
	}
	if got := ran.Load(); got != 3 {
		t.Fatalf("执行任务数 = %d, want 3", got)
	}
	if got := s.Len(); got != 1 {
		t.Fatalf("新 actor 未保留，Len = %d", got)
	}
	_ = s.Shutdown(context.Background())
}

func TestIdleActorIsCollected(t *testing.T) {
	s := NewSystem(WithIdleTimeout(20 * time.Millisecond))
	if err := s.Do(context.Background(), "p", func() {}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for s.Len() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := s.Len(); got != 0 {
		t.Fatalf("空闲 actor 未回收，Len = %d", got)
	}
	_ = s.Shutdown(context.Background())
}

func TestStaleIdleTimerCannotRetireNewIdleCycle(t *testing.T) {
	s := NewSystem(WithIdleTimeout(0))
	a := &Actor{id: "p", system: s, done: make(chan struct{})}
	a.cond = sync.NewCond(&a.mu)
	a.mu.Lock()
	a.idleGeneration = 2
	a.idleDeadline = time.Now().Add(time.Minute)
	a.mu.Unlock()
	a.expire(1, time.Now().Add(-time.Minute))
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.retiring {
		t.Fatal("旧 timer 回调错误地退休了新 idle 周期")
	}
}

func TestRepeatedShutdownStillWaitsForDrain(t *testing.T) {
	s := NewSystem(WithIdleTimeout(0))
	started := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_ = s.Do(context.Background(), "p", func() {
			close(started)
			<-release
		})
	}()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := s.Shutdown(ctx); err == nil {
		t.Fatal("第一次 Shutdown 应因超时返回错误")
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- s.Shutdown(context.Background()) }()
	select {
	case err := <-secondDone:
		t.Fatalf("第二次 Shutdown 提前返回: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
}

func TestShutdownDrainsRetiredAndRejectsNewActors(t *testing.T) {
	s := NewSystem(WithIdleTimeout(0))
	started := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_ = s.Do(context.Background(), "p", func() {
			close(started)
			<-release
		})
	}()
	<-started
	shutdown := make(chan error, 1)
	go func() { shutdown <- s.Shutdown(context.Background()) }()
	select {
	case err := <-shutdown:
		t.Fatalf("Shutdown 提前返回: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
	if err := s.Do(context.Background(), "new", func() {}); err == nil {
		t.Fatal("Shutdown 后不应创建新 actor")
	}
	if got := s.Len(); got != 0 {
		t.Fatalf("Shutdown 后仍有 actor，Len = %d", got)
	}
}
