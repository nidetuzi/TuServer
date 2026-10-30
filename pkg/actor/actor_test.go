package actor

import (
	"context"
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
