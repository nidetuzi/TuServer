package actor

import (
	"context"
	"sync"

	"tuserv/pkg/log"
)

// System 按编号查找 actor。同一个编号始终对应同一个邮箱。
type System struct {
	mu      sync.Mutex
	actors  map[string]*Actor
	stopped bool
}

// NewSystem 创建 actor 表。
func NewSystem() *System {
	return &System{actors: make(map[string]*Actor)}
}

// Actor 是一个串行邮箱。Do 返回后，本次函数已经执行完。
type Actor struct {
	id string
	ch chan func()
}

// Do 把 fn 放进 id 对应的邮箱并等待它执行完。
// 在 fn 里再同步 Do 同一个 id 会把自己堵死，跨节点通知必须另起协程。
func (s *System) Do(ctx context.Context, id string, fn func()) error {
	if ctx == nil {
		ctx = context.Background()
	}
	a := s.get(id)
	if a == nil {
		return context.Canceled
	}
	done := make(chan struct{})
	job := func() {
		defer close(done)
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("actor panic", "actor", id, "panic", rec)
			}
		}()
		fn()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case a.ch <- job:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

// Stop 关闭全部邮箱。进程退出时调用，避免邮箱协程留下来。
func (s *System) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.stopped = true
	for _, a := range s.actors {
		close(a.ch)
	}
}

func (s *System) get(id string) *Actor {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil
	}
	a, ok := s.actors[id]
	if !ok {
		a = &Actor{id: id, ch: make(chan func(), 64)}
		s.actors[id] = a
		go a.loop()
	}
	return a
}

func (a *Actor) loop() {
	for fn := range a.ch {
		fn()
	}
}
