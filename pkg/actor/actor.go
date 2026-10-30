package actor

import (
	"context"
	"sync"

	"tuserv/pkg/log"
)

const mailboxCap = 64

// System 按编号查找 actor。同一个编号始终对应同一个邮箱。
type System struct {
	mu      sync.Mutex
	actors  map[string]*Actor
	stopped bool
	wg      sync.WaitGroup
}

// NewSystem 创建 actor 表。
func NewSystem() *System {
	return &System{actors: make(map[string]*Actor)}
}

// Actor 是一个串行邮箱。Do 返回后，本次函数已经执行完。
type Actor struct {
	id     string
	mu     sync.Mutex
	cond   *sync.Cond
	queue  []func()
	closed bool
}

// Do 把 fn 放进 id 对应的邮箱并等待它执行完。
// 在 fn 里再同步 Do 同一个 id 会把自己堵死，跨节点通知必须另起协程。
func (s *System) Do(ctx context.Context, id string, fn func()) error {
	if ctx == nil {
		ctx = context.Background()
	}
	a, err := s.get(id)
	if err != nil {
		return err
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
	if err := a.post(ctx, job); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

// Stop 停止新任务并等已排队的任务跑完。重复调用直接返回。
func (s *System) Stop() {
	_ = s.Shutdown(context.Background())
}

// Shutdown 停止接收新任务，排空已有邮箱，并在 ctx 到期时放弃等待。
// 关闭和入队共用同一把锁，停止后不会再向邮箱发送，因此不会出现 send on closed channel。
func (s *System) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	actors := make([]*Actor, 0, len(s.actors))
	for _, a := range s.actors {
		actors = append(actors, a)
	}
	s.mu.Unlock()
	for _, a := range actors {
		a.mu.Lock()
		a.closed = true
		a.cond.Broadcast()
		a.mu.Unlock()
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *System) get(id string) (*Actor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil, context.Canceled
	}
	a, ok := s.actors[id]
	if !ok {
		a = &Actor{id: id}
		a.cond = sync.NewCond(&a.mu)
		s.actors[id] = a
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			a.loop()
		}()
	}
	return a, nil
}

func (a *Actor) post(ctx context.Context, job func()) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	// 队列满时要能被取消叫醒。AfterFunc 在已经取消时会立刻执行，所以必须放在加锁之前。
	stop := context.AfterFunc(ctx, func() { a.cond.Broadcast() })
	defer stop()
	a.mu.Lock()
	defer a.mu.Unlock()
	for len(a.queue) >= mailboxCap && !a.closed {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		a.cond.Wait()
	}
	if a.closed {
		return context.Canceled
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	a.queue = append(a.queue, job)
	a.cond.Signal()
	return nil
}

func (a *Actor) loop() {
	for {
		a.mu.Lock()
		for len(a.queue) == 0 && !a.closed {
			a.cond.Wait()
		}
		if len(a.queue) == 0 && a.closed {
			a.mu.Unlock()
			return
		}
		job := a.queue[0]
		a.queue[0] = nil
		a.queue = a.queue[1:]
		if len(a.queue) == 0 {
			a.queue = nil
		}
		a.cond.Signal()
		a.mu.Unlock()
		a.run(job)
	}
}

func (a *Actor) run(job func()) {
	defer func() {
		if rec := recover(); rec != nil {
			log.Error("actor panic", "actor", a.id, "panic", rec)
		}
	}()
	job()
}
