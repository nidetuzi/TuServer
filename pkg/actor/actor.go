package actor

import (
	"context"
	"errors"
	"sync"
	"time"

	"tuserv/pkg/log"
)

const mailboxCap = 64

var errRetired = errors.New("actor retired")

// Option 配置 actor 系统的生命周期行为。
type Option func(*systemConfig)

type systemConfig struct {
	idleTimeout time.Duration
}

// WithIdleTimeout 设置 actor 空闲多久后自动回收；非正数表示关闭自动回收。
func WithIdleTimeout(timeout time.Duration) Option {
	return func(c *systemConfig) {
		c.idleTimeout = timeout
	}
}

// System 按编号查找 actor。同一个编号始终对应同一个邮箱。
type System struct {
	mu          sync.Mutex
	actors      map[string]*Actor
	stopped     bool
	idleTimeout time.Duration
	wg          sync.WaitGroup
}

// NewSystem 创建 actor 表。
func NewSystem(options ...Option) *System {
	config := systemConfig{idleTimeout: time.Minute}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	return &System{actors: make(map[string]*Actor), idleTimeout: config.idleTimeout}
}

// Actor 是一个串行邮箱。Do 返回后，本次函数已经执行完。
type Actor struct {
	id             string
	system         *System
	mu             sync.Mutex
	cond           *sync.Cond
	queue          []func()
	retiring       bool
	closed         bool
	running        bool
	idle           *time.Timer
	idleGeneration uint64
	idleDeadline   time.Time
	done           chan struct{}
}

// Do 把 fn 放进 id 对应的邮箱并等待它执行完。
// 在 fn 里再同步 Do 同一个 id 会把自己堵死，跨节点通知必须另起协程。
func (s *System) Do(ctx context.Context, id string, fn func()) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
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
			if errors.Is(err, errRetired) {
				select {
				case <-a.done:
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return nil
		}
	}
}

// Retire 标记指定 actor 进入退休状态，并立即返回；已入队任务仍会排空。
func (s *System) Retire(id string) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return context.Canceled
	}
	a := s.actors[id]
	s.mu.Unlock()
	if a == nil {
		return nil
	}
	a.retire()
	return nil
}

// Len 返回当前仍保存在系统中的 actor 数量。
func (s *System) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.actors)
}

// Stop 停止新任务并等已排队的任务跑完。重复调用也会等待排空。
func (s *System) Stop() {
	_ = s.Shutdown(context.Background())
}

// Shutdown 停止接收新任务，排空已有邮箱，并在 ctx 到期时放弃等待。
func (s *System) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if !s.stopped {
		s.stopped = true
	}
	actors := make([]*Actor, 0, len(s.actors))
	for _, a := range s.actors {
		actors = append(actors, a)
	}
	s.mu.Unlock()
	for _, a := range actors {
		a.retire()
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
	if ok {
		return a, nil
	}
	a = &Actor{id: id, system: s, done: make(chan struct{})}
	a.cond = sync.NewCond(&a.mu)
	s.actors[id] = a
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		a.loop()
	}()
	return a, nil
}

func (a *Actor) post(ctx context.Context, job func()) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	stop := context.AfterFunc(ctx, func() {
		a.mu.Lock()
		a.cond.Broadcast()
		a.mu.Unlock()
	})
	defer stop()
	a.mu.Lock()
	defer a.mu.Unlock()
	for len(a.queue) >= mailboxCap && !a.retiring && !a.closed {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		a.cond.Wait()
	}
	if a.retiring || a.closed {
		return errRetired
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if a.idle != nil {
		a.idle.Stop()
		a.idle = nil
		a.idleGeneration++
	}
	a.queue = append(a.queue, job)
	a.cond.Signal()
	return nil
}

func (a *Actor) retire() {
	a.mu.Lock()
	if !a.retiring && !a.closed {
		a.retiring = true
		if a.idle != nil {
			a.idle.Stop()
			a.idle = nil
			a.idleGeneration++
		}
		a.cond.Broadcast()
	}
	a.mu.Unlock()
}

func (a *Actor) expire(generation uint64, deadline time.Time) {
	a.mu.Lock()
	if generation == a.idleGeneration && deadline.Equal(a.idleDeadline) && !deadline.After(time.Now()) && !a.retiring && !a.closed && !a.running && len(a.queue) == 0 {
		a.idle = nil
		a.retiring = true
		a.cond.Broadcast()
	}
	a.mu.Unlock()
}

func (a *Actor) loop() {
	defer a.finish()
	for {
		a.mu.Lock()
		for len(a.queue) == 0 && !a.retiring && !a.closed {
			a.running = false
			if a.system.idleTimeout > 0 && a.idle == nil {
				a.idleGeneration++
				generation := a.idleGeneration
				deadline := time.Now().Add(a.system.idleTimeout)
				a.idleDeadline = deadline
				a.idle = time.AfterFunc(a.system.idleTimeout, func() {
					a.expire(generation, deadline)
				})
			}
			a.cond.Wait()
		}
		if len(a.queue) == 0 && (a.retiring || a.closed) {
			a.mu.Unlock()
			return
		}
		if a.idle != nil {
			a.idle.Stop()
			a.idle = nil
			a.idleGeneration++
		}
		job := a.queue[0]
		a.queue[0] = nil
		a.queue = a.queue[1:]
		if len(a.queue) == 0 {
			a.queue = nil
		}
		a.running = true
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

func (a *Actor) finish() {
	a.mu.Lock()
	a.closed = true
	if a.idle != nil {
		a.idle.Stop()
		a.idle = nil
		a.idleGeneration++
	}
	a.cond.Broadcast()
	a.mu.Unlock()
	a.system.mu.Lock()
	if a.system.actors[a.id] == a {
		delete(a.system.actors, a.id)
	}
	a.system.mu.Unlock()
	close(a.done)
}
