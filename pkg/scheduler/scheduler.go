package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	_ "time/tzdata" // 精简部署环境也需要能解析业务时区。

	"github.com/robfig/cron/v3"

	"tuserv/pkg/log"
)

// ErrStopped 阻止停机后重新注册或启动任务。
var ErrStopped = errors.New("scheduler: stopped")

// ErrDuplicate 避免同名业务任务被重复注册。
var ErrDuplicate = errors.New("scheduler: duplicate task")

// Config 显式限定调度时区、总并发和每次执行的协作超时。
type Config struct {
	Timezone      string
	MaxConcurrent int
	Timeout       time.Duration
}

// Entry 是任务的只读快照，时间表示触发时间而不是业务完成时间。
type Entry struct {
	Name     string
	Spec     string
	Previous time.Time
	Next     time.Time
	Running  bool
	Skipped  uint64
}

// Scheduler 将业务任务与 cron 的编号、日志和生命周期细节隔离。
type Scheduler struct {
	mu       sync.Mutex
	cron     *cron.Cron
	tasks    map[string]*Task
	slots    chan struct{}
	timeout  time.Duration
	stopped  bool
	stopDone context.Context
}

// Task 是一次注册的取消句柄；旧句柄不会取消后来注册的同名任务。
type Task struct {
	scheduler *Scheduler
	id        cron.EntryID
	name      string
	spec      string
	fn        func(context.Context) error
	ctx       context.Context
	cancel    context.CancelFunc
	running   atomic.Bool
	skipped   atomic.Uint64
}

// New 校验配置后创建未启动的调度器，注册完业务任务再显式启动。
func New(cfg Config) (*Scheduler, error) {
	if cfg.Timezone == "" {
		cfg.Timezone = "Asia/Shanghai"
	}
	location, err := time.LoadLocation(cfg.Timezone)
	if err != nil {
		return nil, fmt.Errorf("scheduler timezone: %w", err)
	}
	if cfg.MaxConcurrent < 0 || cfg.Timeout < 0 {
		return nil, errors.New("scheduler: invalid limits")
	}
	if cfg.MaxConcurrent == 0 {
		cfg.MaxConcurrent = 64
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	logger := cronLogger{}
	return &Scheduler{
		cron:  cron.New(cron.WithLocation(location), cron.WithLogger(logger), cron.WithChain(cron.Recover(logger))),
		tasks: make(map[string]*Task), slots: make(chan struct{}, cfg.MaxConcurrent), timeout: cfg.Timeout,
	}, nil
}

// Add 使用五字段表达式或 @every 注册任务，名称在本调度器中唯一。
func (s *Scheduler) Add(name, spec string, fn func(context.Context) error) (*Task, error) {
	if strings.TrimSpace(name) == "" || fn == nil {
		return nil, errors.New("scheduler: name and callback required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil, ErrStopped
	}
	if _, exists := s.tasks[name]; exists {
		return nil, fmt.Errorf("%w: %s", ErrDuplicate, name)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &Task{scheduler: s, name: name, spec: spec, fn: fn, ctx: ctx, cancel: cancel}
	id, err := s.cron.AddJob(spec, cron.FuncJob(t.run))
	if err != nil {
		cancel()
		return nil, err
	}
	t.id = id
	s.tasks[name] = t
	return t, nil
}

// Start 可重复调用，但停机后的调度器不能复活。
func (s *Scheduler) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return ErrStopped
	}
	s.cron.Start()
	return nil
}

// Cancel 移除这次注册并通知运行中的回调协作退出，不强制终止业务代码。
func (t *Task) Cancel() {
	s := t.scheduler
	s.mu.Lock()
	defer s.mu.Unlock()
	t.cancel()
	if s.tasks[t.name] == t {
		delete(s.tasks, t.name)
		s.cron.Remove(t.id)
	}
}

// Done 在任务取消或调度器停机时关闭，可用于清理外部作用域中的旧句柄。
func (t *Task) Done() <-chan struct{} { return t.ctx.Done() }

// Entries 按名称排序返回快照，避免调用方依赖 cron 内部编号。
func (s *Scheduler) Entries() []Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := s.cron.Entries()
	byID := make(map[cron.EntryID]cron.Entry, len(entries))
	for _, e := range entries {
		byID[e.ID] = e
	}
	result := make([]Entry, 0, len(s.tasks))
	for _, t := range s.tasks {
		e := byID[t.id]
		result = append(result, Entry{Name: t.name, Spec: t.spec, Previous: e.Prev, Next: e.Next, Running: t.running.Load(), Skipped: t.skipped.Load()})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

// Shutdown 停止新触发并等待真实回调结束，超时返回错误而不假定任务已退出。
func (s *Scheduler) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if !s.stopped {
		s.stopped = true
		for _, t := range s.tasks {
			t.cancel()
		}
		s.stopDone = s.cron.Stop()
	}
	done := s.stopDone
	s.mu.Unlock()
	select {
	case <-done.Done():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// run 保持整个业务回调都在防重叠和并发配额内，actor 调用方必须等真实执行完成。
func (t *Task) run() {
	if t.ctx.Err() != nil {
		return
	}
	if !t.running.CompareAndSwap(false, true) {
		t.skip("running")
		return
	}
	defer t.running.Store(false)
	select {
	case t.scheduler.slots <- struct{}{}:
		defer func() { <-t.scheduler.slots }()
	default:
		t.skip("capacity")
		return
	}
	ctx, cancel := context.WithTimeout(t.ctx, t.scheduler.timeout)
	defer cancel()
	if ctx.Err() != nil {
		return
	}
	start := time.Now()
	defer func() {
		if rec := recover(); rec != nil {
			log.Error("scheduled task panic", "task", t.name, "panic", rec, "elapsed", time.Since(start))
		}
	}()
	if err := t.fn(ctx); err != nil {
		log.Error("scheduled task failed", "task", t.name, "err", err, "elapsed", time.Since(start))
	} else {
		log.Debug("scheduled task completed", "task", t.name, "elapsed", time.Since(start))
	}
}

func (t *Task) skip(reason string) {
	t.skipped.Add(1)
	log.Debug("scheduled task skipped", "task", t.name, "reason", reason)
}

type cronLogger struct{}

func (cronLogger) Info(msg string, args ...interface{}) { log.Debug(msg, args...) }
func (cronLogger) Error(err error, msg string, args ...interface{}) {
	log.Error(msg, append(args, "err", err)...)
}
