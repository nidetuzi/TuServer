package scheduler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func newTestScheduler(t *testing.T, cfg Config) *Scheduler {
	t.Helper()
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return s
}

func TestValidationAndCancelIdentity(t *testing.T) {
	if _, err := New(Config{Timezone: "invalid/zone"}); err == nil {
		t.Fatal("非法时区应失败")
	}
	s := newTestScheduler(t, Config{})
	if _, err := s.Add("bad", "bad", func(context.Context) error { return nil }); err == nil {
		t.Fatal("非法规则应失败")
	}
	fn := func(context.Context) error { return nil }
	old, err := s.Add("same", "0 5 * * *", fn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Add("same", "@every 1s", fn); !errors.Is(err, ErrDuplicate) {
		t.Fatal(err)
	}
	old.Cancel()
	current, err := s.Add("same", "@every 1s", fn)
	if err != nil {
		t.Fatal(err)
	}
	old.Cancel()
	if len(s.Entries()) != 1 {
		t.Fatal("旧句柄取消了新任务")
	}
	select {
	case <-current.Done():
		t.Fatal("新任务被取消")
	default:
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
	if _, err := s.Add("late", "@every 1s", fn); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
}

func TestDailyTimezoneAndEntrySnapshot(t *testing.T) {
	s := newTestScheduler(t, Config{Timezone: "Asia/Shanghai"})
	task, err := s.Add("daily", "0 5 * * *", func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 10, 31, 20, 0, 0, 0, time.UTC)
	// cron 运行循环先把当前时间转换成调度器时区，直接测 Schedule 时需保持相同前提。
	next := s.cron.Entry(task.id).Schedule.Next(from.In(s.cron.Location()))
	want := time.Date(2026, 10, 31, 21, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("每日任务时区错误: %v", next)
	}
	if _, err := s.Add("seconds", "0 0 5 * * *", func(context.Context) error { return nil }); err == nil {
		t.Fatal("不应启用六字段表达式")
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	entries := s.Entries()
	if len(entries) != 1 || entries[0].Name != "daily" || entries[0].Next.IsZero() {
		t.Fatalf("缺少触发时间: %+v", entries)
	}
}

func TestOverlapCapacityAndPanic(t *testing.T) {
	s := newTestScheduler(t, Config{MaxConcurrent: 1})
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	task, _ := s.Add("slow", "@every 1s", func(context.Context) error { close(started); <-release; return nil })
	go func() { task.run(); close(done) }()
	<-started
	task.run()
	var runs atomic.Int32
	other, _ := s.Add("other", "@every 1s", func(context.Context) error { runs.Add(1); return nil })
	other.run()
	if runs.Load() != 0 || task.skipped.Load() != 1 || other.skipped.Load() != 1 {
		t.Fatal("重叠或限流未生效")
	}
	// 手动 Run 用于隔离执行语义，等待结束由本测试的释放通道保证。
	t.Cleanup(func() { <-done })
	// 另一个调度器避免本测试刻意占满的配额遮住 panic 路径。
	p := newTestScheduler(t, Config{})
	panicTask, _ := p.Add("panic", "@every 1s", func(context.Context) error { runs.Add(1); panic("业务失败") })
	panicTask.run()
	panicTask.run()
	if runs.Load() != 2 || panicTask.running.Load() {
		t.Fatal("panic 后任务没有释放执行状态")
	}
}

func TestCronShutdownWaitsForCallback(t *testing.T) {
	s := newTestScheduler(t, Config{Timeout: 10 * time.Millisecond})
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	var runs atomic.Int32
	_, err := s.Add("wait", "@every 1s", func(ctx context.Context) error {
		runs.Add(1)
		close(started)
		<-release
		return ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("cron 未触发任务")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("回调未退出时停机应超时: %v", err)
	}
	if !s.Entries()[0].Running || runs.Load() != 1 {
		t.Fatal("业务超时提前释放了运行状态")
	}
}
