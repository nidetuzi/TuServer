package cache

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"tuserv/pkg/entity"
	"tuserv/pkg/store"
	"tuserv/pkg/store/memory"
)

type hookStore struct {
	*memory.Store
	save func(key string) error
}

func (s *hookStore) Save(ctx context.Context, serverID int32, collection, key string, doc any) error {
	if s.save != nil {
		if err := s.save(key); err != nil {
			return err
		}
	}
	return s.Store.Save(ctx, serverID, collection, key, doc)
}

type stepClock struct{ n atomic.Int64 }

func (s *stepClock) now() time.Time { return time.Unix(0, s.n.Add(1)) }

type manualClock struct{ n atomic.Int64 }

func (m *manualClock) now() time.Time { return time.Unix(0, m.n.Load()) }

func (m *manualClock) advance(d time.Duration) { m.n.Add(int64(d)) }

func newRec(id, name string) *rec {
	return &rec{ID: id, Server: 1, Name: name}
}

func absent(t *testing.T, c *Cache, key string) {
	t.Helper()
	_, err := c.Get(context.Background(), 1, "player", key, nil)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("键 %s 应不在内存: %v", key, err)
	}
}

func TestEvictDisabledWithoutPolicy(t *testing.T) {
	c := New(memory.New())
	c.Put(newRec("a", "a"))
	c.Put(newRec("b", "b"))
	if err := c.Evict(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.Len() != 2 {
		t.Fatalf("未配置淘汰时不应丢掉实体, len=%d", c.Len())
	}
}

func TestLRUDropsOldestAndKeepsAliasWithIt(t *testing.T) {
	clk := &stepClock{}
	mem := memory.New()
	c := New(mem, WithPolicy(Policy{MaxItems: 2}), WithClock(clk.now))
	a := newRec("a", "a")
	c.Put(a)
	c.Alias(a, "alias-a")
	c.Put(newRec("b", "b"))
	c.Put(newRec("c", "c"))
	if err := c.Evict(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.Len() != 2 {
		t.Fatalf("容量应为 2, len=%d", c.Len())
	}
	absent(t, c, "a")
	absent(t, c, "alias-a")
	if _, err := c.Get(context.Background(), 1, "player", "b", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(context.Background(), 1, "player", "c", nil); err != nil {
		t.Fatal(err)
	}
	var loaded rec
	if err := mem.Load(context.Background(), 1, "player", "a", &loaded); err != nil {
		t.Fatal(err)
	}
	if loaded.Name != "a" {
		t.Fatalf("淘汰前应先把脏数据写下: %s", loaded.Name)
	}
}

func TestIdleEvictsAndPinKeyHoldsOnlineAlias(t *testing.T) {
	clk := &manualClock{}
	c := New(memory.New(), WithPolicy(Policy{Idle: time.Second, MaxItems: 1}), WithClock(clk.now))
	player := newRec("oid", "ann")
	c.Put(player)
	c.Alias(player, "42")
	other := newRec("other", "x")
	c.Put(other)
	pin := c.PinKey(1, "player", "42")
	clk.advance(time.Hour)
	if err := c.Evict(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(context.Background(), 1, "player", "42", nil); err != nil {
		t.Fatal(err)
	}
	absent(t, c, "other")
	// Get 会刷新访问时间，要把时钟再拨过空闲阈值。
	clk.advance(time.Hour)
	pin()
	if err := c.Evict(context.Background()); err != nil {
		t.Fatal(err)
	}
	absent(t, c, "42")
	absent(t, c, "oid")
}

func TestGuardHoldsEntityAcrossDirtyGap(t *testing.T) {
	clk := &manualClock{}
	mem := memory.New()
	c := New(mem, WithPolicy(Policy{Idle: time.Nanosecond}), WithClock(clk.now))
	release := c.Guard()
	a := newRec("a", "old")
	c.Put(a)
	a.Lock()
	a.Name = "new"
	a.Unlock()
	clk.advance(time.Hour)
	if err := c.Evict(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.Len() != 1 || !a.Dirty() {
		t.Fatalf("邮箱还没打脏标记时不能淘汰, len=%d dirty=%v", c.Len(), a.Dirty())
	}
	a.MarkDirty()
	release()
	if err := c.Evict(context.Background()); err != nil {
		t.Fatal(err)
	}
	absent(t, c, "a")
	var loaded rec
	if err := mem.Load(context.Background(), 1, "player", "a", &loaded); err != nil {
		t.Fatal(err)
	}
	if loaded.Name != "new" {
		t.Fatalf("放开之后应先写下最新内容: %s", loaded.Name)
	}
}

func TestEvictKeepsDirtyWhenSaveFailsAndContinues(t *testing.T) {
	mem := memory.New()
	st := &hookStore{Store: mem, save: func(key string) error {
		if key == "a" {
			return errors.New("boom")
		}
		return nil
	}}
	clk := &stepClock{}
	c := New(st, WithPolicy(Policy{MaxItems: 1}), WithClock(clk.now))
	c.Put(newRec("a", "a"))
	c.Put(newRec("b", "b"))
	err := c.Evict(context.Background())
	if err == nil || err.Error() == "" {
		t.Fatal("保存失败应返回错误")
	}
	if _, getErr := c.Get(context.Background(), 1, "player", "a", nil); getErr != nil {
		t.Fatal(getErr)
	}
	absent(t, c, "b")
	var loaded rec
	if err := mem.Load(context.Background(), 1, "player", "b", &loaded); err != nil {
		t.Fatal(err)
	}
	if loaded.Name != "b" {
		t.Fatalf("失败的那条不应挡住下一条: %s", loaded.Name)
	}
	if err := mem.Load(context.Background(), 1, "player", "a", &loaded); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("失败的保存不应假装已经落库: %v", err)
	}
}

func TestFlushContinuesAfterSaveError(t *testing.T) {
	mem := memory.New()
	st := &hookStore{Store: mem, save: func(key string) error {
		if key == "bad" {
			return errors.New("boom")
		}
		return nil
	}}
	c := New(st)
	c.Put(newRec("bad", "bad"))
	c.Put(newRec("ok", "ok"))
	err := c.Flush(context.Background())
	if err == nil {
		t.Fatal("应报告失败的那一条")
	}
	bad, err := c.Get(context.Background(), 1, "player", "bad", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bad.Dirty() {
		t.Fatal("失败的实体应保持脏标记")
	}
	ok, err := c.Get(context.Background(), 1, "player", "ok", nil)
	if err != nil {
		t.Fatal(err)
	}
	if ok.Dirty() {
		t.Fatal("成功的实体应已清脏")
	}
	var loaded rec
	if err := mem.Load(context.Background(), 1, "player", "ok", &loaded); err != nil || loaded.Name != "ok" {
		t.Fatalf("成功的实体应已落库: %v name=%s", err, loaded.Name)
	}
}

func TestFlushStopsWhenContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var saves atomic.Int32
	st := &hookStore{Store: memory.New(), save: func(string) error {
		saves.Add(1)
		cancel()
		return nil
	}}
	c := New(st)
	c.Put(newRec("a", "a"))
	c.Put(newRec("b", "b"))
	err := c.Flush(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("取消后应停下: %v", err)
	}
	if saves.Load() != 1 {
		t.Fatalf("取消后不应再保存后面的实体, saves=%d", saves.Load())
	}
}

func TestEvictStaleDropsWithoutOverwrite(t *testing.T) {
	mem := memory.New()
	ctx := context.Background()
	newer := snap{ID: "9", Server: 1, Name: "new", Rev: 3, Fence: 4}
	if err := mem.Save(ctx, 1, "player", "9", newer); err != nil {
		t.Fatal(err)
	}
	clk := &manualClock{}
	c := New(mem, WithPolicy(Policy{Idle: time.Second}), WithClock(clk.now))
	stale := newRec("9", "old")
	stale.InitRev(2)
	stale.MarkDirty()
	c.Adopt(stale)
	clk.advance(time.Hour)
	if err := c.Evict(ctx); err != nil {
		t.Fatal(err)
	}
	var loaded snap
	if err := mem.Load(ctx, 1, "player", "9", &loaded); err != nil {
		t.Fatal(err)
	}
	if loaded.Name != "new" || loaded.Fence != 4 {
		t.Fatalf("过期快照不能覆盖新文档: %+v", loaded)
	}
	absent(t, c, "9")
}

func TestMarkDirtyDuringSaveIsNotEvicted(t *testing.T) {
	mem := memory.New()
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	st := &hookStore{Store: mem, save: func(key string) error {
		if key != "a" {
			return nil
		}
		once.Do(func() { close(started) })
		<-release
		return nil
	}}
	clk := &manualClock{}
	c := New(st, WithPolicy(Policy{Idle: time.Second}), WithClock(clk.now))
	a := newRec("a", "v1")
	c.Put(a)
	clk.advance(time.Hour)
	done := make(chan error, 1)
	go func() { done <- c.Evict(context.Background()) }()
	<-started
	a.Lock()
	a.Name = "v2"
	a.Unlock()
	a.MarkDirty()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(context.Background(), 1, "player", "a", nil); err != nil {
		t.Fatal(err)
	}
	if !a.Dirty() {
		t.Fatal("保存过程中的新修改必须留在内存")
	}
	if err := c.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	var loaded rec
	if err := mem.Load(context.Background(), 1, "player", "a", &loaded); err != nil {
		t.Fatal(err)
	}
	if loaded.Name != "v2" {
		t.Fatalf("最终应写下保存期间的新值: %s", loaded.Name)
	}
}

func TestNilStoreDoesNotEvictDirty(t *testing.T) {
	clk := &stepClock{}
	c := New(nil, WithPolicy(Policy{MaxItems: 1}), WithClock(clk.now))
	a := newRec("a", "a")
	b := newRec("b", "b")
	c.Put(a)
	c.Put(b)
	if err := c.Evict(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.Len() != 2 {
		t.Fatalf("没有存储时脏实体不能淘汰, len=%d", c.Len())
	}
	a.EndSave(a.Generation())
	if err := c.Evict(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.Len() != 1 {
		t.Fatalf("干净实体在没有存储时仍可淘汰, len=%d", c.Len())
	}
	absent(t, c, "a")
	if _, err := c.Get(context.Background(), 1, "player", "b", nil); err != nil {
		t.Fatal(err)
	}
}

func commitOnce(t *testing.T, c *Cache, mem *memory.Store, ctx context.Context, id string, committed *atomic.Int64) bool {
	t.Helper()
	release := c.Guard()
	defer release()
	e, err := c.Get(ctx, 1, "player", id, func() entity.Entity {
		return &rec{ID: id, Server: 1}
	})
	prev := committed.Load()
	var r *rec
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) || prev != 0 {
			t.Errorf("实体 %s 已提交的值不在缓存也不在库里: %v", id, err)
			return false
		}
		r = &rec{ID: id, Server: 1}
		c.Put(r)
	} else {
		r = e.(*rec)
		r.Lock()
		got := r.Name
		r.Unlock()
		if prev > 0 && got != strconv.FormatInt(prev, 10) {
			t.Errorf("实体 %s 读回旧值 %s, 已提交 %d", id, got, prev)
			return false
		}
	}
	v := prev + 1
	r.Lock()
	r.Name = strconv.FormatInt(v, 10)
	r.Unlock()
	r.MarkDirty()
	committed.Store(v)
	return true
}

func TestEvictRacePreservesCommitted(t *testing.T) {
	mem := memory.New()
	var now atomic.Int64
	c := New(mem, WithPolicy(Policy{MaxItems: 3, Idle: time.Millisecond}), WithClock(func() time.Time {
		return time.Unix(0, now.Add(int64(time.Millisecond)))
	}))
	const n = 8
	var committed [n]atomic.Int64
	ctx := context.Background()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := strconv.Itoa(i)
			for {
				select {
				case <-stop:
					return
				default:
				}
				if !commitOnce(t, c, mem, ctx, id, &committed[i]) {
					return
				}
			}
		}(i)
	}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = c.Flush(ctx)
				_ = c.Evict(ctx)
			}
		}()
	}
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	if err := c.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		want := committed[i].Load()
		if want == 0 {
			t.Fatalf("实体 %d 没有完成一次写入", i)
		}
		var loaded rec
		if err := mem.Load(ctx, 1, "player", strconv.Itoa(i), &loaded); err != nil {
			t.Fatal(err)
		}
		if loaded.Name != strconv.FormatInt(want, 10) {
			t.Fatalf("实体 %d 丢失更新: got %s want %d", i, loaded.Name, want)
		}
	}
}
