package cache

import (
	"context"
	"testing"

	"tuserv/pkg/entity"
	"tuserv/pkg/store/memory"
)

type rec struct {
	entity.State `bson:"-"`
	ID           string `bson:"_id"`
	Server       int32  `bson:"server_id"`
	Name         string `bson:"name"`
	Rev          uint64 `bson:"rev,omitempty"`
}

func (r *rec) CacheKey() string            { return r.ID }
func (r *rec) ServerID() int32             { return r.Server }
func (r *rec) Collection() string          { return "player" }
func (r *rec) RouteKey() string            { return r.ID }
func (r *rec) Placement() entity.Placement { return entity.PlacementLocal }

// NoteLoaded 恢复库里的代数。淘汰后再加载如果从 0 开始，条件写会把这份新修改当成过期快照丢掉。
func (r *rec) NoteLoaded() { r.Restore(r.Rev) }

type snap struct {
	ID     string `bson:"_id"`
	Server int32  `bson:"server_id"`
	Name   string `bson:"name"`
	Rev    uint64 `bson:"rev,omitempty"`
	Fence  uint64 `bson:"fence,omitempty"`
}

func (s snap) DocVersion() (uint64, uint64) { return s.Rev, s.Fence }

func (r *rec) Persist() (uint64, any) {
	var doc snap
	gen := r.Capture(func() {
		doc.ID = r.ID
		doc.Server = r.Server
		doc.Name = r.Name
		doc.Fence = 1
	})
	doc.Rev = gen
	return gen, doc
}

func TestGetLoadsOnceAndFlushClearsDirty(t *testing.T) {
	mem := memory.New()
	ctx := context.Background()
	if err := mem.Save(ctx, 1, "player", "9", &rec{ID: "9", Server: 1, Name: "ann"}); err != nil {
		t.Fatal(err)
	}
	c := New(mem)
	factory := func() entity.Entity { return &rec{} }
	first, err := c.Get(ctx, 1, "player", "9", factory)
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.Get(ctx, 1, "player", "9", factory)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("第二次 Get 应返回同一份内存")
	}
	if mem.Loads() != 1 {
		t.Fatalf("不应重复 Load，次数=%d", mem.Loads())
	}
	first.(*rec).Name = "ann2"
	first.MarkDirty()
	if err := c.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if first.Dirty() {
		t.Fatal("刷盘成功后应清掉脏标记")
	}
	var loaded rec
	if err := mem.Load(ctx, 1, "player", "9", &loaded); err != nil {
		t.Fatal(err)
	}
	if loaded.Name != "ann2" {
		t.Fatalf("落库内容不符: %s", loaded.Name)
	}
}

func TestFlushDoesNotOverwriteNewerFence(t *testing.T) {
	mem := memory.New()
	ctx := context.Background()
	newer := snap{ID: "9", Server: 1, Name: "new", Rev: 3, Fence: 4}
	if err := mem.Save(ctx, 1, "player", "9", newer); err != nil {
		t.Fatal(err)
	}
	c := New(mem)
	stale := &rec{ID: "9", Server: 1, Name: "old"}
	stale.InitRev(2)
	stale.Name = "old"
	stale.MarkDirty()
	c.Adopt(stale)
	if err := c.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	var loaded snap
	if err := mem.Load(ctx, 1, "player", "9", &loaded); err != nil {
		t.Fatal(err)
	}
	if loaded.Name != "new" || loaded.Fence != 4 {
		t.Fatalf("旧快照覆盖了新文档: %+v", loaded)
	}
	if _, err := c.Get(ctx, 1, "player", "9", nil); err == nil {
		t.Fatal("过期实体应被移出缓存")
	}
}
