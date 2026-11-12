package logic

import (
	"context"
	"errors"
	"testing"

	"tuserv/pkg/cache"
	"tuserv/pkg/entity"
	"tuserv/pkg/router"
	"tuserv/pkg/store"
	"tuserv/pkg/store/memory"
)

type bindRec struct {
	entity.State `bson:"-"`
	ID           string `bson:"_id"`
	Server       int32  `bson:"server_id"`
	Name         string `bson:"name"`
	Rev          uint64 `bson:"rev,omitempty"`
}

func (r *bindRec) CacheKey() string            { return r.ID }
func (r *bindRec) ServerID() int32             { return r.Server }
func (r *bindRec) Collection() string          { return "player" }
func (r *bindRec) RouteKey() string            { return r.ID }
func (r *bindRec) Placement() entity.Placement { return entity.PlacementLocal }
func (r *bindRec) NoteLoaded()                 { r.Restore(r.Rev) }

type bindSnap struct {
	ID     string `bson:"_id"`
	Server int32  `bson:"server_id"`
	Name   string `bson:"name"`
	Rev    uint64 `bson:"rev,omitempty"`
	Fence  uint64 `bson:"fence,omitempty"`
}

func (s bindSnap) DocVersion() (uint64, uint64) { return s.Rev, s.Fence }

func (r *bindRec) Persist() (uint64, any) {
	var doc bindSnap
	gen := r.Capture(func() {
		doc.ID = r.ID
		doc.Server = r.Server
		doc.Name = r.Name
		doc.Fence = 1
	})
	doc.Rev = gen
	return gen, doc
}

type failSaveStore struct {
	store.Store
	fail bool
}

func (s *failSaveStore) Save(ctx context.Context, serverID int32, collection, key string, doc any) error {
	if s.fail {
		return errors.New("save boom")
	}
	return s.Store.Save(ctx, serverID, collection, key, doc)
}

// TestBindLocalCacheForgetOnlyAfterFlushOK 保存失败时不能 Forget，否则交出租约后对端只能读到旧档。
func TestBindLocalCacheForgetOnlyAfterFlushOK(t *testing.T) {
	mem := memory.New()
	st := &failSaveStore{Store: mem, fail: true}
	c := cache.New(st)
	app := New(router.New[int32](), nil)
	t.Cleanup(app.Stop)
	BindLocalCache(&Host{App: app, Cache: c}, "player", nil)

	e := &bindRec{ID: "p1", Server: 1, Name: "ann"}
	c.Put(e)
	if err := app.Hooks.FlushKey(context.Background(), 1, "p1"); err == nil {
		t.Fatal("保存失败应返回错误")
	}
	got, err := c.Get(context.Background(), 1, "player", "p1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != e || !got.Dirty() {
		t.Fatal("失败后实体应仍留在内存且保持脏标记")
	}

	st.fail = false
	if err := app.Hooks.FlushKey(context.Background(), 1, "p1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(context.Background(), 1, "player", "p1", nil); err == nil {
		t.Fatal("成功刷盘后应 Forget")
	}
}
