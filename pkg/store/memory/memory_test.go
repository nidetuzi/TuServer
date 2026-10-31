package memory

import (
	"context"
	"errors"
	"testing"

	"tuserv/pkg/store"
)

type versioned struct {
	ID    string `bson:"_id"`
	Name  string `bson:"name"`
	Rev   uint64 `bson:"rev"`
	Fence uint64 `bson:"fence"`
}

func (v versioned) DocVersion() (uint64, uint64) { return v.Rev, v.Fence }

func TestInsertRejectsDuplicateName(t *testing.T) {
	s := New()
	ctx := context.Background()
	if err := s.EnsureUnique(ctx, 1, "player", "name"); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(ctx, 1, "player", "a", versioned{ID: "a", Name: "ann", Rev: 1}); err != nil {
		t.Fatal(err)
	}
	err := s.Insert(ctx, 1, "player", "b", versioned{ID: "b", Name: "ann", Rev: 1})
	if !errors.Is(err, store.ErrDuplicate) {
		t.Fatalf("同名插入应冲突: %v", err)
	}
}

func TestSaveRejectsOlderSnapshot(t *testing.T) {
	s := New()
	ctx := context.Background()
	if err := s.Save(ctx, 1, "player", "a", versioned{ID: "a", Name: "new", Rev: 2, Fence: 5}); err != nil {
		t.Fatal(err)
	}
	err := s.Save(ctx, 1, "player", "a", versioned{ID: "a", Name: "old", Rev: 2, Fence: 4})
	if !errors.Is(err, store.ErrStale) {
		t.Fatalf("更小的任期应被拒绝: %v", err)
	}
	var got versioned
	if err := s.Load(ctx, 1, "player", "a", &got); err != nil {
		t.Fatal(err)
	}
	if got.Name != "new" || got.Fence != 5 {
		t.Fatalf("旧快照不应落库: %+v", got)
	}
}

type newDoc struct {
	ID    string `bson:"_id"`
	Name  string `bson:"name"`
	Title string `bson:"title"`
	Rev   uint64 `bson:"rev"`
	Fence uint64 `bson:"fence"`
}

func (n newDoc) DocVersion() (uint64, uint64) { return n.Rev, n.Fence }

type oldDoc struct {
	ID    string `bson:"_id"`
	Name  string `bson:"name"`
	Rev   uint64 `bson:"rev"`
	Fence uint64 `bson:"fence"`
}

func (o oldDoc) DocVersion() (uint64, uint64) { return o.Rev, o.Fence }

func TestSaveKeepsUnknownFields(t *testing.T) {
	s := New()
	ctx := context.Background()
	if err := s.Save(ctx, 1, "player", "a", newDoc{ID: "a", Name: "ann", Title: "hero", Rev: 2, Fence: 3}); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, 1, "player", "a", oldDoc{ID: "a", Name: "anne", Rev: 3, Fence: 3}); err != nil {
		t.Fatal(err)
	}
	var got newDoc
	if err := s.Load(ctx, 1, "player", "a", &got); err != nil {
		t.Fatal(err)
	}
	if got.Name != "anne" || got.Title != "hero" || got.Rev != 3 || got.Fence != 3 {
		t.Fatalf("旧结构不应抹掉未知字段: %+v", got)
	}
}

func TestSaveRejectsStaleRevision(t *testing.T) {
	s := New()
	ctx := context.Background()
	if err := s.Save(ctx, 1, "player", "a", newDoc{ID: "a", Name: "new", Title: "hero", Rev: 4, Fence: 2}); err != nil {
		t.Fatal(err)
	}
	err := s.Save(ctx, 1, "player", "a", oldDoc{ID: "a", Name: "old", Rev: 3, Fence: 2})
	if !errors.Is(err, store.ErrStale) {
		t.Fatalf("更小的代数应被拒绝: %v", err)
	}
	var got newDoc
	if err := s.Load(ctx, 1, "player", "a", &got); err != nil {
		t.Fatal(err)
	}
	if got.Name != "new" || got.Title != "hero" || got.Rev != 4 {
		t.Fatalf("过期快照不应改库: %+v", got)
	}
}

func TestSaveReplacesKnownMapAndKeepsUnknown(t *testing.T) {
	s := New()
	ctx := context.Background()
	if err := s.Save(ctx, 1, "player", "a", versionedBag{ID: "a", Bag: map[string]int32{"a": 1, "b": 2}, Title: "hero", Rev: 1, Fence: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, 1, "player", "a", oldBag{ID: "a", Bag: map[string]int32{"a": 3}, Rev: 2, Fence: 1}); err != nil {
		t.Fatal(err)
	}
	var got versionedBag
	if err := s.Load(ctx, 1, "player", "a", &got); err != nil {
		t.Fatal(err)
	}
	if got.Title != "hero" || got.Bag["a"] != 3 || len(got.Bag) != 1 {
		t.Fatalf("已知容器应整段替换，未知字段应保留: %+v", got)
	}
}

type versionedBag struct {
	ID    string           `bson:"_id"`
	Bag   map[string]int32 `bson:"bag"`
	Title string           `bson:"title"`
	Rev   uint64           `bson:"rev"`
	Fence uint64           `bson:"fence"`
}

func (v versionedBag) DocVersion() (uint64, uint64) { return v.Rev, v.Fence }

type oldBag struct {
	ID    string           `bson:"_id"`
	Bag   map[string]int32 `bson:"bag"`
	Rev   uint64           `bson:"rev"`
	Fence uint64           `bson:"fence"`
}

func (o oldBag) DocVersion() (uint64, uint64) { return o.Rev, o.Fence }

func TestSaveStillRejectsDuplicateName(t *testing.T) {
	s := New()
	ctx := context.Background()
	if err := s.EnsureUnique(ctx, 1, "player", "name"); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, 1, "player", "a", versioned{ID: "a", Name: "ann", Rev: 1}); err != nil {
		t.Fatal(err)
	}
	err := s.Save(ctx, 1, "player", "b", versioned{ID: "b", Name: "ann", Rev: 1})
	if !errors.Is(err, store.ErrDuplicate) {
		t.Fatalf("同名保存应冲突: %v", err)
	}
	if err := s.Save(ctx, 1, "player", "a", versioned{ID: "a", Name: "ann", Rev: 2}); err != nil {
		t.Fatalf("同一文档再次保存不应和自己冲突: %v", err)
	}
}
