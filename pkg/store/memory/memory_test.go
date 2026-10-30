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
