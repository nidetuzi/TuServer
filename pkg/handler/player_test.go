package handler

import (
	"context"
	"errors"
	"testing"
)

type memPlayer struct {
	dirty bool
	n     int
}

func (p *memPlayer) MarkDirty() { p.dirty = true }

func TestPlayerAndMutate(t *testing.T) {
	p := &memPlayer{n: 1}
	ctx := New(context.Background(), Meta{PlayerID: 1}).WithLoader(func(context.Context, int32, int64) (any, error) {
		return p, nil
	})
	got, err := Player[*memPlayer](ctx)
	if err != nil || got != p {
		t.Fatalf("Player: %v %v", got, err)
	}
	out, err := Mutate(ctx, func(pl *memPlayer) (int, error) {
		pl.n++
		return pl.n, nil
	})
	if err != nil || out != 2 || !p.dirty {
		t.Fatalf("Mutate success: out=%d dirty=%v err=%v", out, p.dirty, err)
	}
	p.dirty = false
	_, err = Mutate(ctx, func(*memPlayer) (int, error) {
		return 0, errors.New("boom")
	})
	if err == nil || p.dirty {
		t.Fatalf("Mutate failure should not dirty: dirty=%v err=%v", p.dirty, err)
	}
}

func TestPlayerTypeMismatch(t *testing.T) {
	ctx := New(context.Background(), Meta{PlayerID: 1}).WithLoader(func(context.Context, int32, int64) (any, error) {
		return "x", nil
	})
	if _, err := Player[*memPlayer](ctx); err == nil {
		t.Fatal("expected type error")
	}
}
