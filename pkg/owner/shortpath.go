package owner

import (
	"context"
	"errors"

	"tuserv/pkg/entity"
	"tuserv/pkg/lease"
	"tuserv/pkg/store"
)

type fencer interface {
	SetFence(uint64)
}

// OnMutate 注册「加载已有实体 → 套任期 → 改写 → 快照」的主人操作。
// 是否 MarkDirty 由 mutate 决定，避免无变更操作产生无意义脏标记；跨实体回写仍由调用侧负责。
func OnMutate[E entity.Entity, Args any, DTO any](
	rt *Runtime,
	op string,
	load func(ctx context.Context, key EntityKey) (E, error),
	snapshot func(E) DTO,
	mutate func(ctx context.Context, e E, args Args) error,
) error {
	if load == nil || snapshot == nil || mutate == nil {
		return NewError("owner.invalid", "load, snapshot and mutate are required")
	}
	return Register(rt, op, func(ctx context.Context, key EntityKey, args Args) (DTO, error) {
		var zero DTO
		e, err := load(ctx, key)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return zero, NewError(key.Kind+".not_found", key.Kind+" not found")
			}
			return zero, err
		}
		applyFence(ctx, e)
		if err := mutate(ctx, e, args); err != nil {
			return zero, err
		}
		return snapshot(e), nil
	})
}

// OnCreate 注册「构造实体 → 套任期 → keep → 快照」的主人操作。
// keep 通常执行 Put 与业务编号 Alias；Put 会打脏，这里不再重复 MarkDirty。
func OnCreate[E entity.Entity, Args any, DTO any](
	rt *Runtime,
	op string,
	create func(ctx context.Context, key EntityKey, args Args) (E, error),
	keep func(E),
	snapshot func(E) DTO,
) error {
	if create == nil || keep == nil || snapshot == nil {
		return NewError("owner.invalid", "create, keep and snapshot are required")
	}
	return Register(rt, op, func(ctx context.Context, key EntityKey, args Args) (DTO, error) {
		var zero DTO
		e, err := create(ctx, key, args)
		if err != nil {
			return zero, err
		}
		applyFence(ctx, e)
		keep(e)
		return snapshot(e), nil
	})
}

func applyFence(ctx context.Context, e entity.Entity) {
	term, ok := lease.Term(ctx)
	if !ok {
		return
	}
	if f, ok := e.(fencer); ok {
		f.SetFence(term)
	}
}
