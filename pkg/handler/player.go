package handler

import "fmt"

// Dirty 是 Mutate 成功后需要打脏的最小约束。
type Dirty interface {
	MarkDirty()
}

// Player 读取当前会话角色并断言为目标类型。只读路径用它，不会打脏。
func Player[T any](ctx *Context) (T, error) {
	var zero T
	v, err := ctx.GetPlayer()
	if err != nil {
		return zero, err
	}
	p, ok := v.(T)
	if !ok {
		return zero, fmt.Errorf("handler: player type %T is not %T", v, zero)
	}
	return p, nil
}

// Mutate 在当前会话角色上执行修改。fn 成功才 MarkDirty，失败不打脏。
// 事件由业务在 fn 内显式发布；半改失败靠实体方法事务式或失败前不改字段。
func Mutate[T Dirty, R any](ctx *Context, fn func(T) (R, error)) (R, error) {
	var zero R
	p, err := Player[T](ctx)
	if err != nil {
		return zero, err
	}
	out, err := fn(p)
	if err != nil {
		return zero, err
	}
	p.MarkDirty()
	return out, nil
}

// MutateDo 是无返回值的 Mutate，避免业务为了打脏硬返回实体指针。
func MutateDo[T Dirty](ctx *Context, fn func(T) error) error {
	_, err := Mutate(ctx, func(p T) (struct{}, error) {
		return struct{}{}, fn(p)
	})
	return err
}
