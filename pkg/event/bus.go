package event

import (
	"context"
	"reflect"
	"sync"
)

// Bus 按事件的动态类型保存监听者。
type Bus struct {
	mu   sync.RWMutex
	subs map[reflect.Type][]func(context.Context, any)
}

// New 创建一个空事件总线。
func New() *Bus {
	return &Bus{subs: make(map[reflect.Type][]func(context.Context, any))}
}

// On 注册某一种事件的监听者。T 用具体结构体，避免和指针类型对不上。
func On[T any](b *Bus, h func(context.Context, T)) {
	t := reflect.TypeOf((*T)(nil)).Elem()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs[t] = append(b.subs[t], func(ctx context.Context, v any) {
		h(ctx, v.(T))
	})
}

// Publish 在当前协程里依次调用监听者。
// 升级发事件后，奖励监听者要马上改到同一份角色内存，不能等下一次调度。
func (b *Bus) Publish(ctx context.Context, ev any) {
	if ev == nil {
		return
	}
	t := reflect.TypeOf(ev)
	b.mu.RLock()
	list := append([]func(context.Context, any){}, b.subs[t]...)
	b.mu.RUnlock()
	for _, h := range list {
		h(ctx, ev)
	}
}
