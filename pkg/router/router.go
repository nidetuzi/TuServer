package router

import (
	"errors"
	"fmt"

	"tuserv/pkg/handler"
)

// ErrNoHandler 表示这个键没有注册处理函数。
// 单独的哨兵是为了让外层把它编成客户端错误码，而不是当成节点故障。
var ErrNoHandler = errors.New("router: no handler")

// ErrBadMessage 表示协议体无法按当前 BodyCodec 解开。
// 这是客户端把包发错了，换一台 Logic 也不会成功。
var ErrBadMessage = errors.New("router: bad message")

// Handler 是协议体仍为原始字节时的处理函数。
// 带编解码的注册走 Register；自定义协议也可直接 Handle。
type Handler func(ctx *handler.Context, body []byte) ([]byte, error)

// Router 保存协议键到处理函数的映射。
type Router[K comparable] struct {
	handlers map[K]Handler
}

// New 创建一个空路由。不同键类型使用不同的 Router，避免 int32 和 string 混在一张表里。
func New[K comparable]() *Router[K] {
	return &Router[K]{handlers: make(map[K]Handler)}
}

// Handle 注册原始处理函数。后注册的同键会覆盖前面的，方便测试替换。
func (r *Router[K]) Handle(key K, h Handler) {
	r.handlers[key] = h
}

// Dispatch 找到键对应的函数。未注册返回错误，避免静默丢包。
func (r *Router[K]) Dispatch(key K, ctx *handler.Context, body []byte) ([]byte, error) {
	h, ok := r.handlers[key]
	if !ok {
		return nil, fmt.Errorf("%w: %v", ErrNoHandler, key)
	}
	return h(ctx, body)
}
