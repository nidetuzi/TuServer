package router

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"tuserv/pkg/handler"
)

// ErrNoHandler 表示这个键没有注册处理函数。
// 单独的哨兵是为了让外层把它编成客户端错误码，而不是当成节点故障。
var ErrNoHandler = errors.New("router: no handler")

// ErrBadMessage 表示正文不是该协议的 protobuf。
// 这是客户端把包发错了，换一台 Logic 也不会成功。
var ErrBadMessage = errors.New("router: bad message")

// Handler 是已经反序列化之前的原始处理函数。
// 业务注册走 Register，只有测试或非 protobuf 协议才直接使用它。
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

// Register 在进入业务函数前解开 protobuf，返回前再装上。
// 业务函数因此只看到结构体，协议二进制留在框架边界。
func Register[K comparable, Req any, PReq interface {
	proto.Message
	*Req
}, Resp any, PResp interface {
	proto.Message
	*Resp
}](r *Router[K], key K, h func(ctx *handler.Context, req *Req) (*Resp, error)) {
	r.Handle(key, func(ctx *handler.Context, body []byte) ([]byte, error) {
		var req Req
		if err := proto.Unmarshal(body, PReq(&req)); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadMessage, err)
		}
		resp, err := h(ctx, &req)
		if err != nil {
			return nil, err
		}
		if resp == nil {
			return nil, nil
		}
		return proto.Marshal(PResp(resp))
	})
}
