package router

import (
	"fmt"

	"tuserv/pkg/handler"
)

// BodyCodec 把协议体字节与业务消息互转。游戏可替换实现，框架不绑死某种 IDL。
type BodyCodec interface {
	Unmarshal(cmd int32, body []byte, dst any) error
	Marshal(cmd int32, msg any) ([]byte, error)
}

// cmdOf 把可比较键尽量收成 int32 传给 BodyCodec；非 int32 键传 0。
func cmdOf[K comparable](key K) int32 {
	switch v := any(key).(type) {
	case int32:
		return v
	case int:
		return int32(v)
	default:
		return 0
	}
}

// Register 用 BodyCodec 解开请求、装上响应后再进入业务函数。
// 业务只看到结构体；换协议时替换 codec，不必改 Router 核心。
func Register[K comparable, Req, Resp any](
	r *Router[K],
	key K,
	codec BodyCodec,
	h func(ctx *handler.Context, req *Req) (*Resp, error),
) {
	if codec == nil {
		panic("router: BodyCodec is required")
	}
	cmd := cmdOf(key)
	r.Handle(key, func(ctx *handler.Context, body []byte) ([]byte, error) {
		var req Req
		if err := codec.Unmarshal(cmd, body, &req); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadMessage, err)
		}
		resp, err := h(ctx, &req)
		if err != nil {
			return nil, err
		}
		if resp == nil {
			return nil, nil
		}
		return codec.Marshal(cmd, resp)
	})
}
