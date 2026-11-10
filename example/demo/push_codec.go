package demo

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"tuserv/example/demo/gen/game"
	"tuserv/example/demo/service"
	routepb "tuserv/pkg/router/protobuf"
)

// pushCodec 把聊天推送视图编成 protobuf，请求路由仍用独立的 BodyCodec。
type pushCodec struct {
	routepb.Codec
}

// Marshal 识别 ChatPushView；其他消息回退到 protobuf Codec。
func (c pushCodec) Marshal(cmd int32, msg any) ([]byte, error) {
	switch v := msg.(type) {
	case service.ChatPushView:
		return proto.Marshal(&game.ChatPush{Name: v.Name, Text: v.Text})
	case *service.ChatPushView:
		if v == nil {
			return nil, nil
		}
		return proto.Marshal(&game.ChatPush{Name: v.Name, Text: v.Text})
	default:
		return c.Codec.Marshal(cmd, msg)
	}
}

// Unmarshal 推送编码不用于请求解析。
func (pushCodec) Unmarshal(int32, []byte, any) error {
	return fmt.Errorf("demo: push codec does not unmarshal requests")
}
