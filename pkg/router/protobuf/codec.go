// Package protobuf 提供基于 protobuf 的可选协议体编解码。
// Router 核心不依赖本包；游戏选用本适配或自写满足 BodyCodec 的类型。
package protobuf

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// Codec 用 proto.Message 编解码协议体。dst/msg 必须是 proto.Message。
type Codec struct{}

// Unmarshal 把正文解进 dst；dst 必须是指向消息结构体的指针（proto.Message）。
func (Codec) Unmarshal(_ int32, body []byte, dst any) error {
	m, ok := dst.(proto.Message)
	if !ok {
		return fmt.Errorf("protobuf: %T is not proto.Message", dst)
	}
	return proto.Unmarshal(body, m)
}

// Marshal 把消息编码成正文；msg 必须是 proto.Message。
func (Codec) Marshal(_ int32, msg any) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}
	m, ok := msg.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("protobuf: %T is not proto.Message", msg)
	}
	return proto.Marshal(m)
}

// Body 返回可注入 Router.Register 的编解码器。
func Body() Codec { return Codec{} }
