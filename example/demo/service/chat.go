package service

import (
	"context"

	"tuserv/example/demo/entity"
	"tuserv/pkg/handler"
	"tuserv/pkg/router"
)

// CmdChatPush 是推给客户端的聊天协议号，和请求号分开，客户端才能区分回包和广播。
const CmdChatPush int32 = 2001

// Broadcaster 把一条消息发给某个区服的全部逻辑节点。
// 同一区服的玩家可能不在本节点，所以不能只推本地会话。
type Broadcaster interface {
	NotifyServer(ctx context.Context, serverID, cmdID int32, body []byte) error
}

// ChatPushView 是协议无关的聊天推送内容，编码由注入的 BodyCodec 完成。
type ChatPushView struct {
	Name string
	Text string
}

// ChatService 校验通过后组广播包。它不写库，也不依赖具体 IDL。
type ChatService struct {
	bus  Broadcaster
	push router.BodyCodec
}

// NewChatService 创建聊天服务。push 负责把 ChatPushView 编成客户端正文。
func NewChatService(bus Broadcaster, push router.BodyCodec) *ChatService {
	if push == nil {
		panic("service: chat push codec is required")
	}
	return &ChatService{bus: bus, push: push}
}

// Send 异步广播。在玩家 actor 里同步等待全部节点会把邮箱堵死。
func (s *ChatService) Send(ctx *handler.Context, text string) error {
	if len(text) < 1 || len(text) > 80 {
		return ErrBadText
	}
	p, err := handler.Player[*entity.Player](ctx)
	if err != nil {
		return err
	}
	body, err := s.push.Marshal(CmdChatPush, ChatPushView{Name: p.Snapshot().Name, Text: text})
	if err != nil {
		return err
	}
	serverID := ctx.ServerID
	go func() {
		_ = s.bus.NotifyServer(context.Background(), serverID, CmdChatPush, body)
	}()
	return nil
}
