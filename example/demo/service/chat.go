package service

import (
	"context"

	"google.golang.org/protobuf/proto"

	"tuserv/example/demo/entity"
	"tuserv/example/demo/gen/game"
	"tuserv/pkg/handler"
)

// CmdChatPush 是推给客户端的聊天协议号，和请求号分开，客户端才能区分回包和广播。
const CmdChatPush int32 = 2001

// Broadcaster 把一条消息发给某个区服的全部逻辑节点。
// 同一区服的玩家可能不在本节点，所以不能只推本地会话。
type Broadcaster interface {
	NotifyServer(ctx context.Context, serverID, cmdID int32, body []byte) error
}

// ChatService 校验通过后组广播包。它不写库。
type ChatService struct {
	bus Broadcaster
}

// NewChatService 创建聊天服务。
func NewChatService(bus Broadcaster) *ChatService {
	return &ChatService{bus: bus}
}

// Send 异步广播。在玩家 actor 里同步等待全部节点会把邮箱堵死。
func (s *ChatService) Send(ctx *handler.Context, text string) error {
	if len(text) < 1 || len(text) > 80 {
		return ErrBadText
	}
	v, err := ctx.GetPlayer()
	if err != nil {
		return err
	}
	p := v.(*entity.Player)
	body, err := proto.Marshal(&game.ChatPush{Name: p.Snapshot().Name, Text: text})
	if err != nil {
		return err
	}
	serverID := ctx.ServerID
	go func() {
		_ = s.bus.NotifyServer(context.Background(), serverID, CmdChatPush, body)
	}()
	return nil
}
