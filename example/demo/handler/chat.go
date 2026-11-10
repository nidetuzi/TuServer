package handler

import (
	"tuserv/example/demo/gen/game"
	"tuserv/example/demo/service"
	"tuserv/pkg/handler"
)

// OnChat 文本门槛由 service 校验；handler 只做调用与空回包。
func OnChat(svc *service.ChatService) func(*handler.Context, *game.ChatReq) (*game.ChatResp, error) {
	return func(ctx *handler.Context, req *game.ChatReq) (*game.ChatResp, error) {
		if err := svc.Send(ctx, req.GetText()); err != nil {
			return nil, err
		}
		return &game.ChatResp{}, nil
	}
}
