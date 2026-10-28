package handler

import (
	"tuserv/example/demo/gen/game"
	"tuserv/example/demo/service"
	"tuserv/pkg/handler"
)

// OnChat 校验文本长度。广播由 service 发给所有逻辑节点。
func OnChat(svc *service.ChatService) func(*handler.Context, *game.ChatReq) (*game.ChatResp, error) {
	return func(ctx *handler.Context, req *game.ChatReq) (*game.ChatResp, error) {
		if len(req.GetText()) < 1 || len(req.GetText()) > 80 {
			return nil, service.ErrBadText
		}
		if err := svc.Send(ctx, req.GetText()); err != nil {
			return nil, err
		}
		return &game.ChatResp{}, nil
	}
}
