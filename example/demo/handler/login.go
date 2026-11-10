package handler

import (
	"tuserv/example/demo/gen/game"
	"tuserv/example/demo/service"
	"tuserv/pkg/handler"
)

// OnLogin 登录门槛由 service 校验；handler 只做调用与回包映射。
func OnLogin(svc *service.PlayerService) func(*handler.Context, *game.LoginReq) (*game.LoginResp, error) {
	return func(ctx *handler.Context, req *game.LoginReq) (*game.LoginResp, error) {
		p, err := svc.Login(ctx, ctx.ServerID, req.GetUsername())
		if err != nil {
			return nil, err
		}
		view := p.Snapshot()
		ctx.BindPlayer(view.PlayerID, p.CacheKey())
		return &game.LoginResp{PlayerId: view.PlayerID, Level: view.Level, Exp: view.Exp}, nil
	}
}
