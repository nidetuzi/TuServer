package handler

import (
	"tuserv/example/demo/gen/game"
	"tuserv/example/demo/service"
	"tuserv/pkg/handler"
)

// OnLogin 使用渠道用户名登录，避免把角色昵称当作账号身份。
func OnLogin(svc *service.PlayerService) func(*handler.Context, *game.LoginReq) (*game.LoginResp, error) {
	return func(ctx *handler.Context, req *game.LoginReq) (*game.LoginResp, error) {
		if req.GetUsername() == "" || len(req.GetUsername()) > 256 {
			return nil, service.ErrBadUsername
		}
		p, err := svc.Login(ctx, ctx.ServerID, req.GetUsername())
		if err != nil {
			return nil, err
		}
		view := p.Snapshot()
		ctx.BindPlayer(view.PlayerID, p.CacheKey())
		return &game.LoginResp{PlayerId: view.PlayerID, Level: view.Level, Exp: view.Exp}, nil
	}
}
