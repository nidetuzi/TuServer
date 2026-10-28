package handler

import (
	"tuserv/example/demo/gen/game"
	"tuserv/example/demo/service"
	"tuserv/pkg/handler"
)

// OnLogin 校验名字并创建或读取角色。名字规则在这里先挡一层，service 里再挡一次是为了非协议入口也不能绕过。
func OnLogin(svc *service.PlayerService) func(*handler.Context, *game.LoginReq) (*game.LoginResp, error) {
	return func(ctx *handler.Context, req *game.LoginReq) (*game.LoginResp, error) {
		if req.GetName() == "" || len(req.GetName()) > 16 {
			return nil, service.ErrBadName
		}
		p, err := svc.Login(ctx, ctx.ServerID, req.GetName())
		if err != nil {
			return nil, err
		}
		ctx.BindPlayer(p.PlayerID)
		return &game.LoginResp{PlayerId: p.PlayerID, Level: p.Level, Exp: p.Exp}, nil
	}
}
