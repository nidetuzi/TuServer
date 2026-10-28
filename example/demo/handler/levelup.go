package handler

import (
	"tuserv/example/demo/gen/game"
	"tuserv/example/demo/service"
	"tuserv/pkg/handler"
)

// OnLevelUp 只接受正数经验，具体升级在 service 里完成。
func OnLevelUp(svc *service.PlayerService) func(*handler.Context, *game.LevelUpReq) (*game.LevelUpResp, error) {
	return func(ctx *handler.Context, req *game.LevelUpReq) (*game.LevelUpResp, error) {
		if req.GetExp() <= 0 {
			return nil, service.ErrBadExp
		}
		p, err := svc.AddExp(ctx, req.GetExp())
		if err != nil {
			return nil, err
		}
		return &game.LevelUpResp{Level: p.Level, Exp: p.Exp}, nil
	}
}
