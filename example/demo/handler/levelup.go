package handler

import (
	"tuserv/example/demo/entity"
	"tuserv/example/demo/gen/game"
	"tuserv/example/demo/service"
	"tuserv/pkg/handler"
)

// OnLevelUp 经验门槛由 service 校验；回包再读玩家快照。
func OnLevelUp(svc *service.PlayerService) func(*handler.Context, *game.LevelUpReq) (*game.LevelUpResp, error) {
	return func(ctx *handler.Context, req *game.LevelUpReq) (*game.LevelUpResp, error) {
		if err := svc.AddExp(ctx, req.GetExp()); err != nil {
			return nil, err
		}
		p, err := handler.Player[*entity.Player](ctx)
		if err != nil {
			return nil, err
		}
		view := p.Snapshot()
		return &game.LevelUpResp{Level: view.Level, Exp: view.Exp}, nil
	}
}