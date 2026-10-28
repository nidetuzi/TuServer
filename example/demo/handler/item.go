package handler

import (
	"tuserv/example/demo/gen/game"
	"tuserv/example/demo/service"
	"tuserv/pkg/handler"
)

// OnUseItem 校验道具编号和数量，库存判断留给 service。
func OnUseItem(svc *service.BagService) func(*handler.Context, *game.UseItemReq) (*game.UseItemResp, error) {
	return func(ctx *handler.Context, req *game.UseItemReq) (*game.UseItemResp, error) {
		if req.GetItemId() <= 0 || req.GetCount() <= 0 {
			return nil, service.ErrBadArg
		}
		left, err := svc.Use(ctx, req.GetItemId(), req.GetCount())
		if err != nil {
			return nil, err
		}
		return &game.UseItemResp{ItemId: req.GetItemId(), Left: left}, nil
	}
}
