package handler

import (
	"tuserv/example/demo/entity"
	"tuserv/example/demo/gen/game"
	"tuserv/example/demo/service"
	"tuserv/pkg/handler"
)

// OnUseItem 简单扣道具：校验后直接 Mutate，不强制再进一层 service。
func OnUseItem() func(*handler.Context, *game.UseItemReq) (*game.UseItemResp, error) {
	return func(ctx *handler.Context, req *game.UseItemReq) (*game.UseItemResp, error) {
		itemID, count := req.GetItemId(), req.GetCount()
		if itemID <= 0 || count <= 0 {
			return nil, service.ErrBadArg
		}
		left, err := handler.Mutate(ctx, func(p *entity.Player) (int32, error) {
			left, ok := p.TryUse(itemID, count)
			if !ok {
				return 0, service.ErrNotEnough
			}
			return left, nil
		})
		if err != nil {
			return nil, err
		}
		return &game.UseItemResp{ItemId: itemID, Left: left}, nil
	}
}
