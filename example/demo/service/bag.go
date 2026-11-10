package service

import (
	"tuserv/example/demo/entity"
	"tuserv/pkg/handler"
)

// BagService 修改角色背包。背包嵌在角色上，因此不再单独落一份文档。
// 简单扣减也可直接写在 handler；保留 service 供测试与复用调用。
type BagService struct{}

// NewBagService 创建背包服务。
func NewBagService() *BagService { return &BagService{} }

// Use 扣减道具。数量不足时返回业务错误，不把数量扣成负数。
func (s *BagService) Use(ctx *handler.Context, itemID, count int32) (int32, error) {
	if itemID <= 0 || count <= 0 {
		return 0, ErrBadArg
	}
	return handler.Mutate(ctx, func(p *entity.Player) (int32, error) {
		left, ok := p.TryUse(itemID, count)
		if !ok {
			return 0, ErrNotEnough
		}
		return left, nil
	})
}
