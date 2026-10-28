package service

import (
	"context"
	"errors"

	"tuserv/example/demo/entity"
	"tuserv/example/demo/event"
	"tuserv/pkg/cache"
	core "tuserv/pkg/entity"
	"tuserv/pkg/handler"
	"tuserv/pkg/store"
)

// Publisher 是事件总线中服务需要的那一部分。用接口是为了测试可以替换成记录器。
type Publisher interface {
	Publish(ctx context.Context, ev any)
}

// PlayerService 负责角色的创建、读取和升级。
type PlayerService struct {
	cache *cache.Cache
	store store.Store
	bus   Publisher
}

// NewPlayerService 创建角色服务。自增号从存储的计数器取，0 仍然表示会话还没登录。
func NewPlayerService(c *cache.Cache, st store.Store, bus Publisher) *PlayerService {
	return &PlayerService{cache: c, store: st, bus: bus}
}

// Login 按区服和名字取得角色，没有则创建。未命中时由缓存内部读库，业务不调用 Load。
func (s *PlayerService) Login(ctx context.Context, serverID int32, name string) (*entity.Player, error) {
	if name == "" || len(name) > 16 {
		return nil, ErrBadName
	}
	alias := "name:" + name
	if e, err := s.cache.Get(ctx, serverID, "player", alias, nil); err == nil {
		return e.(*entity.Player), nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	var found entity.Player
	err := s.store.Find(ctx, serverID, "player", "name", name, &found)
	if err == nil {
		e, err := s.cache.Get(ctx, serverID, "player", found.CacheKey(), func() core.Entity { return &entity.Player{} })
		if err != nil {
			return nil, err
		}
		p := e.(*entity.Player)
		s.cache.Alias(p, alias)
		s.cache.Alias(p, idKey(p.PlayerID))
		return p, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	p := entity.NewPlayer(serverID, name)
	id, err := s.store.NextID(ctx, serverID, "player")
	if err != nil {
		return nil, err
	}
	p.PlayerID, err = core.WithServer(serverID, id)
	if err != nil {
		return nil, err
	}
	s.cache.Put(p)
	s.cache.Alias(p, alias)
	s.cache.Alias(p, idKey(p.PlayerID))
	return p, nil
}

// GetByID 按自增编号读取角色。缓存别名没有时再按 id 字段查库，不能把这个号当成 _id。
func (s *PlayerService) GetByID(ctx context.Context, serverID int32, playerID int64) (any, error) {
	p, err := remember(ctx, s.cache, s.store, serverID, "player", playerID, func() core.Entity { return &entity.Player{} })
	if err != nil {
		return nil, err
	}
	return p.(*entity.Player), nil
}

// AddExp 增加经验。每次升级都发事件，奖励由监听者修改背包。
func (s *PlayerService) AddExp(ctx *handler.Context, exp int32) (*entity.Player, error) {
	if exp <= 0 {
		return nil, ErrBadExp
	}
	v, err := ctx.GetPlayer()
	if err != nil {
		return nil, err
	}
	p := v.(*entity.Player)
	levels := p.AddExp(exp)
	for _, level := range levels {
		s.bus.Publish(ctx, event.PlayerLevelUp{Player: p, Level: level})
	}
	p.MarkDirty()
	return p, nil
}
