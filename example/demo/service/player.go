package service

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"

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
	ids   atomic.Int64
}

// NewPlayerService 创建角色服务。编号从 1 开始，0 留给未登录会话。
func NewPlayerService(c *cache.Cache, st store.Store, bus Publisher) *PlayerService {
	s := &PlayerService{cache: c, store: st, bus: bus}
	s.ids.Store(0)
	return s
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
		return p, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	p := entity.NewPlayer(serverID, s.ids.Add(1), name)
	s.cache.Put(p)
	s.cache.Alias(p, alias)
	return p, nil
}

// GetByID 按编号读取角色，供上下文的 GetPlayer 使用。
func (s *PlayerService) GetByID(ctx context.Context, serverID int32, playerID int64) (any, error) {
	e, err := s.cache.Get(ctx, serverID, "player", strconv.FormatInt(playerID, 10), func() core.Entity {
		return &entity.Player{}
	})
	if err != nil {
		return nil, err
	}
	return e.(*entity.Player), nil
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
