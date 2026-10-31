package service

import (
	"context"
	"errors"
	"sync"
	"time"

	"tuserv/example/demo/entity"
	"tuserv/example/demo/event"
	"tuserv/pkg/cache"
	core "tuserv/pkg/entity"
	"tuserv/pkg/handler"
	"tuserv/pkg/lease"
	"tuserv/pkg/store"
)

// Publisher 是事件总线中服务需要的那一部分。用接口是为了测试可以替换成记录器。
type Publisher interface {
	Publish(ctx context.Context, ev any)
}

// PlayerService 负责角色的创建、读取和升级。
type PlayerService struct {
	cache    *cache.Cache
	store    store.Store
	bus      Publisher
	ready    sync.Map
	now      func() time.Time
	location *time.Location
}

// NewPlayerService 创建角色服务。自增号从存储的计数器取，0 仍然表示会话还没登录。
func NewPlayerService(c *cache.Cache, st store.Store, bus Publisher) *PlayerService {
	return &PlayerService{cache: c, store: st, bus: bus, now: time.Now, location: time.FixedZone("Asia/Shanghai", 8*60*60)}
}

// Login 按区服和名字取得角色，没有则创建。未命中时由缓存内部读库，业务不调用 Load。
// 新建会立刻插入并依赖 name、id 的唯一索引；并发创建撞索引时改为读取已经插入的那份。
func (s *PlayerService) Login(ctx context.Context, serverID int32, name string) (*entity.Player, error) {
	if name == "" || len(name) > 16 {
		return nil, ErrBadName
	}
	if err := s.ensure(ctx, serverID); err != nil {
		return nil, err
	}
	alias := "name:" + name
	if e, err := s.cache.Get(ctx, serverID, "player", alias, nil); err == nil {
		return s.decorate(ctx, e.(*entity.Player)), nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	var found entity.Player
	err := s.store.Find(ctx, serverID, "player", "name", name, &found)
	if err == nil {
		return s.rememberPlayer(ctx, serverID, alias, &found)
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
	p.InitRev(1)
	_, doc := p.Persist()
	if err := s.store.Insert(ctx, serverID, "player", p.CacheKey(), doc); err != nil {
		if errors.Is(err, store.ErrDuplicate) {
			if ferr := s.store.Find(ctx, serverID, "player", "name", name, &found); ferr != nil {
				return nil, ferr
			}
			return s.rememberPlayer(ctx, serverID, alias, &found)
		}
		return nil, err
	}
	kept := s.cache.Adopt(p).(*entity.Player)
	s.cache.Alias(kept, alias)
	s.cache.Alias(kept, idKey(kept.PlayerID))
	return s.decorate(ctx, kept), nil
}

func (s *PlayerService) rememberPlayer(ctx context.Context, serverID int32, alias string, found *entity.Player) (*entity.Player, error) {
	e, err := s.cache.Get(ctx, serverID, "player", found.CacheKey(), func() core.Entity { return &entity.Player{} })
	if err != nil {
		return nil, err
	}
	p := e.(*entity.Player)
	s.cache.Alias(p, alias)
	s.cache.Alias(p, idKey(p.PlayerID))
	return s.decorate(ctx, p), nil
}

func (s *PlayerService) ensure(ctx context.Context, serverID int32) error {
	if _, ok := s.ready.Load(serverID); ok {
		return nil
	}
	if err := s.store.EnsureUnique(ctx, serverID, "player", "name"); err != nil {
		return err
	}
	if err := s.store.EnsureUnique(ctx, serverID, "player", "id"); err != nil {
		return err
	}
	s.ready.Store(serverID, struct{}{})
	return nil
}

func (s *PlayerService) decorate(ctx context.Context, p *entity.Player) *entity.Player {
	if term, ok := lease.Term(ctx); ok {
		p.SetFence(term)
	}
	return p
}

// GetByID 按自增编号读取角色。缓存别名没有时再按 id 字段查库，不能把这个号当成 _id。
func (s *PlayerService) GetByID(ctx context.Context, serverID int32, playerID int64) (any, error) {
	p, err := remember(ctx, s.cache, s.store, serverID, "player", playerID, func() core.Entity { return &entity.Player{} })
	if err != nil {
		return nil, err
	}
	return s.decorate(ctx, p.(*entity.Player)), nil
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
	if term, ok := lease.Term(ctx); ok {
		p.SetFence(term)
	}
	levels := p.AddExp(exp)
	for _, level := range levels {
		s.bus.Publish(ctx, event.PlayerLevelUp{Player: p, Level: level})
	}
	p.MarkDirty()
	return p, nil
}
