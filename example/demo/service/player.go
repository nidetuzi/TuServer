package service

import (
	"context"
	"errors"
	"fmt"
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

// Login 按区服和渠道用户名取得角色，昵称不参与身份查询。
// 新建依赖 username、id 唯一索引，并发创建撞索引时读取已经插入的角色。
func (s *PlayerService) Login(ctx context.Context, serverID int32, username string) (*entity.Player, error) {
	if username == "" || len(username) > 256 {
		return nil, ErrBadUsername
	}
	if err := s.ensure(ctx, serverID); err != nil {
		return nil, err
	}
	var found entity.Player
	err := s.store.Find(ctx, serverID, "player", "username", username, &found)
	if err == nil {
		return s.loginPlayer(ctx, serverID, found.CacheKey())
	}
	if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	p := entity.NewPlayer(serverID, "")
	p.Username = username
	id, err := s.store.NextID(ctx, serverID, "player")
	if err != nil {
		return nil, err
	}
	p.PlayerID, err = core.WithServer(serverID, id)
	if err != nil {
		return nil, err
	}
	// 渠道用户名可能包含内部账号信息，默认昵称使用角色编号。
	p.Name = fmt.Sprintf("玩家%d", p.PlayerID)
	p.InitRev(1)
	_, doc := p.Persist()
	if err := s.store.Insert(ctx, serverID, "player", p.CacheKey(), doc); err != nil {
		if errors.Is(err, store.ErrDuplicate) {
			if ferr := s.store.Find(ctx, serverID, "player", "username", username, &found); ferr != nil {
				return nil, ferr
			}
			return s.loginPlayer(ctx, serverID, found.CacheKey())
		}
		return nil, err
	}
	kept := s.cache.Adopt(p).(*entity.Player)
	return s.decorate(ctx, kept), nil
}

func (s *PlayerService) loginPlayer(ctx context.Context, serverID int32, key string) (*entity.Player, error) {
	v, err := s.GetByKey(ctx, serverID, key)
	if err != nil {
		return nil, err
	}
	return v.(*entity.Player), nil
}

// GetByKey 按登录解析出的 ObjectID 访问缓存，未命中由框架加载存档。
func (s *PlayerService) GetByKey(ctx context.Context, serverID int32, key string) (any, error) {
	e, err := s.cache.Get(ctx, serverID, "player", key, func() core.Entity { return &entity.Player{} })
	if err != nil {
		return nil, err
	}
	return s.decorate(ctx, e.(*entity.Player)), nil
}

func (s *PlayerService) ensure(ctx context.Context, serverID int32) error {
	if _, ok := s.ready.Load(serverID); ok {
		return nil
	}
	if err := s.store.EnsureUnique(ctx, serverID, "player", "username"); err != nil {
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

// GetByID 仅供按业务编号查询角色的业务入口使用，不创建缓存别名。
func (s *PlayerService) GetByID(ctx context.Context, serverID int32, playerID int64) (any, error) {
	var found entity.Player
	if err := s.store.Find(ctx, serverID, "player", "id", playerID, &found); err != nil {
		return nil, err
	}
	return s.GetByKey(ctx, serverID, found.CacheKey())
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
