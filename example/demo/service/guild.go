package service

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"

	"tuserv/example/demo/entity"
	"tuserv/pkg/cache"
	core "tuserv/pkg/entity"
	"tuserv/pkg/handler"
	"tuserv/pkg/store"
)

// GuildRemote 在本节点不是公会主人时把操作转到主人节点。
// Local 返回 true 时服务直接改本地缓存。
type GuildRemote interface {
	Local(routeKey string) bool
	Create(ctx context.Context, g *entity.Guild) error
	Join(ctx context.Context, serverID int32, guildID, playerID int64) (*entity.Guild, error)
}

// GuildService 维护公会成员，并把公会编号写回角色。
type GuildService struct {
	cache  *cache.Cache
	store  store.Store
	remote GuildRemote
	ids    atomic.Int64
}

// NewGuildService 创建公会服务。remote 为空时视为所有公会都在本节点。
func NewGuildService(c *cache.Cache, st store.Store, remote GuildRemote) *GuildService {
	s := &GuildService{cache: c, store: st, remote: remote}
	return s
}

// Create 新建公会。已有公会的角色不能再创建，避免一个人挂在两个公会里。
func (s *GuildService) Create(ctx *handler.Context, name string) (*entity.Guild, error) {
	if name == "" || len(name) > 16 {
		return nil, ErrBadName
	}
	p, err := s.player(ctx)
	if err != nil {
		return nil, err
	}
	if p.GuildID != 0 {
		return nil, ErrAlreadyInGuild
	}
	g := entity.NewGuild(ctx.ServerID, s.ids.Add(1), name, p.PlayerID)
	if s.remote != nil && !s.remote.Local(g.RouteKey()) {
		if err := s.remote.Create(ctx, g); err != nil {
			return nil, err
		}
	} else {
		s.cache.Put(g)
	}
	p.GuildID = g.GuildID
	p.MarkDirty()
	return g, nil
}

// Join 把角色写入公会成员。非主人节点只转发，不在本地再改一份成员列表。
func (s *GuildService) Join(ctx *handler.Context, guildID int64) (*entity.Guild, error) {
	if guildID == 0 {
		return nil, ErrBadArg
	}
	p, err := s.player(ctx)
	if err != nil {
		return nil, err
	}
	if p.GuildID != 0 {
		return nil, ErrAlreadyInGuild
	}
	routeKey := strconv.FormatInt(int64(ctx.ServerID), 10) + ":" + strconv.FormatInt(guildID, 10)
	if s.remote != nil && !s.remote.Local(routeKey) {
		g, err := s.remote.Join(ctx, ctx.ServerID, guildID, p.PlayerID)
		if err != nil {
			return nil, err
		}
		p.GuildID = g.GuildID
		p.MarkDirty()
		return g, nil
	}
	e, err := s.cache.Get(ctx, ctx.ServerID, "guild", strconv.FormatInt(guildID, 10), func() core.Entity {
		return &entity.Guild{}
	})
	if err != nil {
		return nil, err
	}
	g := e.(*entity.Guild)
	g.AddMember(p.PlayerID)
	g.MarkDirty()
	p.GuildID = g.GuildID
	p.MarkDirty()
	return g, nil
}

func (s *GuildService) player(ctx *handler.Context) (*entity.Player, error) {
	v, err := ctx.GetPlayer()
	if err != nil {
		return nil, err
	}
	p, ok := v.(*entity.Player)
	if !ok {
		return nil, errors.New("service: player type")
	}
	return p, nil
}
