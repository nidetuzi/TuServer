package service

import (
	"context"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"tuserv/example/demo/entity"
	"tuserv/pkg/cache"
	core "tuserv/pkg/entity"
	"tuserv/pkg/handler"
	"tuserv/pkg/owner"
	"tuserv/pkg/store"
)

// GuildResult 返回公会快照，避免调用方持有主人节点的可变实体。
type GuildResult struct {
	GuildID  int64
	ServerID int32
	Name     string
	Members  []int64
}

type guildCreateArgs struct {
	OID      primitive.ObjectID
	Name     string
	PlayerID int64
}

type guildJoinArgs struct {
	PlayerID int64
}

// GuildService 维护公会成员，并把公会编号写回角色。
type GuildService struct {
	cache  *cache.Cache
	store  store.Store
	owners *owner.Runtime
}

// NewGuildService 要求显式提供主人运行时，避免缺少路由时静默写入本节点。
func NewGuildService(c *cache.Cache, st store.Store, owners *owner.Runtime) *GuildService {
	if owners == nil {
		panic("service: guild owner runtime is required")
	}
	return &GuildService{cache: c, store: st, owners: owners}
}

// RegisterOwners 在装配阶段注册公会操作，确保本地与转发请求共用主人邮箱。
func (s *GuildService) RegisterOwners() error {
	if err := owner.OnCreate(s.owners, "guild.create", s.newGuild, s.keep, guildResult); err != nil {
		return err
	}
	return owner.OnMutate(s.owners, "guild.join", s.loadGuild, guildResult,
		func(_ context.Context, g *entity.Guild, args guildJoinArgs) error {
			if args.PlayerID <= 0 {
				return owner.NewError("guild.bad_arg", "invalid guild join arguments")
			}
			if !g.HasMember(args.PlayerID) {
				g.AddMember(args.PlayerID)
				g.MarkDirty()
			}
			return nil
		})
}

func (s *GuildService) newGuild(_ context.Context, key owner.EntityKey, args guildCreateArgs) (*entity.Guild, error) {
	if key.Kind != "guild" || key.ServerID <= 0 || key.ID <= 0 || args.OID.IsZero() || args.PlayerID <= 0 || args.Name == "" || len(args.Name) > 16 {
		return nil, owner.NewError("guild.bad_arg", "invalid guild creation arguments")
	}
	return &entity.Guild{
		Identity: core.Identity{OID: args.OID},
		GuildID:  key.ID,
		Server:   key.ServerID,
		Name:     args.Name,
		Members:  []int64{args.PlayerID},
	}, nil
}

func (s *GuildService) loadGuild(ctx context.Context, key owner.EntityKey) (*entity.Guild, error) {
	if key.Kind != "guild" || key.ServerID <= 0 || key.ID <= 0 {
		return nil, owner.NewError("guild.bad_arg", "invalid guild join arguments")
	}
	return s.Get(ctx, key.ServerID, key.ID)
}

// Create 新建公会。已有公会的角色不能再创建，避免一个人挂在两个公会里。
func (s *GuildService) Create(ctx *handler.Context, name string) (*GuildResult, error) {
	if name == "" || len(name) > 16 {
		return nil, ErrBadName
	}
	p, err := handler.Player[*entity.Player](ctx)
	if err != nil {
		return nil, err
	}
	if p.Guild() != 0 {
		return nil, ErrAlreadyInGuild
	}
	guildID, err := core.NextBizID(ctx, s.store, ctx.ServerID, "guild")
	if err != nil {
		return nil, err
	}
	result, err := owner.Call[GuildResult](ctx, s.owners, owner.Key(ctx.ServerID, "guild", guildID), "guild.create", guildCreateArgs{
		OID: primitive.NewObjectID(), Name: name, PlayerID: p.PlayerID,
	})
	if err != nil {
		return nil, err
	}
	p.SetGuildID(result.GuildID)
	p.MarkDirty()
	return &result, nil
}

// Join 把角色写入公会成员。调用方只接收快照，不在本地缓存另一份公会。
func (s *GuildService) Join(ctx *handler.Context, guildID int64) (*GuildResult, error) {
	if guildID == 0 {
		return nil, ErrBadArg
	}
	p, err := handler.Player[*entity.Player](ctx)
	if err != nil {
		return nil, err
	}
	if p.Guild() != 0 {
		return nil, ErrAlreadyInGuild
	}
	result, err := owner.Call[GuildResult](ctx, s.owners, owner.Key(ctx.ServerID, "guild", guildID), "guild.join", guildJoinArgs{PlayerID: p.PlayerID})
	if err != nil {
		return nil, err
	}
	p.SetGuildID(result.GuildID)
	p.MarkDirty()
	return &result, nil
}

func guildResult(g *entity.Guild) GuildResult {
	name, members := g.Copy()
	return GuildResult{GuildID: g.GuildID, ServerID: g.Server, Name: name, Members: members}
}

// Get 只应在主人邮箱内调用，缓存未命中时由框架加载公会。
func (s *GuildService) Get(ctx context.Context, serverID int32, guildID int64) (*entity.Guild, error) {
	return cache.GetByBizIDOf[*entity.Guild](ctx, s.cache, s.store, serverID, "guild", guildID)
}

func (s *GuildService) keep(g *entity.Guild) {
	s.cache.Put(g)
	s.cache.Alias(g, idKey(g.GuildID))
}
