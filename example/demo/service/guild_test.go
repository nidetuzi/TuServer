package service

import (
	"context"
	"errors"
	"testing"

	"tuserv/example/demo/entity"
	"tuserv/pkg/actor"
	"tuserv/pkg/cache"
	"tuserv/pkg/handler"
	"tuserv/pkg/owner"
	"tuserv/pkg/store"
	"tuserv/pkg/store/memory"
)

func TestLoginUsesObjectIDAndIncrement(t *testing.T) {
	mem := memory.New()
	c := cache.New(mem)
	players := NewPlayerService(c, mem, nopBus{})
	a, err := players.Login(context.Background(), 1, "ann")
	if err != nil {
		t.Fatal(err)
	}
	b, err := players.Login(context.Background(), 1, "bob")
	if err != nil {
		t.Fatal(err)
	}
	if a.OID.IsZero() || a.OID == b.OID {
		t.Fatal("每个角色应有各自的 Mongo _id")
	}
	if a.PlayerID != 100001 || b.PlayerID != 100002 {
		t.Fatalf("编号应带上区服: %d %d", a.PlayerID, b.PlayerID)
	}
	if err := c.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	again, err := NewPlayerService(cache.New(mem), mem, nopBus{}).GetByID(context.Background(), 1, a.PlayerID)
	if err != nil {
		t.Fatal(err)
	}
	if again.(*entity.Player).OID != a.OID {
		t.Fatal("按自增号读回应得到原来的 _id")
	}
}

func TestGuildRemoteCreateJoinOnlyOwnerCaches(t *testing.T) {
	mem := memory.New()
	callerCache, ownerCache := cache.New(mem), cache.New(mem)
	caller, remote := pairedGuildServices(t, mem, callerCache, ownerCache, nil)
	players := NewPlayerService(callerCache, mem, nopBus{})
	creator, createCtx := guildPlayer(t, players, "ann")
	result, err := caller.Create(createCtx, "lions")
	if err != nil {
		t.Fatal(err)
	}
	if result.GuildID != 100001 || creator.GuildID != result.GuildID {
		t.Fatalf("创建后角色应记住带区服的公会编号: %+v", result)
	}
	g, err := remote.Get(context.Background(), 1, result.GuildID)
	if err != nil {
		t.Fatal(err)
	}
	if g.OID.IsZero() || g.RouteKey() != owner.Key(1, "guild", g.GuildID).String() || !g.Dirty() {
		t.Fatal("主人应持有带 ObjectID、统一路由键和脏标记的公会")
	}
	assertGuildNotCached(t, callerCache, g)
	result.Name, result.Members[0] = "changed", -1
	if g.Name != "lions" || !g.HasMember(creator.PlayerID) {
		t.Fatal("修改返回快照不能影响主人实体")
	}
	joiner, joinCtx := guildPlayer(t, players, "bob")
	joined, err := caller.Join(joinCtx, g.GuildID)
	if err != nil {
		t.Fatal(err)
	}
	if joiner.GuildID != g.GuildID || !g.HasMember(joiner.PlayerID) || len(joined.Members) != 2 {
		t.Fatal("远端加入应更新主人成员和本地角色")
	}
	joined.Members[1] = -1
	if !g.HasMember(joiner.PlayerID) {
		t.Fatal("加入快照的切片不能引用主人实体")
	}
	assertGuildNotCached(t, callerCache, g)
	if err := ownerCache.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Call[GuildResult](joinCtx, caller.owners, owner.Key(1, "guild", g.GuildID), "guild.join", guildJoinArgs{PlayerID: joiner.PlayerID}); err != nil {
		t.Fatal(err)
	}
	if len(g.Members) != 2 || g.Dirty() {
		t.Fatal("主人收到重复加入不能重复成员或产生无意义脏标记")
	}
}

func TestGuildLocalFlowAndGetReload(t *testing.T) {
	mem := memory.New()
	c := cache.New(mem)
	actors := actor.NewSystem()
	t.Cleanup(actors.Stop)
	guilds := NewGuildService(c, mem, owner.NewLocal(actors))
	if err := guilds.RegisterOwners(); err != nil {
		t.Fatal(err)
	}
	players := NewPlayerService(c, mem, nopBus{})
	creator, ctx := guildPlayer(t, players, "ann")
	result, err := guilds.Create(ctx, "lions")
	if err != nil {
		t.Fatal(err)
	}
	g, err := guilds.Get(ctx, 1, result.GuildID)
	if err != nil {
		t.Fatal(err)
	}
	result.Members[0] = -1
	if !g.HasMember(creator.PlayerID) {
		t.Fatal("本地运行时也必须隔离返回快照")
	}
	joiner, joinCtx := guildPlayer(t, players, "bob")
	if _, err := guilds.Join(joinCtx, result.GuildID); err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	reloaded := NewGuildService(cache.New(mem), mem, owner.NewLocal(actors))
	again, err := reloaded.Get(ctx, 1, result.GuildID)
	if err != nil {
		t.Fatal(err)
	}
	if again.OID != g.OID || again.Name != "lions" || !again.HasMember(joiner.PlayerID) {
		t.Fatal("Get 未命中应由缓存加载原公会和成员")
	}
	cached, err := reloaded.Get(ctx, 1, result.GuildID)
	if err != nil || cached != again {
		t.Fatal("Get 应复用同一份已加载公会")
	}
}

func TestGuildFailuresDoNotChangePlayer(t *testing.T) {
	mem := memory.New()
	c := cache.New(mem)
	blocked := true
	caller, _ := pairedGuildServices(t, mem, c, cache.New(mem), &blocked)
	players := NewPlayerService(c, mem, nopBus{})
	p, ctx := guildPlayer(t, players, "ann")
	if err := c.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := caller.Create(ctx, "lions"); err == nil {
		t.Fatal("传输失败应返回错误")
	}
	if _, err := caller.Join(ctx, 100001); err == nil {
		t.Fatal("加入传输失败应返回错误")
	}
	if p.GuildID != 0 || p.Dirty() {
		t.Fatal("主人失败不能修改角色公会或脏标记")
	}
	blocked = false
	if _, err := caller.Join(ctx, 100001); !errors.Is(err, owner.NewError("guild.not_found", "")) {
		t.Fatalf("远端不存在错误应保留错误码: %v", err)
	}
	if p.GuildID != 0 || p.Dirty() {
		t.Fatal("公会不存在不能修改角色")
	}
	if _, err := caller.Create(ctx, ""); !errors.Is(err, ErrBadName) {
		t.Fatalf("空名称应在调用主人前拒绝: %v", err)
	}
	if _, err := caller.Join(ctx, 0); !errors.Is(err, ErrBadArg) {
		t.Fatalf("空编号应在调用主人前拒绝: %v", err)
	}
}

func TestGuildOwnerRejectsWrongEntityKind(t *testing.T) {
	actors := actor.NewSystem()
	t.Cleanup(actors.Stop)
	runtime := owner.NewLocal(actors)
	guilds := NewGuildService(cache.New(nil), nil, runtime)
	if err := guilds.RegisterOwners(); err != nil {
		t.Fatal(err)
	}
	_, err := owner.Call[GuildResult](context.Background(), runtime, owner.Key(1, "room", 100001), "guild.join", guildJoinArgs{PlayerID: 100001})
	if !errors.Is(err, owner.NewError("guild.bad_arg", "")) {
		t.Fatalf("公会操作不能借用其他实体邮箱: %v", err)
	}
}

func TestGuildRequiresRuntime(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("缺少主人运行时不能静默切换为本地写入")
		}
	}()
	NewGuildService(nil, nil, nil)
}

func TestGuildRegisterOwnersRejectsDuplicate(t *testing.T) {
	actors := actor.NewSystem()
	t.Cleanup(actors.Stop)
	guilds := NewGuildService(nil, nil, owner.NewLocal(actors))
	if err := guilds.RegisterOwners(); err != nil {
		t.Fatal(err)
	}
	if err := guilds.RegisterOwners(); err == nil {
		t.Fatal("装配阶段重复注册应报错")
	}
}

func pairedGuildServices(t *testing.T, st store.Store, callerCache, remoteCache *cache.Cache, blocked *bool) (*GuildService, *GuildService) {
	t.Helper()
	localActors, remoteActors := actor.NewSystem(), actor.NewSystem()
	t.Cleanup(localActors.Stop)
	t.Cleanup(remoteActors.Stop)
	runtimes := make(map[string]*owner.Runtime)
	resolve := func(context.Context, owner.EntityKey) (string, error) { return "remote", nil }
	transport := func(ctx context.Context, address string, payload []byte) ([]byte, error) {
		if blocked != nil && *blocked {
			return nil, errors.New("transport unavailable")
		}
		return runtimes[address].Handle(ctx, payload)
	}
	runtimes["local"] = owner.New("local", localActors, resolve, transport)
	runtimes["remote"] = owner.New("remote", remoteActors, resolve, transport)
	caller := NewGuildService(callerCache, st, runtimes["local"])
	remote := NewGuildService(remoteCache, st, runtimes["remote"])
	for _, svc := range []*GuildService{caller, remote} {
		if err := svc.RegisterOwners(); err != nil {
			t.Fatal(err)
		}
	}
	return caller, remote
}

func guildPlayer(t *testing.T, players *PlayerService, name string) (*entity.Player, *handler.Context) {
	t.Helper()
	p, err := players.Login(context.Background(), 1, name)
	if err != nil {
		t.Fatal(err)
	}
	ctx := handler.New(context.Background(), handler.Meta{ServerID: 1, PlayerID: p.PlayerID}).WithLoader(players.GetByID)
	return p, ctx
}

func assertGuildNotCached(t *testing.T, c *cache.Cache, g *entity.Guild) {
	t.Helper()
	for _, key := range []string{g.CacheKey(), idKey(g.GuildID)} {
		if _, err := c.Get(context.Background(), 1, "guild", key, nil); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("非主人不能缓存公会 %s: %v", key, err)
		}
	}
}

type nopBus struct{}

func (nopBus) Publish(context.Context, any) {}
