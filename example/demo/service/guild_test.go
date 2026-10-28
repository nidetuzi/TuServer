package service

import (
	"context"
	"testing"

	"tuserv/example/demo/entity"
	"tuserv/pkg/cache"
	"tuserv/pkg/handler"
	"tuserv/pkg/store/memory"
)

type fakeRemote struct {
	created bool
}

func (f *fakeRemote) Local(string) bool { return false }

func (f *fakeRemote) Create(context.Context, *entity.Guild) error {
	f.created = true
	return nil
}

func (f *fakeRemote) Join(context.Context, int32, int64, int64) (*entity.Guild, error) {
	return &entity.Guild{GuildID: 9, Name: "remote"}, nil
}

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

func TestCreateForwardsWhenNotOwner(t *testing.T) {
	mem := memory.New()
	c := cache.New(mem)
	players := NewPlayerService(c, mem, nopBus{})
	p, err := players.Login(context.Background(), 1, "ann")
	if err != nil {
		t.Fatal(err)
	}
	remote := &fakeRemote{}
	guilds := NewGuildService(c, mem, remote)
	ctx := handler.New(context.Background(), handler.Meta{ServerID: 1, PlayerID: p.PlayerID}).WithLoader(players.GetByID)
	if _, err := guilds.Create(ctx, "lions"); err != nil {
		t.Fatal(err)
	}
	if !remote.created {
		t.Fatal("非主人节点应把创建转到主人")
	}
	if p.GuildID == 0 {
		t.Fatal("转发成功后应把公会编号记到角色上")
	}
}

type nopBus struct{}

func (nopBus) Publish(context.Context, any) {}
