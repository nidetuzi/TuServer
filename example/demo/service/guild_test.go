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
	return &entity.Guild{ID: "9", GuildID: 9, Name: "remote"}, nil
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
