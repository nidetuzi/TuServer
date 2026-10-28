package demo

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"tuserv/example/demo/entity"
	"tuserv/example/demo/gen/game"
	"tuserv/example/demo/handler"
	"tuserv/example/demo/service"
	"tuserv/pkg/cache"
	core "tuserv/pkg/entity"
	"tuserv/pkg/event"
	"tuserv/pkg/logic"
	"tuserv/pkg/owner"
	"tuserv/pkg/router"
	"tuserv/pkg/store/memory"
)

type recordBroadcaster struct {
	ch       chan int32
	serverID int32
	cmdID    int32
}

func (b *recordBroadcaster) NotifyServer(_ context.Context, serverID, cmdID int32, _ []byte) error {
	b.serverID = serverID
	b.cmdID = cmdID
	b.ch <- serverID
	return nil
}

func TestDemoFlows(t *testing.T) {
	mem := memory.New()
	c := cache.New(mem)
	bus := event.New()
	players := service.NewPlayerService(c, mem, bus)
	bags := service.NewBagService()
	chatBus := &recordBroadcaster{ch: make(chan int32, 1)}
	chat := service.NewChatService(chatBus)
	r := router.New[int32]()
	app := logic.New(r, players.GetByID)
	defer app.Stop()
	owners := owner.NewLocal(app.Actors)
	guilds := service.NewGuildService(c, mem, owners)
	if err := guilds.RegisterOwners(); err != nil {
		t.Fatal(err)
	}
	app.Owners = owners
	Register(r, Deps{Players: players, Bags: bags, Chat: chat, Guilds: guilds, Bus: bus})

	loginBody, _ := proto.Marshal(&game.LoginReq{Name: "ann"})
	resp, err := app.Handle(context.Background(), logic.Request{
		SessionID: "s1", ServerID: 3, CmdID: handler.CmdLogin, Body: loginBody, GateID: "127.0.0.1:9001",
	})
	if err != nil {
		t.Fatal(err)
	}
	var login game.LoginResp
	if err := proto.Unmarshal(resp.Body, &login); err != nil {
		t.Fatal(err)
	}
	if login.GetPlayerId() == 0 || login.GetLevel() != 1 {
		t.Fatalf("登录结果不符: %+v", &login)
	}

	upBody, _ := proto.Marshal(&game.LevelUpReq{Exp: 100})
	resp, err = app.Handle(context.Background(), logic.Request{
		SessionID: "s1", ServerID: 3, PlayerID: login.GetPlayerId(), CmdID: handler.CmdLevelUp, Body: upBody,
	})
	if err != nil {
		t.Fatal(err)
	}
	var up game.LevelUpResp
	if err := proto.Unmarshal(resp.Body, &up); err != nil {
		t.Fatal(err)
	}
	if up.GetLevel() != 2 {
		t.Fatalf("升级后等级应为 2: %+v", &up)
	}
	p, err := players.GetByID(context.Background(), 3, login.GetPlayerId())
	if err != nil {
		t.Fatal(err)
	}
	if p.(*entity.Player).BagCount(entity.RewardItemID) != 1 {
		t.Fatal("升级事件应放入 1 个奖励道具")
	}

	useBody, _ := proto.Marshal(&game.UseItemReq{ItemId: entity.RewardItemID, Count: 1})
	if _, err := app.Handle(context.Background(), logic.Request{
		SessionID: "s1", ServerID: 3, PlayerID: login.GetPlayerId(), CmdID: handler.CmdUseItem, Body: useBody,
	}); err != nil {
		t.Fatal(err)
	}
	_, err = app.Handle(context.Background(), logic.Request{
		SessionID: "s1", ServerID: 3, PlayerID: login.GetPlayerId(), CmdID: handler.CmdUseItem, Body: useBody,
	})
	if !errors.Is(err, service.ErrNotEnough) {
		t.Fatalf("道具不足应返回业务错误: %v", err)
	}

	chatBody, _ := proto.Marshal(&game.ChatReq{Text: "hi"})
	if _, err := app.Handle(context.Background(), logic.Request{
		SessionID: "s1", ServerID: 3, PlayerID: login.GetPlayerId(), CmdID: handler.CmdChat, Body: chatBody,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case sid := <-chatBus.ch:
		if sid != 3 || chatBus.cmdID != service.CmdChatPush {
			t.Fatalf("广播目标不符: server=%d cmd=%d", sid, chatBus.cmdID)
		}
	case <-time.After(time.Second):
		t.Fatal("聊天没有发出区服广播")
	}

	gBody, _ := proto.Marshal(&game.CreateGuildReq{Name: "lions"})
	resp, err = app.Handle(context.Background(), logic.Request{
		SessionID: "s1", ServerID: 3, PlayerID: login.GetPlayerId(), CmdID: handler.CmdCreateGuild, Body: gBody,
	})
	if err != nil {
		t.Fatal(err)
	}
	var gr game.GuildResp
	if err := proto.Unmarshal(resp.Body, &gr); err != nil {
		t.Fatal(err)
	}
	ge, err := c.Get(context.Background(), 3, "guild", strconv.FormatInt(gr.GetGuildId(), 10), func() core.Entity { return &entity.Guild{} })
	if err != nil {
		t.Fatal(err)
	}
	if !ge.(*entity.Guild).HasMember(login.GetPlayerId()) {
		t.Fatal("创建者应在公会成员里")
	}
}
