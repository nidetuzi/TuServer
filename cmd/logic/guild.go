package main

import (
	"context"
	"encoding/json"
	"strconv"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"tuserv/example/demo/entity"
	"tuserv/example/demo/service"
	"tuserv/pkg/cluster"
	"tuserv/pkg/logic"
)

type guildOp struct {
	Action   string  `json:"action"`
	OID      string  `json:"oid"`
	GuildID  int64   `json:"guild_id"`
	Server   int32   `json:"server"`
	Name     string  `json:"name"`
	Members  []int64 `json:"members"`
	PlayerID int64   `json:"player_id"`
}

type guildRemote struct {
	self  string
	nodes *cluster.Nodes
}

func (g *guildRemote) Local(key string) bool {
	list, err := g.nodes.List(context.Background(), "Logic")
	if err != nil || len(list) == 0 {
		return true
	}
	owner, err := cluster.Owner(list, key)
	if err != nil {
		return true
	}
	return owner.Addr == g.self
}

func (g *guildRemote) Create(ctx context.Context, guild *entity.Guild) error {
	return g.call(ctx, guild.RouteKey(), guildOp{
		Action: "create", OID: guild.OID.Hex(), GuildID: guild.GuildID,
		Server: guild.ServerID(), Name: guild.Name, Members: append([]int64(nil), guild.Members...),
	})
}

func (g *guildRemote) Join(ctx context.Context, serverID int32, guildID, playerID int64) (*entity.Guild, error) {
	key := strconv.FormatInt(int64(serverID), 10) + ":" + strconv.FormatInt(guildID, 10)
	op := guildOp{Action: "join", Server: serverID, GuildID: guildID, PlayerID: playerID}
	body, err := json.Marshal(op)
	if err != nil {
		return nil, err
	}
	owner, err := g.owner(key)
	if err != nil {
		return nil, err
	}
	resp, err := g.nodes.Dispatch(ctx, owner, logic.Request{CmdID: 0, SessionID: key, ServerID: serverID, Body: body})
	if err != nil {
		return nil, err
	}
	var back guildOp
	if err := json.Unmarshal(resp.Body, &back); err != nil {
		return nil, err
	}
	oid, err := primitive.ObjectIDFromHex(back.OID)
	if err != nil {
		return nil, err
	}
	return &entity.Guild{OID: oid, GuildID: back.GuildID, Server: back.Server, Name: back.Name, Members: back.Members}, nil
}

func (g *guildRemote) call(ctx context.Context, key string, op guildOp) error {
	body, err := json.Marshal(op)
	if err != nil {
		return err
	}
	owner, err := g.owner(key)
	if err != nil {
		return err
	}
	_, err = g.nodes.Dispatch(ctx, owner, logic.Request{CmdID: 0, SessionID: key, ServerID: op.Server, Body: body})
	return err
}

func (g *guildRemote) owner(key string) (string, error) {
	list, err := g.nodes.List(context.Background(), "Logic")
	if err != nil {
		return "", err
	}
	inst, err := cluster.Owner(list, key)
	if err != nil {
		return "", err
	}
	return inst.Addr, nil
}

func applyGuild(guilds *service.GuildService) func(context.Context, logic.Request) (logic.Response, error) {
	return func(ctx context.Context, req logic.Request) (logic.Response, error) {
		var op guildOp
		if err := json.Unmarshal(req.Body, &op); err != nil {
			return logic.Response{}, err
		}
		switch op.Action {
		case "create":
			oid, err := primitive.ObjectIDFromHex(op.OID)
			if err != nil {
				return logic.Response{}, err
			}
			g := &entity.Guild{OID: oid, GuildID: op.GuildID, Server: op.Server, Name: op.Name, Members: op.Members}
			guilds.Keep(g)
			return logic.Response{}, nil
		case "join":
			g, err := guilds.Load(ctx, op.Server, op.GuildID)
			if err != nil {
				return logic.Response{}, err
			}
			g.AddMember(op.PlayerID)
			g.MarkDirty()
			g.Lock()
			back := guildOp{OID: g.OID.Hex(), GuildID: g.GuildID, Server: g.ServerID(), Name: g.Name, Members: append([]int64(nil), g.Members...)}
			g.Unlock()
			body, err := json.Marshal(back)
			return logic.Response{Body: body}, err
		default:
			return logic.Response{}, nil
		}
	}
}
