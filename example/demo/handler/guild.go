package handler

import (
	"tuserv/example/demo/gen/game"
	"tuserv/example/demo/service"
	"tuserv/pkg/handler"
)

// OnCreateGuild 公会名门槛由 service 校验；成员写入在主人节点完成。
func OnCreateGuild(svc *service.GuildService) func(*handler.Context, *game.CreateGuildReq) (*game.GuildResp, error) {
	return func(ctx *handler.Context, req *game.CreateGuildReq) (*game.GuildResp, error) {
		g, err := svc.Create(ctx, req.GetName())
		if err != nil {
			return nil, err
		}
		return &game.GuildResp{GuildId: g.GuildID, Name: g.Name}, nil
	}
}

// OnJoinGuild 公会编号门槛由 service 校验。
func OnJoinGuild(svc *service.GuildService) func(*handler.Context, *game.JoinGuildReq) (*game.GuildResp, error) {
	return func(ctx *handler.Context, req *game.JoinGuildReq) (*game.GuildResp, error) {
		g, err := svc.Join(ctx, req.GetGuildId())
		if err != nil {
			return nil, err
		}
		return &game.GuildResp{GuildId: g.GuildID, Name: g.Name}, nil
	}
}
