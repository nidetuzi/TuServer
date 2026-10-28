package handler

import (
	"tuserv/example/demo/gen/game"
	"tuserv/example/demo/service"
	"tuserv/pkg/handler"
)

// OnCreateGuild 校验公会名。成员写入由 service 在主人节点完成。
func OnCreateGuild(svc *service.GuildService) func(*handler.Context, *game.CreateGuildReq) (*game.GuildResp, error) {
	return func(ctx *handler.Context, req *game.CreateGuildReq) (*game.GuildResp, error) {
		if req.GetName() == "" || len(req.GetName()) > 16 {
			return nil, service.ErrBadName
		}
		g, err := svc.Create(ctx, req.GetName())
		if err != nil {
			return nil, err
		}
		return &game.GuildResp{GuildId: g.GuildID, Name: g.Name}, nil
	}
}

// OnJoinGuild 校验公会编号。已入会的判断在 service 里，因为要读到角色。
func OnJoinGuild(svc *service.GuildService) func(*handler.Context, *game.JoinGuildReq) (*game.GuildResp, error) {
	return func(ctx *handler.Context, req *game.JoinGuildReq) (*game.GuildResp, error) {
		if req.GetGuildId() == 0 {
			return nil, service.ErrBadArg
		}
		g, err := svc.Join(ctx, req.GetGuildId())
		if err != nil {
			return nil, err
		}
		return &game.GuildResp{GuildId: g.GuildID, Name: g.Name}, nil
	}
}
