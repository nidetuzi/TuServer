// Package demo 把演示协议注册到 int32 路由上。
// 集中注册是为了进程入口不用知道每条协议的校验细节。
package demo

import (
	"tuserv/example/demo/event"
	"tuserv/example/demo/handler"
	"tuserv/example/demo/service"
	fw "tuserv/pkg/event"
	"tuserv/pkg/router"
	routepb "tuserv/pkg/router/protobuf"
)

// Deps 是演示玩法依赖的服务。
type Deps struct {
	Players *service.PlayerService
	Bags    *service.BagService
	Chat    *service.ChatService
	Guilds  *service.GuildService
	Bus     *fw.Bus
	// Codec 为空时使用 protobuf 适配，便于测试与换协议。
	Codec router.BodyCodec
}

// Register 把 1001 到 1006 注册到路由，并挂上升级奖励。
// 监听者必须在第一条升级请求之前注册，否则第一次升级不会发道具。
func Register(r *router.Router[int32], d Deps) {
	codec := d.Codec
	if codec == nil {
		codec = routepb.Body()
	}
	router.Register(r, handler.CmdLogin, codec, handler.OnLogin(d.Players))
	router.Register(r, handler.CmdLevelUp, codec, handler.OnLevelUp(d.Players))
	router.Register(r, handler.CmdUseItem, codec, handler.OnUseItem(d.Bags))
	router.Register(r, handler.CmdChat, codec, handler.OnChat(d.Chat))
	router.Register(r, handler.CmdCreateGuild, codec, handler.OnCreateGuild(d.Guilds))
	router.Register(r, handler.CmdJoinGuild, codec, handler.OnJoinGuild(d.Guilds))
	if d.Bus != nil {
		fw.On(d.Bus, event.OnLevelUpReward)
	}
}
