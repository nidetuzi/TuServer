package demo

import (
	"tuserv/example/demo/service"
	"tuserv/pkg/handler"
	"tuserv/pkg/log"
	"tuserv/pkg/logic"
)

// BindPlayerLifecycle 在接入前装配跨天与离线业务，在线任务使用同一个幂等检查入口。
// 每三十秒检查可在零点任务被限流或邮箱繁忙后自动补检查，不依赖单次零点触发。
func BindPlayerLifecycle(app *logic.App, players *service.PlayerService) {
	app.Hooks.OnPlayerOnline = players.OnOnline
	app.Hooks.OnPlayerOffline = players.OnOffline
	app.Hooks.OnOnline = func(session logic.Session) {
		_, err := app.AddPlayerTask(session, "new-day", "@every 30s", func(ctx *handler.Context) error {
			_, err := players.CheckNewDay(ctx)
			return err
		})
		if err != nil {
			log.Error("register player new day", "err", err, "server_id", session.ServerID, "player_id", session.PlayerID)
		}
	}
}
