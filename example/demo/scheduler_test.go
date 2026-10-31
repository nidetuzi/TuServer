package demo_test

import (
	"context"

	"tuserv/example/demo/service"
	"tuserv/pkg/cache"
	"tuserv/pkg/event"
	"tuserv/pkg/handler"
	"tuserv/pkg/logic"
	"tuserv/pkg/router"
	"tuserv/pkg/scheduler"
	"tuserv/pkg/store/memory"
)

// Example 展示节点任务和在线任务的注册位置，经验奖励仅作为业务接入示例，不默认开启。
func Example() {
	tasks, err := scheduler.New(scheduler.Config{Timezone: "Asia/Shanghai", MaxConcurrent: 64})
	if err != nil {
		panic(err)
	}
	defer tasks.Shutdown(context.Background())
	st := memory.New()
	players := service.NewPlayerService(cache.New(st), st, event.New())
	app := logic.New(router.New[int32](), nil)
	app.KeyLoad = players.GetByKey
	app.Scheduler = tasks
	defer app.Stop()
	app.Hooks.OnOnline = func(session logic.Session) {
		_, err := app.AddPlayerTask(session, "online-exp", "@every 30s", func(ctx *handler.Context) error {
			_, err := players.AddExp(ctx, 1)
			return err
		})
		if err != nil {
			panic(err)
		}
	}
	// 这里只演示不修改实体的节点任务；实体业务必须经由对应运行时进入 actor。
	_, err = tasks.Add("daily-check", "0 5 * * *", func(ctx context.Context) error {
		return ctx.Err()
	})
	if err != nil {
		panic(err)
	}
	if err := tasks.Start(); err != nil {
		panic(err)
	}
	// Output:
}
