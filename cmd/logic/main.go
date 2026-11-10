// Package main 启动逻辑节点。
// 续租、刷盘和失联网关清理各自一个循环，协议处理仍在玩家 actor 里串行。
package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"tuserv/example/demo"
	node "tuserv/kitex_gen/node"
	logiccli "tuserv/kitex_gen/node/logic"
	"tuserv/pkg/cache"
	"tuserv/pkg/cluster"
	"tuserv/pkg/config"
	"tuserv/pkg/event"
	"tuserv/pkg/log"
	"tuserv/pkg/logic"
	"tuserv/pkg/router"
	routepb "tuserv/pkg/router/protobuf"
	"tuserv/pkg/scheduler"
	mongostore "tuserv/pkg/store/mongo"
)

func main() {
	path := "config/logic.yaml"
	if len(os.Args) > 1 {
		path = os.Args[1]
	}
	cfg, err := config.Load(path)
	if err != nil {
		panic(err)
	}
	if err := log.Init(cfg.LogConfig()); err != nil {
		panic(err)
	}
	log.BindKitex()
	schedulerConfig, err := cfg.SchedulerConfig()
	if err != nil {
		log.Error("scheduler config", "err", err)
		os.Exit(1)
	}
	tasks, err := scheduler.New(schedulerConfig)
	if err != nil {
		log.Error("scheduler init", "err", err)
		os.Exit(1)
	}
	secret, err := cfg.ClusterKey()
	if err != nil {
		log.Error("cluster secret", "err", err)
		os.Exit(1)
	}
	log.Info("logic start", "addr", cfg.AdvertiseAddr)
	ctx := context.Background()
	store, err := mongostore.Open(ctx, cfg.Mongo.URI, cfg.Mongo.DBPrefix)
	if err != nil {
		log.Error("mongo", "err", err)
		os.Exit(1)
	}
	nodes, err := cluster.Dial(cfg.Etcd.Endpoints, secret)
	if err != nil {
		log.Error("etcd dial", "err", err)
		os.Exit(1)
	}
	bus := event.New()
	c := cache.New(store, cache.WithPolicy(cache.Policy{
		MaxItems: cfg.CacheMaxItems(),
		Idle:     cfg.CacheIdle(),
	}))
	dayLocation, err := time.LoadLocation(schedulerConfig.Timezone)
	if schedulerConfig.Timezone == "" {
		dayLocation, err = time.LoadLocation("Asia/Shanghai")
	}
	if err != nil {
		log.Error("player day timezone", "err", err)
		os.Exit(1)
	}
	r := router.New[int32]()
	app := logic.New(r, nil)
	app.Scheduler = tasks
	self := cfg.AdvertiseAddr
	if self == "" {
		self = cfg.ListenAddr
	}
	owners := newOwners(self, app.Actors, nodes, store)
	owners.Guard = c.Guard
	app.Hooks = logic.Hooks{
		Self:   self,
		Leases: store,
		AskRelease: func(ctx context.Context, host string, body []byte) error {
			_, err := nodes.Dispatch(ctx, host, logic.Request{GateID: logic.InternalCaller, CmdID: logic.CmdRelinquish, Internal: true, Body: body})
			return err
		},
		Kick: func(ctx context.Context, gateAddr, sessionID string) {
			_ = nodes.Push(ctx, gateAddr, sessionID, logic.CmdKick, nil)
		},
		Guard: c.Guard,
	}
	if err := (demo.Module{}).Wire(&logic.Host{
		App:         app,
		Router:      r,
		Cache:       c,
		Store:       store,
		Owners:      owners,
		Bus:         bus,
		BodyCodec:   routepb.Body(),
		Self:        self,
		DayLocation: dayLocation,
		Now:         time.Now,
		Broadcast:   &notifyAll{nodes: nodes},
	}); err != nil {
		log.Error("wire demo module", "err", err)
		os.Exit(1)
	}
	reg, err := cluster.NewRegistry(cfg.Etcd.Endpoints)
	if err != nil {
		log.Error("etcd registry", "err", err)
		os.Exit(1)
	}
	opts, err := cluster.ServerOptions(cfg, reg)
	if err != nil {
		log.Error("kitex server", "err", err)
		os.Exit(1)
	}
	svr := logiccli.NewServer(&logicAPI{app: app, nodes: nodes}, opts...)
	app.ReapParallel = cfg.ReapParallel()
	if err := tasks.Start(); err != nil {
		log.Error("scheduler start", "err", err)
		os.Exit(1)
	}
	stopTicker := make(chan struct{})
	var tickerOnce sync.Once
	tickerDone := make(chan struct{})
	probeEvery := cfg.GateProbeEvery()
	misses := cfg.GateMissLimit()
	go func() {
		defer close(tickerDone)
		logic.Loops{
			RenewEvery: cfg.RenewEvery(),
			FlushEvery: cfg.FlushEvery(),
			ProbeEvery: probeEvery,
			Renew: func(ctx context.Context) {
				app.RenewLeases(ctx)
			},
			Flush: func(ctx context.Context) {
				if err := c.Flush(ctx); err != nil {
					log.Error("flush failed", "err", err)
				}
				if err := c.Evict(ctx); err != nil {
					log.Error("evict failed", "err", err)
				}
			},
			Probe: func(ctx context.Context) {
				pctx, cancel := context.WithTimeout(ctx, probeEvery)
				addrs, err := nodes.Gates(pctx)
				cancel()
				if err != nil {
					log.Warn("gate probe", "err", err)
					return
				}
				app.ReapGates(ctx, addrs, misses)
			},
		}.Run(stopTicker)
	}()
	go func() {
		if err := svr.Run(); err != nil {
			log.Error("logic kitex", "err", err)
		}
	}()
	wait()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = logic.ShutdownSteps{
		StopAccept: func(ctx context.Context) error {
			done := make(chan struct{})
			go func() {
				_ = svr.Stop()
				close(done)
			}()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		StopTasks: tasks.Shutdown,
		StopTicker: func() {
			tickerOnce.Do(func() { close(stopTicker) })
			select {
			case <-tickerDone:
			case <-shutdownCtx.Done():
			}
		},
		Drain: app.Shutdown,
		Flush: func(ctx context.Context) error {
			flushCtx, flushCancel := context.WithTimeout(ctx, 8*time.Second)
			defer flushCancel()
			if err := c.Flush(flushCtx); err != nil {
				log.Error("flush failed", "err", err)
				return err
			}
			return nil
		},
	}.Run(shutdownCtx)
	if err != nil {
		log.Error("logic shutdown", "err", err)
	}
	_ = store.Close(context.Background())
	log.Info("logic exit")
}

type notifyAll struct{ nodes *cluster.Nodes }

func (n *notifyAll) NotifyServer(ctx context.Context, serverID, cmdID int32, body []byte) error {
	return n.nodes.NotifyAll(ctx, serverID, cmdID, body)
}

type logicAPI struct {
	app   *logic.App
	nodes *cluster.Nodes
}

// Dispatch 把网关转发的一帧送进玩家 actor。
// 返回的 error 只留给 Kitex，表示传输或节点没有正常结束。参数、库存、路由和玩法 panic 会变成统一错误包，error 保持 nil，网关才不会拆掉绑定。
func (s *logicAPI) Dispatch(ctx context.Context, req *node.DispatchReq) (*node.DispatchResp, error) {
	resp, err := s.app.Handle(ctx, logic.Request{
		GateID:    req.GetGateId(),
		SessionID: req.GetSessionId(),
		ServerID:  req.GetServerId(),
		PlayerID:  req.GetPlayerId(),
		CmdID:     req.GetCmdId(),
		Body:      req.GetBody(),
		Internal:  cluster.IsInternalCall(ctx),
	})
	if err != nil {
		return nil, err
	}
	return &node.DispatchResp{PlayerId: resp.PlayerID, CmdId: resp.CmdID, Body: resp.Body}, nil
}

func (s *logicAPI) NotifyServer(ctx context.Context, req *node.NotifyReq) (*node.NotifyResp, error) {
	for _, sess := range s.app.Sessions(req.GetServerId()) {
		if err := s.nodes.Push(ctx, sess.GateAddr, sess.ID, req.GetCmdId(), req.GetBody()); err != nil {
			log.Warn("push", "err", err, "session_id", sess.ID)
		}
	}
	return &node.NotifyResp{}, nil
}

func wait() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}
