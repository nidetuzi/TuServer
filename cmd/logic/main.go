// Package main 启动逻辑节点。
// 刷盘定时器和 Kitex 分开跑，协议处理仍在玩家 actor 里串行。
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tuserv/example/demo"
	"tuserv/example/demo/service"
	node "tuserv/kitex_gen/node"
	logiccli "tuserv/kitex_gen/node/logic"
	"tuserv/pkg/cache"
	"tuserv/pkg/cluster"
	"tuserv/pkg/config"
	"tuserv/pkg/event"
	"tuserv/pkg/log"
	"tuserv/pkg/logic"
	"tuserv/pkg/router"
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
	log.Info("logic start", "addr", cfg.AdvertiseAddr)
	ctx := context.Background()
	store, err := mongostore.Open(ctx, cfg.Mongo.URI, cfg.Mongo.DBPrefix)
	if err != nil {
		log.Error("mongo", "err", err)
		os.Exit(1)
	}
	nodes, err := cluster.Dial(cfg.Etcd.Endpoints)
	if err != nil {
		log.Error("etcd dial", "err", err)
		os.Exit(1)
	}
	bus := event.New()
	c := cache.New(store)
	players := service.NewPlayerService(c, store, bus)
	chat := service.NewChatService(&notifyAll{nodes: nodes})
	guilds := service.NewGuildService(c, store, &guildRemote{self: cfg.AdvertiseAddr, nodes: nodes})
	r := router.New[int32]()
	demo.Register(r, demo.Deps{Players: players, Bags: service.NewBagService(), Chat: chat, Guilds: guilds, Bus: bus})
	app := logic.New(r, players.GetByID)
	app.Internal = applyGuild(c)
	reg, err := cluster.NewRegistry(cfg.Etcd.Endpoints)
	if err != nil {
		log.Error("etcd registry", "err", err)
		os.Exit(1)
	}
	opts, err := cluster.ServerOptions(cfg, reg)
	if err != nil {
		log.Error("listen addr", "err", err)
		os.Exit(1)
	}
	svr := logiccli.NewServer(&logicAPI{app: app, nodes: nodes}, opts...)
	go func() {
		t := time.NewTicker(cfg.FlushEvery())
		defer t.Stop()
		for range t.C {
			if err := c.Flush(context.Background()); err != nil {
				log.Error("flush failed", "err", err)
			}
		}
	}()
	go func() {
		if err := svr.Run(); err != nil {
			log.Error("logic kitex", "err", err)
		}
	}()
	wait()
	if err := c.Flush(context.Background()); err != nil {
		log.Error("flush failed", "err", err)
	}
	app.Stop()
	_ = svr.Stop()
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

func (s *logicAPI) Dispatch(ctx context.Context, req *node.DispatchReq) (*node.DispatchResp, error) {
	resp, err := s.app.Handle(ctx, logic.Request{
		GateID:    req.GetGateId(),
		SessionID: req.GetSessionId(),
		ServerID:  req.GetServerId(),
		PlayerID:  req.GetPlayerId(),
		CmdID:     req.GetCmdId(),
		Body:      req.GetBody(),
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
