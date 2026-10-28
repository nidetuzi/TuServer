// Package main 启动跨服节点占位进程。
// 现在只注册服务名并应答 Ping，跨服玩法以后再接到这个进程上。
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	node "tuserv/kitex_gen/node"
	crosscli "tuserv/kitex_gen/node/cross"
	"tuserv/pkg/cluster"
	"tuserv/pkg/config"
	"tuserv/pkg/log"
)

func main() {
	path := "config/cross.yaml"
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
	log.Info("cross start", "addr", cfg.AdvertiseAddr)
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
	svr := crosscli.NewServer(crossAPI{}, opts...)
	go func() {
		if err := svr.Run(); err != nil {
			log.Error("cross kitex", "err", err)
		}
	}()
	wait()
	_ = svr.Stop()
	log.Info("cross exit")
}

type crossAPI struct{}

func (crossAPI) Ping(context.Context, *node.PingReq) (*node.PingResp, error) {
	return &node.PingResp{}, nil
}

func wait() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}
