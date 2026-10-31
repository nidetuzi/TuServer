// Package main 启动网关进程。
// 先初始化日志再监听，这样接入失败和会话绑定写在同一份日志里。
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/cloudwego/kitex/pkg/klog"

	node "tuserv/kitex_gen/node"
	gatecli "tuserv/kitex_gen/node/gate"
	"tuserv/pkg/cluster"
	"tuserv/pkg/config"
	"tuserv/pkg/gate"
	"tuserv/pkg/log"
	"tuserv/pkg/protocol"
	"tuserv/pkg/protocol/tcp"
	"tuserv/pkg/protocol/ws"
)

func main() {
	path := "config/gate.yaml"
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
	secret, err := cfg.ClusterKey()
	if err != nil {
		log.Error("cluster secret", "err", err)
		os.Exit(1)
	}
	log.Info("gate start", "addr", cfg.AdvertiseAddr)
	nodes, err := cluster.Dial(cfg.Etcd.Endpoints, secret)
	if err != nil {
		log.Error("etcd dial", "err", err)
		os.Exit(1)
	}
	g := gate.New(cfg.AdvertiseAddr, nodes, protocol.FrameCodec{})
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
	svr := gatecli.NewServer(&pushAPI{gate: g}, opts...)
	go func() {
		if err := svr.Run(); err != nil {
			klog.Errorf("gate kitex: %v", err)
		}
	}()
	var tcpSrv *tcp.Server
	var wsSrv *ws.Server
	if cfg.TCPAddr != "" {
		tcpSrv, err = tcp.Serve(cfg.TCPAddr, protocol.FrameCodec{}, g)
		if err != nil {
			log.Error("tcp serve", "err", err)
			os.Exit(1)
		}
		log.Info("tcp listen", "addr", cfg.TCPAddr)
	}
	if cfg.WSAddr != "" {
		wsSrv, err = ws.Serve(cfg.WSAddr, protocol.FrameCodec{}, g)
		if err != nil {
			log.Error("ws serve", "err", err)
			os.Exit(1)
		}
		log.Info("ws listen", "addr", cfg.WSAddr)
	}
	wait()
	if tcpSrv != nil {
		tcpSrv.Stop()
	}
	if wsSrv != nil {
		wsSrv.Stop()
	}
	_ = svr.Stop()
	log.Info("gate exit")
}

type pushAPI struct{ gate *gate.Gate }

func (p *pushAPI) Push(_ context.Context, req *node.PushReq) (*node.PushResp, error) {
	err := p.gate.Push(req.GetSessionId(), req.GetCmdId(), 0, req.GetBody())
	return &node.PushResp{}, err
}

func wait() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}
