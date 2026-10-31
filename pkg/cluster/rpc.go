package cluster

import (
	"context"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/client/callopt"
	"github.com/cloudwego/kitex/pkg/discovery"
	"github.com/cloudwego/kitex/pkg/registry"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/server"
	etcd "github.com/kitex-contrib/registry-etcd"

	gatecli "tuserv/kitex_gen/node/gate"
	logiccli "tuserv/kitex_gen/node/logic"
	"tuserv/pkg/config"
	"tuserv/pkg/log"
	"tuserv/pkg/logic"
)

// Nodes 是基于 Kitex 和 etcd 的节点客户端。
type Nodes struct {
	logic    logiccli.Client
	gate     gatecli.Client
	resolver discovery.Resolver
	mu       sync.Mutex
	rand     *rand.Rand
}

// Dial 连接注册中心并准备 Logic、Gate 两个客户端。
// secret 为空时直接失败，避免发出没有签名的节点调用。
func Dial(endpoints []string, secret string) (*Nodes, error) {
	if strings.TrimSpace(secret) == "" {
		return nil, ErrMissingSecret
	}
	ensureLeaseTTL()
	resolver, err := etcd.NewEtcdResolver(endpoints)
	if err != nil {
		return nil, err
	}
	logicOpts := append([]client.Option{client.WithResolver(resolver)}, authClientOpts(secret)...)
	logicClient, err := logiccli.NewClient("Logic", logicOpts...)
	if err != nil {
		return nil, err
	}
	gateOpts := append([]client.Option{client.WithResolver(resolver)}, authClientOpts(secret)...)
	gateClient, err := gatecli.NewClient("Gate", gateOpts...)
	if err != nil {
		return nil, err
	}
	return &Nodes{
		logic:    logicClient,
		gate:     gateClient,
		resolver: resolver,
		rand:     rand.New(rand.NewSource(rand.Int63())),
	}, nil
}

// NewRegistry 创建 etcd 注册器。租约默认 5 秒，节点崩溃后网关不必等一分钟才摘掉它。
func NewRegistry(endpoints []string) (registry.Registry, error) {
	ensureLeaseTTL()
	return etcd.NewEtcdRegistry(endpoints)
}

// ServerOptions 把监听地址、服务名、权重和节点间验签交给 Kitex。
// 没有共享密钥时返回错误，调用方不应再监听。
func ServerOptions(cfg config.File, reg registry.Registry) ([]server.Option, error) {
	secret, err := cfg.ClusterKey()
	if err != nil {
		return nil, err
	}
	listen, err := net.ResolveTCPAddr("tcp", cfg.ListenAddr)
	if err != nil {
		return nil, err
	}
	if listen.IP == nil || listen.IP.IsUnspecified() {
		log.Warn("kitex listens on all interfaces", "addr", cfg.ListenAddr)
	}
	adv := listen
	if cfg.AdvertiseAddr != "" {
		adv, err = net.ResolveTCPAddr("tcp", cfg.AdvertiseAddr)
		if err != nil {
			return nil, err
		}
	}
	weight := cfg.Weight
	if weight <= 0 {
		weight = 10
	}
	name := cfg.ServiceName
	// 注册对外地址而不是监听通配地址，网关才能按权重选中后连上来。
	opts := []server.Option{
		server.WithServiceAddr(listen),
		server.WithRegistry(reg),
		server.WithServerBasicInfo(&rpcinfo.EndpointBasicInfo{ServiceName: name}),
		server.WithRegistryInfo(&registry.Info{
			ServiceName:    name,
			Weight:         weight,
			Addr:           adv,
			SkipListenAddr: true,
		}),
	}
	return append(opts, authServerOpts(secret)...), nil
}

// Dispatch 把一帧发到指定逻辑节点。地址来自会话绑定，不再做负载均衡。
// Internal 只影响这一跳的签名，不会把调用方上下文里的旧标记带到别的 RPC。
func (n *Nodes) Dispatch(ctx context.Context, host string, req logic.Request) (logic.Response, error) {
	ctx = WithInternalCall(ctx, req.Internal)
	resp, err := n.logic.Dispatch(ctx, toDispatch(req), callopt.WithHostPort(host))
	if err != nil {
		return logic.Response{}, err
	}
	return logic.Response{PlayerID: resp.GetPlayerId(), CmdID: resp.GetCmdId(), Body: resp.GetBody()}, nil
}

// Pick 只给新会话用。调用方必须把返回的地址记在会话上。
func (n *Nodes) Pick(ctx context.Context) (Instance, error) {
	list, err := n.List(ctx, "Logic")
	if err != nil {
		return Instance{}, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return PickWeighted(list, func(total int) int { return n.rand.Intn(total) })
}

// Push 把包写回持有该会话的网关。
func (n *Nodes) Push(ctx context.Context, host, sessionID string, cmdID int32, body []byte) error {
	ctx = WithInternalCall(ctx, false)
	_, err := n.gate.Push(ctx, nodePush(sessionID, cmdID, body), callopt.WithHostPort(host))
	return err
}

// NotifyAll 让每个逻辑节点推自己内存里的该区服会话。
func (n *Nodes) NotifyAll(ctx context.Context, serverID, cmdID int32, body []byte) error {
	list, err := n.List(ctx, "Logic")
	if err != nil {
		return err
	}
	req := toNotify(serverID, cmdID, body)
	ctx = WithInternalCall(ctx, false)
	var first error
	for _, inst := range list {
		if _, err := n.logic.NotifyServer(ctx, req, callopt.WithHostPort(inst.Addr)); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Gates 返回当前仍注册着的网关地址。
// 注册中心明确没有 Gate 时返回空列表，逻辑节点才能把挂在已消失网关上的会话清掉。
// 查询本身失败时返回错误，调用方必须跳过本轮，避免把注册中心故障当成网关全灭。
func (n *Nodes) Gates(ctx context.Context) ([]string, error) {
	list, err := n.List(ctx, "Gate")
	if err != nil {
		if noInstance(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]string, 0, len(list))
	for _, inst := range list {
		if inst.Addr != "" {
			out = append(out, inst.Addr)
		}
	}
	return out, nil
}

func noInstance(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no instance remains")
}

// List 读取某个服务的全部实例和权重。
func (n *Nodes) List(ctx context.Context, service string) ([]Instance, error) {
	res, err := n.resolver.Resolve(ctx, service)
	if err != nil {
		return nil, err
	}
	out := make([]Instance, 0, len(res.Instances))
	for _, ins := range res.Instances {
		out = append(out, Instance{Addr: ins.Address().String(), Weight: ins.Weight()})
	}
	return out, nil
}

func ensureLeaseTTL() {
	if os.Getenv("KITEX_ETCD_REGISTRY_LEASE_TTL") == "" {
		_ = os.Setenv("KITEX_ETCD_REGISTRY_LEASE_TTL", "5")
	}
}
