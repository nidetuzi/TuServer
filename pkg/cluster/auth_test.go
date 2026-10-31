package cluster

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/server"

	node "tuserv/kitex_gen/node"
	crosscli "tuserv/kitex_gen/node/cross"
	gatecli "tuserv/kitex_gen/node/gate"
	logiccli "tuserv/kitex_gen/node/logic"
	"tuserv/pkg/config"
)

func TestSignRejectsTamperAndSkew(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	ts := "1700000000"
	sig := sign("secret", ts, "1", "Dispatch")
	if err := verify("secret", ts, "1", "Dispatch", sig, now); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, secret, ts, flag, method, sig string
		at                                  time.Time
	}{
		{name: "bad sig", secret: "secret", ts: ts, flag: "1", method: "Dispatch", sig: sig[:len(sig)-2] + "00", at: now},
		{name: "flag", secret: "secret", ts: ts, flag: "0", method: "Dispatch", sig: sig, at: now},
		{name: "method", secret: "secret", ts: ts, flag: "1", method: "Push", sig: sig, at: now},
		{name: "old", secret: "secret", ts: ts, flag: "1", method: "Dispatch", sig: sig, at: now.Add(authSkew + time.Second)},
		{name: "future", secret: "secret", ts: ts, flag: "1", method: "Dispatch", sig: sig, at: now.Add(-authSkew - time.Second)},
		{name: "empty", secret: "", ts: ts, flag: "1", method: "Dispatch", sig: sig, at: now},
	}
	for _, tc := range cases {
		if err := verify(tc.secret, tc.ts, tc.flag, tc.method, tc.sig, tc.at); !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("%s: got %v", tc.name, err)
		}
	}
}

func TestDialAndServerOptionsRequireSecret(t *testing.T) {
	t.Setenv("TUSERV_CLUSTER_SECRET", "")
	if _, err := Dial([]string{"127.0.0.1:1"}, " "); !errors.Is(err, ErrMissingSecret) {
		t.Fatalf("空密钥不应去连 etcd: %v", err)
	}
	if _, err := ServerOptions(config.File{ListenAddr: "127.0.0.1:9101"}, nil); err == nil {
		t.Fatal("没有密钥时不应返回监听选项")
	}
	opts, err := ServerOptions(config.File{
		ServiceName:   "Logic",
		ListenAddr:    "127.0.0.1:9101",
		AdvertiseAddr: "127.0.0.1:9101",
		ClusterSecret: "s",
	}, nil)
	if err != nil || len(opts) == 0 {
		t.Fatal(err)
	}
	if _, err := ServerOptions(config.File{ListenAddr: ":9101", ClusterSecret: "s"}, nil); err != nil {
		t.Fatal(err)
	}
}

type logicStub struct{}

func (logicStub) Dispatch(ctx context.Context, req *node.DispatchReq) (*node.DispatchResp, error) {
	flag := []byte("0")
	if IsInternalCall(ctx) {
		flag = []byte("1")
	}
	return &node.DispatchResp{CmdId: req.GetCmdId(), Body: flag}, nil
}

func (logicStub) NotifyServer(context.Context, *node.NotifyReq) (*node.NotifyResp, error) {
	return &node.NotifyResp{}, nil
}

type gateStub struct{}

func (gateStub) Push(context.Context, *node.PushReq) (*node.PushResp, error) {
	return &node.PushResp{}, nil
}

type crossStub struct{}

func (crossStub) Ping(context.Context, *node.PingReq) (*node.PingResp, error) {
	return &node.PingResp{}, nil
}

func TestKitexAuthDispatchPushPing(t *testing.T) {
	const secret = "test-secret"
	logicAddr := serveKitex(t, secret, func(opts ...server.Option) server.Server {
		return logiccli.NewServer(&logicStub{}, opts...)
	})
	gateAddr := serveKitex(t, secret, func(opts ...server.Option) server.Server {
		return gatecli.NewServer(&gateStub{}, opts...)
	})
	crossAddr := serveKitex(t, secret, func(opts ...server.Option) server.Server {
		return crosscli.NewServer(&crossStub{}, opts...)
	})

	logicClient, err := logiccli.NewClient("Logic", append(authClientOpts(secret), client.WithHostPorts(logicAddr))...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := logicClient.Dispatch(WithInternalCall(ctx, true), &node.DispatchReq{CmdId: 0})
	if err != nil || string(resp.GetBody()) != "1" {
		t.Fatalf("验签通过的内部标记应到达处理函数: body=%q err=%v", resp.GetBody(), err)
	}
	resp, err = logicClient.Dispatch(ctx, &node.DispatchReq{CmdId: 1001})
	if err != nil || string(resp.GetBody()) != "0" {
		t.Fatalf("普通转发不应变成内部调用: body=%q err=%v", resp.GetBody(), err)
	}
	if _, err := logicClient.NotifyServer(ctx, &node.NotifyReq{ServerId: 1, CmdId: 2001}); err != nil {
		t.Fatal(err)
	}

	bare, err := logiccli.NewClient("Logic", client.WithHostPorts(logicAddr))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bare.Dispatch(ctx, &node.DispatchReq{CmdId: 1001}); err == nil || !strings.Contains(err.Error(), ErrUnauthenticated.Error()) {
		t.Fatalf("无签名应被拒绝: %v", err)
	}
	wrong, err := logiccli.NewClient("Logic", append(authClientOpts("other"), client.WithHostPorts(logicAddr))...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrong.Dispatch(ctx, &node.DispatchReq{CmdId: 1001}); err == nil {
		t.Fatal("错误密钥应被拒绝")
	}

	gateClient, err := gatecli.NewClient("Gate", append(authClientOpts(secret), client.WithHostPorts(gateAddr))...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gateClient.Push(ctx, &node.PushReq{SessionId: "s", CmdId: 1}); err != nil {
		t.Fatal(err)
	}
	bareGate, err := gatecli.NewClient("Gate", client.WithHostPorts(gateAddr))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bareGate.Push(ctx, &node.PushReq{SessionId: "s"}); err == nil {
		t.Fatal("无签名的 Push 应被拒绝")
	}

	crossClient, err := crosscli.NewClient("CrossNode", append(authClientOpts(secret), client.WithHostPorts(crossAddr))...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := crossClient.Ping(ctx, &node.PingReq{}); err != nil {
		t.Fatal(err)
	}
	bareCross, err := crosscli.NewClient("CrossNode", client.WithHostPorts(crossAddr))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bareCross.Ping(ctx, &node.PingReq{}); err == nil {
		t.Fatal("无签名的 Ping 应被拒绝")
	}
}

func serveKitex(t *testing.T, secret string, newSvr func(opts ...server.Option) server.Server) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	opts := []server.Option{server.WithListener(ln), server.WithExitWaitTime(20 * time.Millisecond)}
	opts = append(opts, authServerOpts(secret)...)
	svr := newSvr(opts...)
	go func() { _ = svr.Run() }()
	t.Cleanup(func() {
		_ = svr.Stop()
		_ = ln.Close()
	})
	deadline := time.Now().Add(2 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", ln.Addr().String(), 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return ln.Addr().String()
		}
		last = err
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(last)
	return ""
}
