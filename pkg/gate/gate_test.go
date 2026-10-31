package gate

import (
	"context"
	"errors"
	"testing"

	"tuserv/pkg/cluster"
	"tuserv/pkg/logic"
	"tuserv/pkg/protocol"
)

type fakeFwd struct {
	reqs []logic.Request
}

func (f *fakeFwd) Dispatch(_ context.Context, _ string, req logic.Request) (logic.Response, error) {
	f.reqs = append(f.reqs, req)
	return logic.Response{CmdID: req.CmdID, Body: []byte("ok")}, nil
}

func (f *fakeFwd) Pick(context.Context) (cluster.Instance, error) {
	return cluster.Instance{Addr: "127.0.0.1:9101", Weight: 1}, nil
}

type fakePeer struct {
	id     string
	closed bool
	wrote  int
	last   protocol.Packet
}

func (p *fakePeer) ID() string { return p.id }
func (p *fakePeer) WritePacket(pkt protocol.Packet) error {
	p.wrote++
	p.last = pkt
	return nil
}
func (p *fakePeer) Close() error {
	p.closed = true
	return nil
}

func TestPushKickClosesSession(t *testing.T) {
	g := New("127.0.0.1:9001", nil, nil)
	p := &fakePeer{id: "7"}
	g.OnOpen(p)
	if err := g.Push("7", logic.CmdKick, 1, nil); err != nil {
		t.Fatal(err)
	}
	if !p.closed || p.wrote != 0 {
		t.Fatalf("踢线应关闭连接而不是写业务包: closed=%v wrote=%d", p.closed, p.wrote)
	}
	other := &fakePeer{id: "8"}
	g.OnOpen(other)
	if err := g.Push("8", 2001, 1, []byte{1}); err != nil {
		t.Fatal(err)
	}
	if other.closed || other.wrote != 1 {
		t.Fatalf("普通推送不应关连接: %+v", other)
	}
}

type scriptFwd struct {
	picks int
	resp  logic.Response
	err   error
}

func (f *scriptFwd) Dispatch(context.Context, string, logic.Request) (logic.Response, error) {
	return f.resp, f.err
}

func (f *scriptFwd) Pick(context.Context) (cluster.Instance, error) {
	f.picks++
	return cluster.Instance{Addr: "10.0.0.8:1", Weight: 1}, nil
}

func TestBusinessErrorKeepsLogicBinding(t *testing.T) {
	fwd := &scriptFwd{resp: logic.Response{CmdID: protocol.CmdError, Body: []byte{8, 3}, PlayerID: 0}}
	g := New("127.0.0.1:9001", fwd, nil)
	p := &fakePeer{id: "s"}
	g.OnOpen(p)
	g.OnPacket(p, protocol.Packet{CmdID: 1003, ServerID: 3, Body: []byte{1}})
	s := g.session("s")
	if s.logicAddr != "10.0.0.8:1" || fwd.picks != 1 {
		t.Fatalf("业务错误不应解开绑定: addr=%s picks=%d", s.logicAddr, fwd.picks)
	}
	if p.wrote != 1 || p.last.CmdID != protocol.CmdError || p.last.ServerID != 3 {
		t.Fatalf("业务错误应写回错误帧: wrote=%d pkt=%+v", p.wrote, p.last)
	}

	fwd.resp = logic.Response{CmdID: 1003, Body: []byte{2}, PlayerID: 42}
	g.OnPacket(p, protocol.Packet{CmdID: 1003, ServerID: 3})
	if fwd.picks != 1 || s.logicAddr != "10.0.0.8:1" || s.playerID != 42 {
		t.Fatalf("成功回包应沿用原节点并记下角色: picks=%d addr=%s player=%d", fwd.picks, s.logicAddr, s.playerID)
	}

	fwd.err = errors.New("connection refused")
	g.OnPacket(p, protocol.Packet{CmdID: 1003, ServerID: 3})
	if s.logicAddr != "" || p.wrote != 2 || s.playerID != 42 {
		t.Fatalf("传输失败应清地址且不写包、不抹掉角色: addr=%q wrote=%d player=%d", s.logicAddr, p.wrote, s.playerID)
	}

	fwd.err = nil
	fwd.resp = logic.Response{CmdID: 1003, Body: []byte{3}}
	g.OnPacket(p, protocol.Packet{CmdID: 1003, ServerID: 3})
	if fwd.picks != 2 || s.logicAddr != "10.0.0.8:1" || p.wrote != 3 {
		t.Fatalf("解绑后下一帧应重选节点: picks=%d addr=%s wrote=%d", fwd.picks, s.logicAddr, p.wrote)
	}
}

func TestOnPacketRejectsNonPositiveCmd(t *testing.T) {
	fwd := &fakeFwd{}
	g := New("127.0.0.1:9001", fwd, nil)
	p := &fakePeer{id: "1"}
	g.OnOpen(p)
	for _, cmd := range []int32{0, -1, logic.CmdRelinquish} {
		g.OnPacket(p, protocol.Packet{CmdID: cmd, ServerID: 9, Body: []byte("x")})
	}
	if len(fwd.reqs) != 0 {
		t.Fatalf("非正协议号不应转发: %+v", fwd.reqs)
	}
	if !p.closed {
		t.Fatal("非法帧应关掉连接")
	}
}

func TestOnPacketForwardsPositiveAndCloseIsInternal(t *testing.T) {
	fwd := &fakeFwd{}
	g := New("127.0.0.1:9001", fwd, nil)
	p := &fakePeer{id: "1"}
	g.OnOpen(p)
	g.OnPacket(p, protocol.Packet{CmdID: 1001, ServerID: 3, Body: []byte("a")})
	g.OnPacket(p, protocol.Packet{CmdID: 0, ServerID: 9, Body: []byte("bad")})
	g.OnPacket(p, protocol.Packet{CmdID: 1002, ServerID: 3, Body: []byte("b")})
	g.OnClose(p, nil)
	if len(fwd.reqs) != 2 {
		t.Fatalf("应只转发正数帧和断线通知: %+v", fwd.reqs)
	}
	if fwd.reqs[0].CmdID != 1001 || fwd.reqs[0].Internal || fwd.reqs[0].ServerID != 3 {
		t.Fatalf("客户端帧不应带内部标记: %+v", fwd.reqs[0])
	}
	got := fwd.reqs[1]
	if got.CmdID != logic.CmdDisconnect || !got.Internal || got.ServerID != 3 {
		t.Fatalf("断线通知应是内部调用且沿用原区服: %+v", got)
	}
	if p.wrote != 1 {
		t.Fatalf("非法帧之后不应再回包: wrote=%d", p.wrote)
	}
}
