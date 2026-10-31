package gate

import (
	"context"
	"errors"
	"testing"

	"tuserv/pkg/cluster"
	"tuserv/pkg/logic"
	"tuserv/pkg/protocol"
)

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
