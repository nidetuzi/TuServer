package gate

import (
	"testing"

	"tuserv/pkg/logic"
	"tuserv/pkg/protocol"
)

type fakePeer struct {
	id     string
	closed bool
	wrote  int
}

func (p *fakePeer) ID() string { return p.id }
func (p *fakePeer) WritePacket(protocol.Packet) error {
	p.wrote++
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
