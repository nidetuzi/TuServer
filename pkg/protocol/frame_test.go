package protocol

import "testing"

func TestFrameRoundTripAndShortBuffer(t *testing.T) {
	codec := FrameCodec{}
	raw, err := codec.Encode(Packet{CmdID: 1001, ServerID: 7, Body: []byte{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	pkt, n, err := codec.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(raw) || pkt.CmdID != 1001 || pkt.ServerID != 7 || string(pkt.Body) != string([]byte{1, 2, 3}) {
		t.Fatalf("解码结果不符: %+v n=%d", pkt, n)
	}
	if _, _, err := codec.Decode(raw[:6]); err != ErrShortBuffer {
		t.Fatalf("半包应返回 ErrShortBuffer, got %v", err)
	}

	asm := NewAssembler(codec)
	first, err := asm.Push(raw[:5])
	if err != nil || len(first) != 0 {
		t.Fatalf("半包不应产出帧: %v %v", first, err)
	}
	second, err := asm.Push(raw[5:])
	if err != nil || len(second) != 1 || second[0].CmdID != 1001 {
		t.Fatalf("补齐后应得到一帧: %+v %v", second, err)
	}
}
