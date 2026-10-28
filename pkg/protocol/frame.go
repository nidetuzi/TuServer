package protocol

import (
	"encoding/binary"
	"errors"
)

// 长度字段统计的是 cmdId、serverId 和 body，不含长度自身。
// 这样半包判断只需要再读 4 字节头，Gate 也不必理解玩法 protobuf。
const (
	lenSize    = 4
	cmdSize    = 4
	serverSize = 4
	metaSize   = cmdSize + serverSize
)

// ErrShortBuffer 表示缓冲里还不够一帧，调用方应继续等待后续字节。
var ErrShortBuffer = errors.New("protocol: short buffer")

// ErrBadFrame 表示长度字段不可能构成合法帧，连接应当关闭。
var ErrBadFrame = errors.New("protocol: bad frame")

// Packet 是一帧已经拆开的客户端包。
type Packet struct {
	CmdID    int32
	ServerID int32
	Body     []byte
}

// Codec 把帧和字节互相转换。替换它就能换协议，而不改传输层。
type Codec interface {
	Encode(pkt Packet) ([]byte, error)
	Decode(buf []byte) (pkt Packet, n int, err error)
}

// FrameCodec 是默认帧：uint32 长度、int32 cmdId、int32 serverId、protobuf body。
type FrameCodec struct{}

// Encode 生成可直接写到 TCP 或作为一条 WebSocket 二进制消息发送的字节。
func (FrameCodec) Encode(pkt Packet) ([]byte, error) {
	payload := metaSize + len(pkt.Body)
	buf := make([]byte, lenSize+payload)
	binary.BigEndian.PutUint32(buf[0:4], uint32(payload))
	binary.BigEndian.PutUint32(buf[4:8], uint32(pkt.CmdID))
	binary.BigEndian.PutUint32(buf[8:12], uint32(pkt.ServerID))
	copy(buf[12:], pkt.Body)
	return buf, nil
}

// Decode 从缓冲头部解出一帧。n 是消耗的字节数，剩余字节留给下一帧。
func (FrameCodec) Decode(buf []byte) (Packet, int, error) {
	if len(buf) < lenSize {
		return Packet{}, 0, ErrShortBuffer
	}
	payload := int(binary.BigEndian.Uint32(buf[:lenSize]))
	if payload < metaSize {
		return Packet{}, 0, ErrBadFrame
	}
	total := lenSize + payload
	if len(buf) < total {
		return Packet{}, 0, ErrShortBuffer
	}
	body := make([]byte, payload-metaSize)
	copy(body, buf[lenSize+metaSize:total])
	pkt := Packet{
		CmdID:    int32(binary.BigEndian.Uint32(buf[lenSize : lenSize+cmdSize])),
		ServerID: int32(binary.BigEndian.Uint32(buf[lenSize+cmdSize : lenSize+metaSize])),
		Body:     body,
	}
	return pkt, total, nil
}

// Assembler 把 TCP 流切成帧。WebSocket 已有消息边界，不需要它。
type Assembler struct {
	codec Codec
	buf   []byte
}

// NewAssembler 绑定连接使用的编解码器。
func NewAssembler(codec Codec) *Assembler {
	if codec == nil {
		codec = FrameCodec{}
	}
	return &Assembler{codec: codec}
}

// Push 追加新读到的字节并返回已经凑齐的帧。
// nbio 的 OnData 会复用 data，这里必须先拷贝，否则下一轮读会改掉半包。
func (a *Assembler) Push(data []byte) ([]Packet, error) {
	if len(data) == 0 {
		return nil, nil
	}
	a.buf = append(a.buf, data...)
	var pkts []Packet
	for {
		pkt, n, err := a.codec.Decode(a.buf)
		if err == ErrShortBuffer {
			return pkts, nil
		}
		if err != nil {
			return pkts, err
		}
		pkts = append(pkts, pkt)
		a.buf = append([]byte(nil), a.buf[n:]...)
	}
}
