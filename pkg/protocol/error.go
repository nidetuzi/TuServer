package protocol

import (
	"errors"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"
)

// CmdError 是统一错误包的协议号。
// 玩法协议从 1000 起编号，10 留给框架，失败回包才不会被客户端当成另一种成功 protobuf。
const CmdError int32 = 10

// 错误包正文是一条 protobuf：
//
//	int32 code = 1;
//	int32 req_cmd = 2;
//	string detail = 3;
//
// 帧头仍是长度、cmdId、serverId。错误码放在正文里，成功帧的布局不用改。
const maxDetail = 96

// ErrBadErrorBody 表示错误包正文不完整或类型不对，调用方不应把它当成一次成功。
var ErrBadErrorBody = errors.New("protocol: bad error body")

// ErrorBody 是一帧统一错误包的正文。
type ErrorBody struct {
	// Code 是客户端分支用的数字错误码，0 不表示失败。
	Code int32
	// ReqCmd 是失败的那条请求协议号，客户端按它对应到发出去的包。
	ReqCmd int32
	// Detail 是稳定的短原因。诊断原文留在服务端日志，不放进帧里。
	Detail string
}

// MarshalError 把错误包编码成 protobuf 正文。
// detail 超过上限时按 UTF-8 边界截断，避免一帧被诊断字符串撑大。
func MarshalError(body ErrorBody) []byte {
	detail := body.Detail
	if len(detail) > maxDetail {
		detail = trimDetail(detail)
	}
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.VarintType)
	b = protowire.AppendVarint(b, uint64(int64(body.Code)))
	b = protowire.AppendTag(b, 2, protowire.VarintType)
	b = protowire.AppendVarint(b, uint64(int64(body.ReqCmd)))
	if detail != "" {
		b = protowire.AppendTag(b, 3, protowire.BytesType)
		b = protowire.AppendString(b, detail)
	}
	return b
}

// UnmarshalError 解析错误包正文。未知字段会被跳过，以后加字段时旧代码仍能读出错误码。
func UnmarshalError(buf []byte) (ErrorBody, error) {
	var out ErrorBody
	b := buf
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return ErrorBody{}, ErrBadErrorBody
		}
		b = b[n:]
		switch {
		case num == 1 && typ == protowire.VarintType:
			v, m := protowire.ConsumeVarint(b)
			if m < 0 {
				return ErrorBody{}, ErrBadErrorBody
			}
			out.Code = int32(v)
			b = b[m:]
		case num == 2 && typ == protowire.VarintType:
			v, m := protowire.ConsumeVarint(b)
			if m < 0 {
				return ErrorBody{}, ErrBadErrorBody
			}
			out.ReqCmd = int32(v)
			b = b[m:]
		case num == 3 && typ == protowire.BytesType:
			v, m := protowire.ConsumeString(b)
			if m < 0 {
				return ErrorBody{}, ErrBadErrorBody
			}
			out.Detail = v
			b = b[m:]
		default:
			m := protowire.ConsumeFieldValue(num, typ, b)
			if m < 0 {
				return ErrorBody{}, ErrBadErrorBody
			}
			b = b[m:]
		}
	}
	return out, nil
}

// trimDetail 去掉被截断的半个 UTF-8 字符。客户端按字符串读 detail，半个字符会解失败。
func trimDetail(s string) string {
	s = s[:maxDetail]
	for len(s) > 0 && !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}
