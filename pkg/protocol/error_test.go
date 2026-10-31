package protocol

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestErrorBodyRoundTrip(t *testing.T) {
	in := ErrorBody{Code: 3, ReqCmd: 1003, Detail: "service: not enough items"}
	out, err := UnmarshalError(MarshalError(in))
	if err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("错误包往返不符: %+v", out)
	}
	// code=1, req_cmd=1001, detail="x"。手写字节用来钉住 protobuf 字段号，避免以后改成另一种编码。
	raw := MarshalError(ErrorBody{Code: 1, ReqCmd: 1001, Detail: "x"})
	want := []byte{0x08, 0x01, 0x10, 0xE9, 0x07, 0x1A, 0x01, 'x'}
	if string(raw) != string(want) {
		t.Fatalf("错误包编码不符: %x", raw)
	}
}

func TestErrorBodyTruncatesDetail(t *testing.T) {
	detail := strings.Repeat("错", 33)
	out, err := UnmarshalError(MarshalError(ErrorBody{Code: 100, ReqCmd: 7, Detail: detail}))
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Repeat("错", 32)
	if out.Detail != want || !utf8.ValidString(out.Detail) || len(out.Detail) > maxDetail {
		t.Fatalf("过长 detail 应刚好截在字符边界: %q", out.Detail)
	}
}

func TestErrorBodyRejectsTruncated(t *testing.T) {
	if _, err := UnmarshalError([]byte{0x08}); err != ErrBadErrorBody {
		t.Fatalf("截断的错误包应失败: %v", err)
	}
}
