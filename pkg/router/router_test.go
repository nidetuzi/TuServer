package router

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"tuserv/pkg/handler"
	routepb "tuserv/pkg/router/protobuf"
)

type jsonBody struct{}

func (jsonBody) Unmarshal(_ int32, body []byte, dst any) error {
	return json.Unmarshal(body, dst)
}

func (jsonBody) Marshal(_ int32, msg any) ([]byte, error) {
	return json.Marshal(msg)
}

type echoReq struct {
	Value string `json:"value"`
}

type echoResp struct {
	Value string `json:"value"`
}

func TestIntAndStringKeysWithProtobuf(t *testing.T) {
	ctx := handler.New(context.Background(), handler.Meta{ServerID: 1})
	codec := routepb.Body()
	ints := New[int32]()
	Register(ints, int32(7), codec, func(_ *handler.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return &wrapperspb.StringValue{Value: req.GetValue() + "-i"}, nil
	})
	body, err := ints.Dispatch(7, ctx, mustMsg(wrapperspb.String("a")))
	if err != nil {
		t.Fatal(err)
	}
	var got wrapperspb.StringValue
	if err := proto.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.GetValue() != "a-i" {
		t.Fatalf("int32 路由结果不符: %s", got.GetValue())
	}

	strs := New[string]()
	Register(strs, "login", codec, func(_ *handler.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return &wrapperspb.StringValue{Value: req.GetValue() + "-s"}, nil
	})
	body, err = strs.Dispatch("login", ctx, mustMsg(wrapperspb.String("b")))
	if err != nil {
		t.Fatal(err)
	}
	got.Reset()
	if err := proto.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.GetValue() != "b-s" {
		t.Fatalf("string 路由结果不符: %s", got.GetValue())
	}
}

func TestJSONBodyCodecReplaceable(t *testing.T) {
	ctx := handler.New(context.Background(), handler.Meta{})
	r := New[int32]()
	Register(r, int32(1), jsonBody{}, func(_ *handler.Context, req *echoReq) (*echoResp, error) {
		return &echoResp{Value: req.Value + "-ok"}, nil
	})
	in, _ := json.Marshal(echoReq{Value: "hi"})
	body, err := r.Dispatch(1, ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	var out echoResp
	if err := json.Unmarshal(body, &out); err != nil || out.Value != "hi-ok" {
		t.Fatalf("json codec: %v %v", out, err)
	}
}

func TestMissingHandlerAndBadMessage(t *testing.T) {
	ctx := handler.New(context.Background(), handler.Meta{})
	r := New[int32]()
	if _, err := r.Dispatch(99, ctx, nil); !errors.Is(err, ErrNoHandler) {
		t.Fatalf("未注册应返回哨兵: %v", err)
	}
	Register(r, int32(7), routepb.Body(), func(*handler.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return &wrapperspb.StringValue{}, nil
	})
	if _, err := r.Dispatch(7, ctx, []byte{0xff}); !errors.Is(err, ErrBadMessage) {
		t.Fatalf("坏 protobuf 应返回哨兵: %v", err)
	}
}

func mustMsg(m proto.Message) []byte {
	b, err := proto.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}
