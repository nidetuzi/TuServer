package router

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"tuserv/pkg/handler"
)

func TestIntAndStringKeys(t *testing.T) {
	ctx := handler.New(context.Background(), handler.Meta{ServerID: 1})
	ints := New[int32]()
	Register(ints, int32(7), func(_ *handler.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
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
	Register(strs, "login", func(_ *handler.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
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

func TestMissingHandlerAndBadMessage(t *testing.T) {
	ctx := handler.New(context.Background(), handler.Meta{})
	r := New[int32]()
	if _, err := r.Dispatch(99, ctx, nil); !errors.Is(err, ErrNoHandler) {
		t.Fatalf("未注册应返回哨兵: %v", err)
	}
	Register(r, int32(7), func(*handler.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
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
