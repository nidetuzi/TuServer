package errcode

import (
	"errors"
	"fmt"
	"testing"
)

func TestSentinelIdentity(t *testing.T) {
	a := New(NotEnough, "service: not enough items")
	b := New(NotEnough, "service: not enough items")
	if errors.Is(a, b) {
		t.Fatal("同码不同指针不能当成同一个哨兵")
	}
	if !errors.Is(a, a) {
		t.Fatal("哨兵应能按指针匹配")
	}
	wrapped := fmt.Errorf("use: %w", a)
	if !errors.Is(wrapped, a) {
		t.Fatal("包装后应仍能匹配哨兵")
	}
	var n Numbered
	if !errors.As(wrapped, &n) || n.Errno() != NotEnough {
		t.Fatalf("包装后应读到错误码: %+v", n)
	}
}
