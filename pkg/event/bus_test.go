package event

import (
	"context"
	"testing"
)

type levelUp struct{ Level int32 }

func TestPublishRunsListener(t *testing.T) {
	bus := New()
	var got int32
	On(bus, func(_ context.Context, ev levelUp) {
		got = ev.Level
	})
	bus.Publish(context.Background(), levelUp{Level: 3})
	if got != 3 {
		t.Fatalf("监听者没有收到事件: %d", got)
	}
}
