package cluster

import (
	"fmt"
	"testing"
)

func TestPickWeightedRatio(t *testing.T) {
	list := []Instance{{Addr: "a", Weight: 1}, {Addr: "b", Weight: 3}}
	var seq int
	roll := func(total int) int {
		n := seq % total
		seq++
		return n
	}
	count := map[string]int{}
	for i := 0; i < 400; i++ {
		got, err := PickWeighted(list, roll)
		if err != nil {
			t.Fatal(err)
		}
		count[got.Addr]++
	}
	if count["a"] != 100 || count["b"] != 300 {
		t.Fatalf("权重比例不符: %v", count)
	}
}

func TestOwnerStable(t *testing.T) {
	list := []Instance{{Addr: "b", Weight: 1}, {Addr: "a", Weight: 1}}
	first, err := Owner(list, "guild:9")
	if err != nil {
		t.Fatal(err)
	}
	second, err := Owner(list, "guild:9")
	if err != nil {
		t.Fatal(err)
	}
	if first.Addr != second.Addr {
		t.Fatal("同一实体键应落到同一节点")
	}
	again, err := Owner([]Instance{{Addr: "a", Weight: 1}, {Addr: "b", Weight: 1}}, "guild:9")
	if err != nil || again.Addr != first.Addr {
		t.Fatalf("节点列表顺序不应改变主人: %v %v", again, err)
	}
}

func TestOwnerSkipsDrainingNodes(t *testing.T) {
	list := []Instance{{Addr: "down", Weight: 0}, {Addr: "up", Weight: 1}}
	got, err := Owner(list, "guild:1")
	if err != nil || got.Addr != "up" {
		t.Fatalf("权重为 0 的节点不应成为主人: %+v %v", got, err)
	}
}

func TestOwnerMoveIsPartial(t *testing.T) {
	base := []Instance{{Addr: "a", Weight: 1}, {Addr: "b", Weight: 1}, {Addr: "c", Weight: 1}}
	withD := append(append([]Instance{}, base...), Instance{Addr: "d", Weight: 1})
	moved := 0
	const n = 2000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("guild-%d", i)
		before, err := Owner(base, key)
		if err != nil {
			t.Fatal(err)
		}
		after, err := Owner(withD, key)
		if err != nil {
			t.Fatal(err)
		}
		if before.Addr != after.Addr {
			moved++
			if after.Addr != "d" {
				t.Fatalf("新增节点时只有它应该接收迁移: %s -> %s", before.Addr, after.Addr)
			}
		}
	}
	if moved == 0 || moved > n/2 {
		t.Fatalf("迁移数量不合理: %d / %d", moved, n)
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("guild-%d", i)
		full, err := Owner(base, key)
		if err != nil {
			t.Fatal(err)
		}
		if full.Addr == "c" {
			continue
		}
		left, err := Owner(base[:2], key)
		if err != nil || left.Addr != full.Addr {
			t.Fatalf("摘掉 c 不应搬走 %s 上的 %s: %+v %v", full.Addr, key, left, err)
		}
	}
}
