package cluster

import "testing"

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
}
