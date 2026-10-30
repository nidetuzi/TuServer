package randutil

import (
	"math"
	"reflect"
	"testing"
)

func TestIndexByRoll(t *testing.T) {
	weights := []int64{0, 1, -3, 3}
	if indexByRoll(weights, 0) != 1 {
		t.Fatal("roll 0 应落在权重 1 的项")
	}
	for _, roll := range []int64{1, 2, 3} {
		if got := indexByRoll(weights, roll); got != 3 {
			t.Fatalf("roll %d 落到了 %d，权重 3 的项应占后三个位置", roll, got)
		}
	}
	if indexByRoll(weights, 99) != 3 {
		t.Fatal("越界 roll 应退回最后一个正权重项")
	}
	if indexByRoll([]int64{0, -1}, 0) != -1 {
		t.Fatal("没有正权重时应返回 -1")
	}
}

func TestWeightedSkipsNonPositive(t *testing.T) {
	pool := []Entry[string]{
		{Item: "关", Weight: 0},
		{Item: "开", Weight: 4},
		{Item: "下架", Weight: -2},
		{Item: "最小", Weight: math.MinInt64},
	}
	r := NewSeed(1)
	for i := 0; i < 30; i++ {
		got, err := Weighted(r, pool)
		if err != nil || got != "开" {
			t.Fatalf("非正权重仍被抽中: %q %v", got, err)
		}
	}
	many, err := WeightedN(r, pool, 3)
	if err != nil || !reflect.DeepEqual(many, []string{"开", "开", "开"}) {
		t.Fatalf("有放回应重复唯一可选项: %v %v", many, err)
	}
}

func TestWeightedEmptyAndCount(t *testing.T) {
	r := NewSeed(2)
	if _, err := Weighted[int](r, nil); err != ErrEmpty {
		t.Fatalf("空池: %v", err)
	}
	if _, err := Weighted(r, []Entry[int]{{Item: 1, Weight: 0}, {Item: 2, Weight: -1}}); err != ErrEmpty {
		t.Fatalf("全部非正权重: %v", err)
	}
	got, err := WeightedN(r, []Entry[int](nil), 0)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("空池抽 0 个: %#v %v", got, err)
	}
	got, err = WeightedN(r, []Entry[int]{{Item: 1, Weight: 1}}, -1)
	if err != ErrCount || got != nil {
		t.Fatalf("负数数量: %#v %v", got, err)
	}
	if _, err = WeightedN(r, []Entry[int]{{Item: 1, Weight: 0}}, 1); err != ErrEmpty {
		t.Fatalf("没有可选项却要求抽到: %v", err)
	}
	_, err = Weighted(r, []Entry[int]{{Item: 1, Weight: math.MaxInt64}})
	if err != nil {
		t.Fatalf("单个最大权重应可以抽取: %v", err)
	}
}

func TestWeightedRatio(t *testing.T) {
	pool := []Entry[string]{{Item: "a", Weight: 1}, {Item: "b", Weight: 3}}
	r := NewSeed(5)
	count := map[string]int{}
	const rounds = 20000
	for i := 0; i < rounds; i++ {
		got, err := Weighted(r, pool)
		if err != nil {
			t.Fatal(err)
		}
		count[got]++
	}
	if count["a"]+count["b"] != rounds {
		t.Fatalf("计数丢失: %v", count)
	}
	if count["a"] < 4200 || count["a"] > 5800 || count["b"] < 14200 || count["b"] > 15800 {
		t.Fatalf("1:3 的权重比例异常: %v", count)
	}
}

func TestWeightedUnique(t *testing.T) {
	pool := []Entry[string]{
		{Item: "a", Weight: 1},
		{Item: "b", Weight: 5},
		{Item: "隐藏", Weight: 0},
		{Item: "d", Weight: 2},
		{Item: "下架", Weight: -4},
	}
	snapshot := append([]Entry[string](nil), pool...)
	r := NewSeed(12)
	got, err := WeightedUnique(r, pool, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pool, snapshot) {
		t.Fatal("抽取不应修改调用方的池")
	}
	want := map[string]int{"a": 1, "b": 1, "d": 1}
	gotCount := map[string]int{}
	for _, item := range got {
		gotCount[item]++
	}
	if !reflect.DeepEqual(gotCount, want) {
		t.Fatalf("无放回结果不是全部可选项: %v", got)
	}

	again, err := WeightedUnique(NewSeed(12), pool, 3)
	if err != nil || !reflect.DeepEqual(got, again) {
		t.Fatalf("同一种子的无放回序列不一致: %v %v %v", got, again, err)
	}

	if _, err = WeightedUnique(r, pool, 4); err != ErrCount {
		t.Fatalf("可选项只有 3 个: %v", err)
	}
	one, err := WeightedUnique(NewSeed(1), []Entry[int]{{Item: 7, Weight: 1}, {Item: 8, Weight: 0}}, 1)
	if err != nil || !reflect.DeepEqual(one, []int{7}) {
		t.Fatalf("跳过零权重后应只剩 7: %v %v", one, err)
	}
}

func TestWeightedUniqueByIndex(t *testing.T) {
	// 值相同的两项仍是两个坑位，无放回按坑位去掉，不按值去重。
	pool := []Entry[int]{{Item: 1, Weight: 1}, {Item: 1, Weight: 1}}
	got, err := WeightedUnique(NewSeed(2), pool, 2)
	if err != nil || !reflect.DeepEqual(got, []int{1, 1}) {
		t.Fatalf("相同值的两个坑位都应返回: %v %v", got, err)
	}
}

func TestWeightedNAllowsDuplicates(t *testing.T) {
	pool := []Entry[string]{{Item: "a", Weight: 1}, {Item: "b", Weight: 1}}
	r := NewSeed(15)
	got, err := WeightedN(r, pool, 40)
	if err != nil || len(got) != 40 {
		t.Fatalf("有放回数量: %v %v", got, err)
	}
	seen := map[string]int{}
	dup := false
	var prev string
	for i, item := range got {
		seen[item]++
		if i > 0 && item == prev {
			dup = true
		}
		prev = item
	}
	if len(seen) != 2 || !dup {
		t.Fatalf("有放回应能重复且两种都出现: %v", seen)
	}
}

func TestWeightedMatchesBatch(t *testing.T) {
	pool := []Entry[int]{{Item: 3, Weight: 2}, {Item: 8, Weight: 5}, {Item: 1, Weight: 1}}
	one, err := Weighted(NewSeed(21), pool)
	batch, berr := WeightedN(NewSeed(21), pool, 1)
	if err != nil || berr != nil || len(batch) != 1 || batch[0] != one {
		t.Fatalf("单抽和批量抽 1 个不一致: %d %v %v", one, batch, err)
	}
}
