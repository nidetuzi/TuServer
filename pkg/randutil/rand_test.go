package randutil

import (
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
)

func mustInt(t *testing.T, r *Rand, min, max int) int {
	t.Helper()
	v, err := r.Int(min, max)
	if err != nil {
		t.Fatalf("Int(%d, %d): %v", min, max, err)
	}
	return v
}

func TestNewSeedStable(t *testing.T) {
	const seed = uint64(42)
	a := NewSeed(seed)
	b := NewSeed(seed)
	for i := 0; i < 32; i++ {
		av, aerr := a.Int64(math.MinInt32, math.MaxInt32)
		bv, berr := b.Int64(math.MinInt32, math.MaxInt32)
		if aerr != nil || berr != nil || av != bv {
			t.Fatalf("同一种子第 %d 次不一致: %d %d %v %v", i, av, bv, aerr, berr)
		}
		if a.Float64() != b.Float64() {
			t.Fatal("同一种子的浮点序列不一致")
		}
	}
	pool := []Entry[string]{{Item: "a", Weight: 1}, {Item: "b", Weight: 3}, {Item: "c", Weight: 2}}
	ag, aerr := WeightedN(a, pool, 5)
	bg, berr := WeightedN(b, pool, 5)
	if aerr != nil || berr != nil || !reflect.DeepEqual(ag, bg) {
		t.Fatalf("同一种子的权重序列不一致: %v %v %v %v", ag, bg, aerr, berr)
	}
}

func TestSeedAdvances(t *testing.T) {
	a := NewSeed(42)
	b := NewSeed(42)
	first := mustInt(t, a, 0, 1_000_000)
	_ = mustInt(t, b, 0, 1_000_000)
	if first == mustInt(t, b, 0, 1_000_000) {
		t.Fatal("同一种子的相邻抽样相同，无法确认序列在推进")
	}
	left := NewSeed(1)
	right := NewSeed(2)
	for i := 0; i < 8; i++ {
		if mustInt(t, left, 0, 1<<30) != mustInt(t, right, 0, 1<<30) {
			return
		}
	}
	t.Fatal("不同种子不应得到相同序列")
}

func TestNewDiffers(t *testing.T) {
	a, err := New()
	if err != nil {
		t.Fatalf("创建随机源: %v", err)
	}
	b, err := New()
	if err != nil {
		t.Fatalf("创建随机源: %v", err)
	}
	same := true
	for i := 0; i < 8; i++ {
		av, aerr := a.Int64(0, math.MaxInt64)
		bv, berr := b.Int64(0, math.MaxInt64)
		if aerr != nil || berr != nil {
			t.Fatalf("Int64: %v %v", aerr, berr)
		}
		if av != bv {
			same = false
			break
		}
	}
	if same {
		t.Fatal("两次 New 不应得到相同序列")
	}
}

func TestNoConsume(t *testing.T) {
	next := func(t *testing.T, r *Rand) int64 {
		t.Helper()
		v, err := r.Int64(0, 1_000_000)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	cases := []struct {
		name  string
		touch func(*testing.T, *Rand)
	}{
		{"单点整数", func(t *testing.T, r *Rand) {
			v, err := r.Int(7, 7)
			if err != nil || v != 7 {
				t.Fatalf("单点区间: %d %v", v, err)
			}
		}},
		{"单点 int64", func(t *testing.T, r *Rand) {
			v, err := r.Int64(-3, -3)
			if err != nil || v != -3 {
				t.Fatalf("单点 int64: %d %v", v, err)
			}
		}},
		{"IntN 只有零", func(t *testing.T, r *Rand) {
			v, err := r.IntN(1)
			if err != nil || v != 0 {
				t.Fatalf("IntN(1): %d %v", v, err)
			}
		}},
		{"单点浮点", func(t *testing.T, r *Rand) {
			v, err := r.Float64Range(1.25, 1.25)
			if err != nil || v != 1.25 {
				t.Fatalf("单点浮点: %v %v", v, err)
			}
		}},
		{"必不中", func(t *testing.T, r *Rand) {
			hit, err := r.Hit(0, 10000)
			if err != nil || hit {
				t.Fatalf("权重为 0 应不中: %v %v", hit, err)
			}
		}},
		{"必中", func(t *testing.T, r *Rand) {
			hit, err := r.Hit(10000, 10000)
			if err != nil || !hit {
				t.Fatalf("满权重应必中: %v %v", hit, err)
			}
		}},
		{"抽零个", func(t *testing.T, r *Rand) {
			got, err := WeightedN(r, []Entry[int]{{Item: 1, Weight: 1}}, 0)
			if err != nil || got == nil || len(got) != 0 {
				t.Fatalf("抽 0 个: %#v %v", got, err)
			}
		}},
		{"唯一项不取样", func(t *testing.T, r *Rand) {
			got, err := Weighted(r, []Entry[int]{{Item: 9, Weight: 0}, {Item: 4, Weight: 5}})
			if err != nil || got != 4 {
				t.Fatalf("唯一正权重: %d %v", got, err)
			}
		}},
		{"非法区间", func(t *testing.T, r *Rand) {
			if _, err := r.Int(3, 1); err != ErrRange {
				t.Fatalf("下界大于上界: %v", err)
			}
		}},
		{"权重溢出", func(t *testing.T, r *Rand) {
			_, err := Weighted(r, []Entry[int]{{Item: 1, Weight: math.MaxInt64}, {Item: 2, Weight: 1}})
			if err != ErrWeight {
				t.Fatalf("权重溢出: %v", err)
			}
		}},
		{"无放回超出数量", func(t *testing.T, r *Rand) {
			_, err := WeightedUnique(r, []Entry[int]{{Item: 1, Weight: 1}, {Item: 2, Weight: 1}}, 3)
			if err != ErrCount {
				t.Fatalf("数量超出可选项: %v", err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			touched := NewSeed(42)
			plain := NewSeed(42)
			tc.touch(t, touched)
			if next(t, touched) != next(t, plain) {
				t.Fatal("这次调用不应消耗随机序列")
			}
		})
	}
}

func TestIntClosedInterval(t *testing.T) {
	r := NewSeed(7)
	seen := map[int]int{}
	for i := 0; i < 300; i++ {
		v := mustInt(t, r, -2, 2)
		if v < -2 || v > 2 {
			t.Fatalf("越界: %d", v)
		}
		seen[v]++
	}
	if len(seen) != 5 {
		t.Fatalf("闭区间端点未覆盖: %v", seen)
	}

	r = NewSeed(8)
	nSeen := map[int]bool{}
	for i := 0; i < 90; i++ {
		v, err := r.IntN(3)
		if err != nil || v < 0 || v >= 3 {
			t.Fatalf("IntN 越界: %d %v", v, err)
		}
		nSeen[v] = true
	}
	if len(nSeen) != 3 {
		t.Fatalf("IntN 未覆盖 [0, 3): %v", nSeen)
	}
	if _, err := r.IntN(0); err != ErrRange {
		t.Fatalf("IntN(0): %v", err)
	}
	if _, err := r.IntN(-5); err != ErrRange {
		t.Fatalf("IntN 负数: %v", err)
	}
}

func TestInt64Edges(t *testing.T) {
	r := NewSeed(11)
	high := map[int64]bool{}
	low := map[int64]bool{}
	for i := 0; i < 80; i++ {
		v, err := r.Int64(math.MaxInt64-1, math.MaxInt64)
		if err != nil || (v != math.MaxInt64 && v != math.MaxInt64-1) {
			t.Fatalf("高端闭区间越界: %d %v", v, err)
		}
		high[v] = true
		v, err = r.Int64(math.MinInt64, math.MinInt64+1)
		if err != nil || (v != math.MinInt64 && v != math.MinInt64+1) {
			t.Fatalf("低端闭区间越界: %d %v", v, err)
		}
		low[v] = true
		v, err = r.Int64(math.MinInt64, math.MaxInt64-1)
		if err != nil || v == math.MaxInt64 {
			t.Fatalf("几乎全范围越界: %d %v", v, err)
		}
		v, err = r.Int64(0, math.MaxInt64)
		if err != nil || v < 0 {
			t.Fatalf("非负区间越界: %d %v", v, err)
		}
	}
	if len(high) != 2 || len(low) != 2 {
		t.Fatalf("端点未覆盖: high=%v low=%v", high, low)
	}
	if _, err := r.Int64(math.MinInt64, math.MaxInt64); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Int64(2, 1); err != ErrRange {
		t.Fatalf("Int64 下界大于上界: %v", err)
	}
}

func TestFloat64(t *testing.T) {
	r := NewSeed(3)
	var low, high bool
	for i := 0; i < 200; i++ {
		v := r.Float64()
		if v < 0 || v >= 1 || math.IsNaN(v) {
			t.Fatalf("Float64 越出 [0, 1): %v", v)
		}
		if v < 0.5 {
			low = true
		} else {
			high = true
		}
	}
	if !low || !high {
		t.Fatal("Float64 分布贴在一侧")
	}

	r = NewSeed(4)
	for i := 0; i < 200; i++ {
		v, err := r.Float64Range(-1.5, 2.5)
		if err != nil || v < -1.5 || v >= 2.5 {
			t.Fatalf("Float64Range 越界: %v %v", v, err)
		}
	}
	if _, err := NewSeed(4).Float64Range(2, 1); err != ErrRange {
		t.Fatalf("浮点下界大于上界: %v", err)
	}
	if _, err := NewSeed(4).Float64Range(math.NaN(), 1); err != ErrRange {
		t.Fatalf("NaN: %v", err)
	}
	if _, err := NewSeed(4).Float64Range(0, math.Inf(1)); err != ErrRange {
		t.Fatalf("无穷上界: %v", err)
	}
	if _, err := NewSeed(4).Float64Range(-math.MaxFloat64, math.MaxFloat64); err != ErrRange {
		t.Fatalf("间距溢出: %v", err)
	}
}

func TestHit(t *testing.T) {
	r := NewSeed(6)
	if _, err := r.Hit(1, 0); err != ErrRange {
		t.Fatalf("分母为 0: %v", err)
	}
	if _, err := r.Hit(1, -10); err != ErrRange {
		t.Fatalf("分母为负: %v", err)
	}
	hits := 0
	const rounds = 10000
	for i := 0; i < rounds; i++ {
		ok, err := r.Hit(3, 10)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			hits++
		}
	}
	if hits < 2500 || hits > 3500 {
		t.Fatalf("3/10 命中次数异常: %d", hits)
	}
}

func TestShuffle(t *testing.T) {
	src := []int{1, 2, 3, 4, 5}
	a := append([]int(nil), src...)
	b := append([]int(nil), src...)
	Shuffle(NewSeed(3), a)
	Shuffle(NewSeed(3), b)
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("同一种子洗牌结果不一致: %v %v", a, b)
	}
	seen := map[int]int{}
	for _, v := range a {
		seen[v]++
	}
	if len(seen) != len(src) {
		t.Fatalf("洗牌后不是原集合的排列: %v", a)
	}
	changed := false
	for seed := uint64(1); seed <= 30; seed++ {
		cp := append([]int(nil), src...)
		Shuffle(NewSeed(seed), cp)
		if !reflect.DeepEqual(cp, src) {
			changed = true
			break
		}
	}
	if !changed {
		t.Fatal("洗牌没有改变任何序列")
	}

	plain := NewSeed(9)
	touched := NewSeed(9)
	one := []int{1}
	Shuffle(touched, one)
	Shuffle(touched, []int(nil))
	if mustInt(t, plain, 0, 1000) != mustInt(t, touched, 0, 1000) {
		t.Fatal("长度不足 2 的洗牌不应消耗随机序列")
	}
}

func TestUninitializedPanics(t *testing.T) {
	wantPanic(t, func() {
		var r Rand
		_, _ = r.Int(0, 1)
	})
	wantPanic(t, func() {
		var r *Rand
		_ = r.Float64()
	})
	wantPanic(t, func() {
		var r *Rand
		_, _ = Weighted(r, []Entry[int]{{Item: 1, Weight: 1}})
	})
}

func wantPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("应当 panic")
		}
	}()
	fn()
}

func TestConcurrent(t *testing.T) {
	r := NewSeed(99)
	pool := []Entry[int]{
		{Item: 1, Weight: 1},
		{Item: 2, Weight: 2},
		{Item: 3, Weight: 3},
		{Item: 4, Weight: 0},
	}
	var wg sync.WaitGroup
	errCh := make(chan error, 16)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 100; n++ {
				v, err := r.Int(-3, 9)
				if err != nil || v < -3 || v > 9 {
					errCh <- fmt.Errorf("Int: %d %v", v, err)
					return
				}
				f := r.Float64()
				if f < 0 || f >= 1 {
					errCh <- fmt.Errorf("Float64: %v", f)
					return
				}
				got, err := WeightedUnique(r, pool, 2)
				if err != nil || len(got) != 2 || got[0] == got[1] || got[0] == 4 || got[1] == 4 {
					errCh <- fmt.Errorf("WeightedUnique: %v %v", got, err)
					return
				}
				items := []int{1, 2, 3}
				Shuffle(r, items)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}
