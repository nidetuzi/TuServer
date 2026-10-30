package randutil

import "math"

// Entry 是权重池中的一项。
// Weight 小于等于 0 的项不参与抽取，用来表示未开放或已经下掉的奖励，而不是让整池失败。
// 正权重只比较相对大小，不要求和是 100 或 10000。
type Entry[T any] struct {
	Item   T
	Weight int64
}

// Weighted 按权重抽一项。
// 池为空，或所有权重都不是正数时返回 ErrEmpty。不修改 entries。
// 出错时返回 T 的零值，调用方必须先看错误，不能把零值当成抽中的奖励。
// 泛型抽样不能做成 Rand 的方法，所以和 Int 分开，以包级函数提供。
func Weighted[T any](r *Rand, entries []Entry[T]) (T, error) {
	var zero T
	got, err := weightedDraw(r, entries, 1, false)
	if err != nil {
		return zero, err
	}
	return got[0], nil
}

// WeightedN 有放回地抽 n 项，同一项可以重复出现。十连抽允许出两件相同奖励时用这个。
// n 为 0 返回空切片且不取样；n 小于 0 返回 ErrCount。池里没有正权重且 n 大于 0 时返回 ErrEmpty。
func WeightedN[T any](r *Rand, entries []Entry[T], n int) ([]T, error) {
	return weightedDraw(r, entries, n, false)
}

// WeightedUnique 无放回地抽 n 项。抽中后该项退出本轮，剩余权重重新归一。
// 请求数量大于正权重项个数时返回 ErrCount，并且不返回半截结果，避免奖励悄悄少发。
// 不重复指的是池中的下标，不是 Item 的值；两条相同奖励如果都配了正权重，会各算一项。
func WeightedUnique[T any](r *Rand, entries []Entry[T], n int) ([]T, error) {
	return weightedDraw(r, entries, n, true)
}

func weightedDraw[T any](r *Rand, entries []Entry[T], n int, unique bool) ([]T, error) {
	r.mustInit()
	if n < 0 {
		return nil, ErrCount
	}
	if n == 0 {
		return []T{}, nil
	}
	weights, total, count, err := collectWeights(entries)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, ErrEmpty
	}
	if unique && n > count {
		return nil, ErrCount
	}

	out := make([]T, 0, n)
	if count > 1 {
		// 整批抽完再解锁。中途解锁的话，别的协程会插进同一条序列，种子就对不齐这一发十连。
		r.mu.Lock()
		defer r.mu.Unlock()
	}
	for len(out) < n {
		var idx int
		if count == 1 {
			// 只剩一项时结果已经确定，不再取样，后面的序列才不会被定值奖励挤开。
			idx = indexByRoll(weights, 0)
		} else {
			idx = indexByRoll(weights, int64(r.src.Uint64N(uint64(total))))
		}
		if idx < 0 {
			return nil, ErrEmpty
		}
		out = append(out, entries[idx].Item)
		if !unique {
			continue
		}
		total -= weights[idx]
		weights[idx] = 0
		count--
	}
	return out, nil
}

func collectWeights[T any](entries []Entry[T]) ([]int64, int64, int, error) {
	weights := make([]int64, len(entries))
	var total int64
	var count int
	for i, e := range entries {
		if e.Weight <= 0 {
			continue
		}
		if total > math.MaxInt64-e.Weight {
			return nil, 0, 0, ErrWeight
		}
		weights[i] = e.Weight
		total += e.Weight
		count++
	}
	return weights, total, count, nil
}

// indexByRoll 按前缀和定位。weights 里小于等于 0 的位置是被跳过或已经抽走的项。
// roll 应当落在 [0, 正权重和) 内。roll 大于等于权重和时退回最后一个正权重项，避免上界和权重和短暂不一致时把抽卡变成失败。
func indexByRoll(weights []int64, roll int64) int {
	var acc int64
	last := -1
	for i, w := range weights {
		if w <= 0 {
			continue
		}
		last = i
		acc += w
		if roll < acc {
			return i
		}
	}
	return last
}
