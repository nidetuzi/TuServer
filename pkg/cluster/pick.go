package cluster

import (
	"errors"
	"hash/fnv"
	"math"
)

// ErrNoInstance 表示当前没有可用的逻辑节点。
var ErrNoInstance = errors.New("cluster: no instance")

// Instance 是注册中心里的一个进程。Weight 小于等于 0 的节点不参与加权随机。
type Instance struct {
	Addr   string
	Weight int
}

// PickWeighted 按权重随机选一个实例。roll 必须返回 [0, total) 内的数，测试才能复现比例。
func PickWeighted(list []Instance, roll func(total int) int) (Instance, error) {
	total := 0
	for _, it := range list {
		if it.Weight > 0 {
			total += it.Weight
		}
	}
	if total == 0 {
		return Instance{}, ErrNoInstance
	}
	n := roll(total)
	if n < 0 || n >= total {
		n = 0
	}
	for _, it := range list {
		if it.Weight <= 0 {
			continue
		}
		if n < it.Weight {
			return it, nil
		}
		n -= it.Weight
	}
	return list[len(list)-1], nil
}

// Owner 用加权 rendezvous hashing 为实体选主人。
// 增减节点时，只有原本会选中新节点或被摘掉节点的键会迁移，而不是按节点个数取模后全体重映射。
// Weight 小于等于 0 的节点不参与，用来把正在摘流的进程从公会主人里拿掉。
func Owner(list []Instance, key string) (Instance, error) {
	best := -1
	var bestScore float64
	for i, it := range list {
		if it.Weight <= 0 || it.Addr == "" {
			continue
		}
		score := rendezvousScore(key, it.Addr, it.Weight)
		if best < 0 || score > bestScore || (score == bestScore && it.Addr < list[best].Addr) {
			best = i
			bestScore = score
		}
	}
	if best < 0 {
		return Instance{}, ErrNoInstance
	}
	return list[best], nil
}

// rendezvousScore 是加权 HRW：weight / -ln(u)。权重越大越容易成为最高分。
// FNV 对「短键 + 单字节地址」的高位不够散，先做一次 splitmix 再映射，避免新增节点搬走一半的键。
func rendezvousScore(key, addr string, weight int) float64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(addr))
	mixed := splitmix64(h.Sum64())
	// 取高 53 位映射到 (0, 1]，避免 ln(0)。
	u := (float64(mixed>>11) + 1) / float64(uint64(1)<<53)
	if u > 1 {
		u = 1
	}
	score := float64(weight) / -math.Log(u)
	if math.IsInf(score, 0) || math.IsNaN(score) {
		return math.MaxFloat64
	}
	return score
}

func splitmix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
