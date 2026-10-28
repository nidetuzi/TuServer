package cluster

import (
	"errors"
	"hash/fnv"
	"sort"
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

// Owner 按实体键在实例列表上做稳定选择。
// 公会这类共享实体必须始终落到同一台节点，不能跟着玩家会话走。
func Owner(list []Instance, key string) (Instance, error) {
	if len(list) == 0 {
		return Instance{}, ErrNoInstance
	}
	sorted := append([]Instance(nil), list...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Addr < sorted[j].Addr })
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return sorted[int(h.Sum32())%len(sorted)], nil
}
