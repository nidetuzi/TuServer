package randutil

import (
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"math"
	mathrand "math/rand/v2"
	"sync"
)

var (
	// ErrRange 表示区间或比例的上下界无法抽样，例如 min 大于 max，或分母不是正数。
	ErrRange = errors.New("randutil: invalid range")

	// ErrEmpty 表示池里没有正权重项。空池、权重全部为 0 或负数都走这个错误。
	ErrEmpty = errors.New("randutil: empty pool")

	// ErrCount 表示抽取数量不合法。数量为负，或不重复抽取时超过正权重项个数。
	ErrCount = errors.New("randutil: invalid count")

	// ErrWeight 表示正权重之和超过 int64。继续累加会回绕，抽中的比例就不再是配置写的比例。
	ErrWeight = errors.New("randutil: weight overflow")
)

// Rand 是一份独立的随机源。
// 同一实例可以跨协程调用；锁只保护这一份源。不同玩家应各自持有，避免抽卡挤在同一把锁上。
// 并发调用虽然安全，但协程会交错推进同一条序列。要用种子回放时，必须在单协程里按同样的顺序调用。
// 不要复制 Rand 值，复制会把锁和内部状态拆开，按指针传递。
// 它只服务玩法抽样。密钥、令牌和签名仍要走 crypto/rand，不能从这里取字节。
type Rand struct {
	mu  sync.Mutex
	src *mathrand.Rand
}

// New 用操作系统熵创建随机源，供线上发奖和抽卡。
// 熵源失败时直接返回错误，不用时间做种子：同一秒内启动的进程会抽出同一串奖励。
func New() (*Rand, error) {
	var seed [32]byte
	if _, err := crand.Read(seed[:]); err != nil {
		return nil, err
	}
	return newChaCha(seed), nil
}

// NewSeed 用 64 位种子创建可复现的随机源。
// 同一个种子、同一串调用，结果一致，测试和问题回放靠这个对齐。
// 种子只有 64 位，并且派生过程是公开的。线上发奖要用 New，不要用能被猜到的种子。
func NewSeed(seed uint64) *Rand {
	return newChaCha(expandSeed(seed))
}

func newChaCha(seed [32]byte) *Rand {
	return &Rand{src: mathrand.New(mathrand.NewChaCha8(seed))}
}

// expandSeed 把一个 64 位种子铺成 ChaCha8 的 32 字节密钥。
// 线上和测试必须用同一个生成器，回放时序列才和进程里的一致；ChaCha8 不接受单独一个 uint64。
// 选用 ChaCha8 而不是 PCG，是为了避免玩家从已经看到的几发奖励推算出下一发。
func expandSeed(seed uint64) [32]byte {
	var out [32]byte
	x := seed
	for i := 0; i < 4; i++ {
		var word uint64
		x, word = splitmix64(x)
		binary.LittleEndian.PutUint64(out[i*8:(i+1)*8], word)
	}
	return out
}

// splitmix64 把种子搅匀后再截成密钥。
// 直接把 seed 填进密钥前 8 字节、其余补 0，相邻种子的序列会贴得太近。
func splitmix64(x uint64) (uint64, uint64) {
	x += 0x9e3779b97f4a7c15
	z := x
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return x, z ^ (z >> 31)
}

func (r *Rand) mustInit() {
	if r == nil || r.src == nil {
		panic("randutil: Rand 未初始化，请使用 New 或 NewSeed")
	}
}

// Int 返回闭区间 [min, max] 上的整数。
// 配置里的“随机 1 到 100”包含 100，这里用闭区间，避免和 math/rand 的半开区间各写一套。
// min 大于 max 返回 ErrRange。区间里只有一个整数时不再取样，把奖励收成定值不会把后面的序列挤开。
func (r *Rand) Int(min, max int) (int, error) {
	v, err := r.Int64(int64(min), int64(max))
	if err != nil {
		return 0, err
	}
	return int(v), nil
}

// Int64 返回闭区间 [min, max] 上的 int64。语义和 Int 相同，给大额数量用。
func (r *Rand) Int64(min, max int64) (int64, error) {
	r.mustInit()
	if min > max {
		return 0, ErrRange
	}
	if min == max {
		return min, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.int64Locked(min, max), nil
}

// int64Locked 在 min 小于 max 时取样。
// 跨度用 uint64 相减，再加回 min 的位模式，避免顶满 int64 时 max-min+1 溢出或变成负数。
func (r *Rand) int64Locked(min, max int64) int64 {
	span := uint64(max) - uint64(min) + 1
	if span == 0 {
		// 两端盖住全部 int64 时，跨度是 2^64，放不进 Uint64N 的参数。
		return int64(r.src.Uint64())
	}
	return int64(uint64(min) + r.src.Uint64N(span))
}

// IntN 返回半开区间 [0, n)。n 小于等于 0 时返回 ErrRange。
// 只有 0 这一个取值时不取样，理由和 Int 的单点区间相同。
func (r *Rand) IntN(n int) (int, error) {
	r.mustInit()
	if n <= 0 {
		return 0, ErrRange
	}
	if n == 1 {
		return 0, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return int(r.src.Int64N(int64(n))), nil
}

// Float64 返回半开区间 [0, 1) 上的浮点数。上界取不到，和阈值比较时不会出现比 1 还大的抽样。
func (r *Rand) Float64() float64 {
	r.mustInit()
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.src.Float64()
}

// Float64Range 返回 [min, max) 上的浮点数。
// 浮点没有可靠的闭区间，伤害、坐标这类连续量用半开区间。
// min 大于 max、NaN、无穷，或间距本身溢出成无穷时返回 ErrRange。min 等于 max 时不取样。
func (r *Rand) Float64Range(min, max float64) (float64, error) {
	r.mustInit()
	if math.IsNaN(min) || math.IsNaN(max) || math.IsInf(min, 0) || math.IsInf(max, 0) || min > max {
		return 0, ErrRange
	}
	if min == max {
		return min, nil
	}
	span := max - min
	if math.IsInf(span, 0) {
		return 0, ErrRange
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	v := min + r.src.Float64()*span
	if v >= max {
		// 极端区间下乘法会舍入到上界，收回去才能维持半开区间。
		v = math.Nextafter(max, min)
	}
	if v < min {
		v = min
	}
	return v, nil
}

// Hit 按 hit/scale 判断是否命中。万分比把 scale 配成 10000。
// scale 小于等于 0 无法表示比例，返回 ErrRange。
// hit 小于等于 0 视为关掉的奖励，hit 大于等于 scale 视为必中；这两种都不取样，
// 开关一条保底掉落时，后面的抽卡序列仍能按原种子复现。
func (r *Rand) Hit(hit, scale int) (bool, error) {
	r.mustInit()
	if scale <= 0 {
		return false, ErrRange
	}
	if hit <= 0 {
		return false, nil
	}
	if hit >= scale {
		return true, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.src.Int64N(int64(scale)) < int64(hit), nil
}

// Shuffle 原地打乱 items，每种排列概率相同。
// 抽完奖励后的展示顺序如果另用一份随机源，同一颗种子就复现不出客户端看到的顺序。
// 长度不足 2 时没有可交换的元素，不取样。
func Shuffle[T any](r *Rand, items []T) {
	r.mustInit()
	if len(items) < 2 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := len(items) - 1; i > 0; i-- {
		j := int(r.src.Uint64N(uint64(i + 1)))
		items[i], items[j] = items[j], items[i]
	}
}
