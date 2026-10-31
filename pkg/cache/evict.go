package cache

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	"tuserv/pkg/entity"
)

// Policy 决定什么时候把实体移出内存。两项都是 0 时不淘汰。
type Policy struct {
	// MaxItems 是不同实体的数量上限，别名不算。超过后按最久未访问淘汰。
	MaxItems int
	// Idle 是距上次 Get、Put、Adopt 或 Alias 的空闲时间，超过就可以离开内存。
	Idle time.Duration
}

// Option 调整缓存。放在 New 上，是为了不改已有的 New(store) 调用。
type Option func(*Cache)

// WithStaleHandler 在保存前捕获会话身份，只有条件写失败时才调用返回的通知。
// 通知处于刷盘流程内，只能投递异步任务，不能同步调用 Flush 或等待业务 actor。
func WithStaleHandler(prepare func(entity.Entity, uint64, uint64) func()) Option {
	return func(c *Cache) { c.prepareStale = prepare }
}

// WithPolicy 打开淘汰。负数按关闭处理，避免配错把缓存立刻清空。
func WithPolicy(p Policy) Option {
	if p.MaxItems < 0 {
		p.MaxItems = 0
	}
	if p.Idle < 0 {
		p.Idle = 0
	}
	return func(c *Cache) { c.policy = p }
}

// WithClock 替换淘汰使用的时间。测试要把空闲时间拨快，又不能真的睡觉。
// 回调不能再进这只缓存，否则会和缓存锁卡死。
func WithClock(now func() time.Time) Option {
	return func(c *Cache) {
		if now == nil {
			return
		}
		c.clock = now
	}
}

type guard struct {
	depth int
	ents  map[entity.Entity]struct{}
}

// Guard 钉住当前协程在这次调用期间放进或读到的实体。
// 角色邮箱和主人邮箱都在自己的协程里跑，Put 和 Adopt 又没有上下文，只能靠协程编号把同一次处理认出来。
// 解析失败时编号会落到 0，多一次处理叠在一起，结果是多钉住而不是把正在用的实体淘汰掉。
func (c *Cache) Guard() func() {
	gid := curGID()
	c.mu.Lock()
	g := c.guards[gid]
	if g == nil {
		g = &guard{ents: make(map[entity.Entity]struct{})}
		c.guards[gid] = g
	}
	g.depth++
	c.guardN.Add(1)
	c.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			g := c.guards[gid]
			if g == nil {
				c.guardN.Add(-1)
				c.mu.Unlock()
				return
			}
			g.depth--
			c.guardN.Add(-1)
			if g.depth > 0 {
				c.mu.Unlock()
				return
			}
			for e := range g.ents {
				c.unpinLocked(e)
			}
			delete(c.guards, gid)
			c.mu.Unlock()
		})
	}
}

// PinKey 按区服、集合和键钉住实体，直到调用返回的函数。
// 在线角色在两次协议之间没有邮箱在跑，只钉 Guard 盖不住这段空闲。
func (c *Cache) PinKey(serverID int32, collection, key string) func() {
	k := itemKey(serverID, collection, key)
	c.mu.Lock()
	c.keyPins[k]++
	c.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			c.keyPins[k]--
			if c.keyPins[k] <= 0 {
				delete(c.keyPins, k)
			}
			c.mu.Unlock()
		})
	}
}

// Evict 按空闲时间和容量丢掉实体。
// 脏数据会先走和 Flush 同一把锁去保存，失败就留在内存里。
// 不在这里释放租约：租约是跨节点的写入任期，不是这份内存还在不在。
// 断线和主人交接已经会先刷盘再 Release。淘汰时再 Release，下一次 Claim 会把任期抬高，
// 本节点还没交出去的快照就会变成 ErrStale，连同后来的修改一起被丢掉。
func (c *Cache) Evict(ctx context.Context) error {
	if c.policy.MaxItems <= 0 && c.policy.Idle <= 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	now := c.now()
	open, idleCount, need := c.plan(now)
	var errs []error
	removed := 0
	for i, it := range open {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		// 空闲的都要尝试。容量只再丢掉最老的若干个；前面保存失败就继续看下一个，不能卡死整轮。
		if i >= idleCount && removed >= need {
			break
		}
		ok, err := c.tryEvict(ctx, it.e)
		if err != nil {
			errs = append(errs, fmt.Errorf("cache: evict %s/%s: %w", it.e.Collection(), it.e.CacheKey(), err))
			continue
		}
		if ok {
			removed++
		}
	}
	return errors.Join(errs...)
}

func (c *Cache) tryEvict(ctx context.Context, e entity.Entity) (bool, error) {
	c.mu.Lock()
	if !c.containsLocked(e) || c.protectedLocked(e) {
		c.mu.Unlock()
		return false, nil
	}
	c.mu.Unlock()
	if e.Dirty() {
		// 没有存储就没法证明已经落库，脏实体必须留下。
		if c.store == nil {
			return false, nil
		}
		if err := c.saveOne(ctx, e); err != nil {
			return false, err
		}
		if !c.resident(e) {
			// ErrStale 已经把这份内存摘掉，不能再拿它去覆盖库里的文档。
			return true, nil
		}
	}
	return c.dropIfClean(e), nil
}

func (c *Cache) resident(e entity.Entity) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.containsLocked(e)
}

// dropIfClean 在实体锁里再确认一次没有变脏，然后才从内存摘掉。
func (c *Cache) dropIfClean(e entity.Entity) bool {
	if h, ok := e.(dirtyHeld); ok {
		h.Lock()
		dirty := h.DirtyHeld()
		if dirty {
			h.Unlock()
			return false
		}
		c.mu.Lock()
		dropped := c.containsLocked(e) && !c.protectedLocked(e)
		if dropped {
			c.forgetLocked(e)
		}
		c.mu.Unlock()
		h.Unlock()
		return dropped
	}
	if e.Dirty() {
		return false
	}
	c.mu.Lock()
	dropped := c.containsLocked(e) && !c.protectedLocked(e)
	if dropped {
		c.forgetLocked(e)
	}
	c.mu.Unlock()
	return dropped
}

type candidate struct {
	e    entity.Entity
	used time.Time
}

func (c *Cache) plan(now time.Time) ([]candidate, int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := make(map[entity.Entity]struct{})
	var open []candidate
	distinct := 0
	for _, e := range c.items {
		if e == nil {
			continue
		}
		if _, ok := seen[e]; ok {
			continue
		}
		seen[e] = struct{}{}
		distinct++
		if c.protectedLocked(e) {
			continue
		}
		used := time.Time{}
		if m := c.meta[e]; m != nil {
			used = m.lastUsed
		}
		open = append(open, candidate{e: e, used: used})
	}
	if len(open) == 0 {
		return nil, 0, 0
	}
	sort.Slice(open, func(i, j int) bool { return open[i].used.Before(open[j].used) })
	idleCount := 0
	if c.policy.Idle > 0 {
		for _, it := range open {
			if now.Sub(it.used) >= c.policy.Idle {
				idleCount++
				continue
			}
			break
		}
	}
	need := 0
	if c.policy.MaxItems > 0 && distinct > c.policy.MaxItems {
		need = distinct - c.policy.MaxItems
	}
	return open, idleCount, need
}

func (c *Cache) now() time.Time {
	if c.clock != nil {
		return c.clock()
	}
	return time.Now()
}

func (c *Cache) guarding() (int64, bool) {
	if c.guardN.Load() == 0 {
		return 0, false
	}
	return curGID(), true
}

func (c *Cache) trackLocked(gid int64, e entity.Entity) {
	g := c.guards[gid]
	if g == nil || e == nil {
		return
	}
	if _, ok := g.ents[e]; ok {
		// ErrStale 会把 meta 一起摘掉。同一次邮箱里又放回来时要重新钉上。
		if m := c.meta[e]; m != nil && m.pins > 0 {
			return
		}
	}
	g.ents[e] = struct{}{}
	c.pinLocked(e)
}

func (c *Cache) pinLocked(e entity.Entity) {
	c.ensureMetaLocked(e).pins++
}

func (c *Cache) unpinLocked(e entity.Entity) {
	m := c.meta[e]
	if m == nil {
		return
	}
	if m.pins > 0 {
		m.pins--
	}
	if m.pins == 0 && len(m.keys) == 0 {
		delete(c.meta, e)
	}
}

func curGID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	const prefix = "goroutine "
	if n <= len(prefix) {
		return 0
	}
	var id int64
	for i := len(prefix); i < n; i++ {
		d := buf[i]
		if d < '0' || d > '9' {
			break
		}
		id = id*10 + int64(d-'0')
	}
	return id
}
