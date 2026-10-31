package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"tuserv/pkg/entity"
	"tuserv/pkg/store"
)

type persister interface {
	Persist() (uint64, any)
}

// dirtyHeld 是嵌入了 entity.State 的实体。淘汰必须在状态锁里看脏标记。
type dirtyHeld interface {
	Lock()
	Unlock()
	DirtyHeld() bool
}

// meta 记录一份实体的访问时间和钉住次数。别名共用这一份，避免一个实体被算成多条。
type meta struct {
	lastUsed time.Time
	pins     int
	keys     map[string]struct{}
}

// Cache 是进程内的实体表。同一个区服的同一个键只保留一份。
type Cache struct {
	mu      sync.Mutex
	flushMu sync.Mutex
	items   map[string]entity.Entity
	meta    map[entity.Entity]*meta
	// keyPins 按缓存键钉住。在线角色只知道业务编号，实体指针要等别名挂上才能对上。
	keyPins map[string]int
	guards  map[int64]*guard
	guardN  atomic.Int32
	store   store.Store
	policy  Policy
	clock   func() time.Time
}

// New 用给定存储创建缓存。store 为 nil 时只能使用 Put 进来的内存数据。
// 不传 Policy 时不淘汰，测试和尚未配置的进程保持原样。
func New(st store.Store, opts ...Option) *Cache {
	c := &Cache{
		items:   make(map[string]entity.Entity),
		meta:    make(map[entity.Entity]*meta),
		keyPins: make(map[string]int),
		guards:  make(map[int64]*guard),
		store:   st,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	return c
}

// Len 返回内存里不同实体的数量。别名不重复计数。
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.distinctLocked()
}

// Get 先读内存，没有再读库。读库失败且不是不存在时，不会把空对象放进缓存。
func (c *Cache) Get(ctx context.Context, serverID int32, collection, key string, factory func() entity.Entity) (entity.Entity, error) {
	id := itemKey(serverID, collection, key)
	gid, guarded := c.guarding()
	c.mu.Lock()
	if e, ok := c.items[id]; ok {
		c.touchLocked(e)
		if guarded {
			c.trackLocked(gid, e)
		}
		c.mu.Unlock()
		return e, nil
	}
	c.mu.Unlock()
	if c.store == nil || factory == nil {
		return nil, store.ErrNotFound
	}
	e := factory()
	if err := c.load(ctx, serverID, collection, key, e); err != nil {
		return nil, err
	}
	if n, ok := e.(interface{ NoteLoaded() }); ok {
		n.NoteLoaded()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.items[id]; ok {
		c.touchLocked(old)
		if guarded {
			c.trackLocked(gid, old)
		}
		return old, nil
	}
	c.indexLocked(id, e)
	c.indexLocked(itemKey(e.ServerID(), e.Collection(), e.CacheKey()), e)
	if guarded {
		c.trackLocked(gid, e)
	}
	return e, nil
}

// Adopt 把已经写入存储的实体放进内存，不再打脏标记。
// 创建角色走插入而不是延迟刷盘时用它，避免同一份文档被刷成两个版本。
func (c *Cache) Adopt(e entity.Entity) entity.Entity {
	if e == nil {
		return nil
	}
	gid, guarded := c.guarding()
	c.mu.Lock()
	defer c.mu.Unlock()
	k := itemKey(e.ServerID(), e.Collection(), e.CacheKey())
	if old, ok := c.items[k]; ok {
		c.touchLocked(old)
		if guarded {
			c.trackLocked(gid, old)
		}
		return old
	}
	c.indexLocked(k, e)
	if guarded {
		c.trackLocked(gid, e)
	}
	return e
}

// Forget 从内存表去掉这份实体的全部别名。交出租约前调用，避免旧节点继续刷盘。
func (c *Cache) Forget(e entity.Entity) {
	if e == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.forgetLocked(e)
}

func (c *Cache) forgetLocked(e entity.Entity) {
	if m := c.meta[e]; m != nil {
		for k := range m.keys {
			if c.items[k] == e {
				delete(c.items, k)
			}
		}
		delete(c.meta, e)
		return
	}
	for k, v := range c.items {
		if v == e {
			delete(c.items, k)
		}
	}
}

// Put 把新建的实体放进内存并打上脏标记，下次 Flush 才会插入数据库。
func (c *Cache) Put(e entity.Entity) {
	if e == nil {
		return
	}
	// 先打脏再拿缓存锁。淘汰会先拿实体锁再拿缓存锁，反过来就会和 MarkDirty 卡死。
	e.MarkDirty()
	gid, guarded := c.guarding()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.indexLocked(itemKey(e.ServerID(), e.Collection(), e.CacheKey()), e)
	if guarded {
		c.trackLocked(gid, e)
	}
}

// Alias 给同一份实体再挂一个查找键。角色要同时按编号和名字命中同一份内存。
func (c *Cache) Alias(e entity.Entity, key string) {
	if e == nil || key == "" {
		return
	}
	gid, guarded := c.guarding()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.indexLocked(itemKey(e.ServerID(), e.Collection(), key), e)
	if guarded {
		c.trackLocked(gid, e)
	}
}

// Flush 把脏实体写回存储。刷盘期间又被改过的实体会保持脏标记，等下一次再写。
// 同一缓存的 Flush 互斥；快照比库里旧时丢掉这份内存，不能覆盖更新的文档。
// 某一条保存失败不会停下其余实体，否则后面本来能淘汰的干净实体也得一直占着内存。
func (c *Cache) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	// 先只拷贝指针。Dirty 要拿实体锁，不能在缓存锁里调用，否则和淘汰的加锁顺序相反。
	c.mu.Lock()
	seen := make(map[entity.Entity]struct{})
	var all []entity.Entity
	for _, e := range c.items {
		if e == nil {
			continue
		}
		if _, ok := seen[e]; ok {
			continue
		}
		seen[e] = struct{}{}
		all = append(all, e)
	}
	c.mu.Unlock()
	var dirty []entity.Entity
	for _, e := range all {
		if e.Dirty() {
			dirty = append(dirty, e)
		}
	}
	if c.store == nil {
		return nil
	}
	var errs []error
	for _, e := range dirty {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if err := c.saveOne(ctx, e); err != nil {
			errs = append(errs, fmt.Errorf("cache: save %s/%s: %w", e.Collection(), e.CacheKey(), err))
		}
	}
	return errors.Join(errs...)
}

// FlushEntity 只刷一份实体。交出租约前用它把脏数据交出去，而不是等下一轮全量刷盘。
func (c *Cache) FlushEntity(ctx context.Context, e entity.Entity) error {
	if e == nil || c.store == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	if !e.Dirty() {
		return nil
	}
	return c.saveOne(ctx, e)
}

func (c *Cache) saveOne(ctx context.Context, e entity.Entity) error {
	gen, doc := persistOf(e)
	err := c.store.Save(ctx, e.ServerID(), e.Collection(), e.CacheKey(), doc)
	if errors.Is(err, store.ErrStale) {
		// 快照已经落后。留在内存里会在下一轮把更新的文档盖掉，所以整份丢掉。
		e.EndSave(gen)
		c.mu.Lock()
		c.forgetLocked(e)
		c.mu.Unlock()
		return nil
	}
	if err != nil {
		return err
	}
	e.EndSave(gen)
	return nil
}

func (c *Cache) load(ctx context.Context, serverID int32, collection, key string, dest any) error {
	return c.store.Load(ctx, serverID, collection, key, dest)
}

func persistOf(e entity.Entity) (uint64, any) {
	if p, ok := e.(persister); ok {
		return p.Persist()
	}
	return e.BeginSave(), e
}

func itemKey(serverID int32, collection, key string) string {
	return fmt.Sprintf("%d|%s|%s", serverID, collection, key)
}

func (c *Cache) indexLocked(k string, e entity.Entity) {
	if old, ok := c.items[k]; ok && old != e {
		if m := c.meta[old]; m != nil {
			delete(m.keys, k)
			if len(m.keys) == 0 && m.pins == 0 {
				delete(c.meta, old)
			}
		}
	}
	c.items[k] = e
	m := c.ensureMetaLocked(e)
	m.keys[k] = struct{}{}
	m.lastUsed = c.now()
}

func (c *Cache) ensureMetaLocked(e entity.Entity) *meta {
	m := c.meta[e]
	if m == nil {
		m = &meta{keys: make(map[string]struct{})}
		c.meta[e] = m
	}
	if m.keys == nil {
		m.keys = make(map[string]struct{})
	}
	return m
}

func (c *Cache) touchLocked(e entity.Entity) {
	if m := c.meta[e]; m != nil {
		m.lastUsed = c.now()
	}
}

func (c *Cache) containsLocked(e entity.Entity) bool {
	m := c.meta[e]
	return m != nil && len(m.keys) > 0
}

func (c *Cache) distinctLocked() int {
	seen := make(map[entity.Entity]struct{})
	n := 0
	for _, e := range c.items {
		if e == nil {
			continue
		}
		if _, ok := seen[e]; ok {
			continue
		}
		seen[e] = struct{}{}
		n++
	}
	return n
}

func (c *Cache) protectedLocked(e entity.Entity) bool {
	m := c.meta[e]
	if m == nil {
		return false
	}
	if m.pins > 0 {
		return true
	}
	for k := range m.keys {
		if c.keyPins[k] > 0 {
			return true
		}
	}
	return false
}
