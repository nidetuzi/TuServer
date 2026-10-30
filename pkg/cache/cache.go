package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"tuserv/pkg/entity"
	"tuserv/pkg/store"
)

type persister interface {
	Persist() (uint64, any)
}

// Cache 是进程内的实体表。同一个区服的同一个键只保留一份。
type Cache struct {
	mu      sync.Mutex
	flushMu sync.Mutex
	items   map[string]entity.Entity
	store   store.Store
}

// New 用给定存储创建缓存。store 为 nil 时只能使用 Put 进来的内存数据。
func New(st store.Store) *Cache {
	return &Cache{items: make(map[string]entity.Entity), store: st}
}

// Get 先读内存，没有再读库。读库失败且不是不存在时，不会把空对象放进缓存。
func (c *Cache) Get(ctx context.Context, serverID int32, collection, key string, factory func() entity.Entity) (entity.Entity, error) {
	id := itemKey(serverID, collection, key)
	c.mu.Lock()
	if e, ok := c.items[id]; ok {
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
		return old, nil
	}
	c.items[id] = e
	c.items[itemKey(e.ServerID(), e.Collection(), e.CacheKey())] = e
	return e, nil
}

// Adopt 把已经写入存储的实体放进内存，不再打脏标记。
// 创建角色走插入而不是延迟刷盘时用它，避免同一份文档被刷成两个版本。
func (c *Cache) Adopt(e entity.Entity) entity.Entity {
	if e == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	k := itemKey(e.ServerID(), e.Collection(), e.CacheKey())
	if old, ok := c.items[k]; ok {
		return old
	}
	c.items[k] = e
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
	for k, v := range c.items {
		if v == e {
			delete(c.items, k)
		}
	}
}

// Put 把新建的实体放进内存并打上脏标记，下次 Flush 才会插入数据库。
func (c *Cache) Put(e entity.Entity) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e.MarkDirty()
	c.items[itemKey(e.ServerID(), e.Collection(), e.CacheKey())] = e
}

// Alias 给同一份实体再挂一个查找键。角色要同时按编号和名字命中同一份内存。
func (c *Cache) Alias(e entity.Entity, key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[itemKey(e.ServerID(), e.Collection(), key)] = e
}

// Flush 把脏实体写回存储。刷盘期间又被改过的实体会保持脏标记，等下一次再写。
// 同一缓存的 Flush 互斥；快照比库里旧时丢掉这份内存，不能覆盖更新的文档。
func (c *Cache) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	c.mu.Lock()
	seen := make(map[entity.Entity]struct{})
	var dirty []entity.Entity
	for _, e := range c.items {
		if _, ok := seen[e]; ok || !e.Dirty() {
			continue
		}
		seen[e] = struct{}{}
		dirty = append(dirty, e)
	}
	c.mu.Unlock()
	if c.store == nil {
		return nil
	}
	for _, e := range dirty {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.saveOne(ctx, e); err != nil {
			return err
		}
	}
	return nil
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
