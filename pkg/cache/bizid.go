package cache

import (
	"context"
	"errors"
	"strconv"

	"tuserv/pkg/entity"
	"tuserv/pkg/store"
)

// GetByBizID 先按业务编号别名读内存，未命中再按 id 字段查库并挂上别名。
// 业务编号不是 Mongo _id，不能直接拿去 Load。
func (c *Cache) GetByBizID(ctx context.Context, st store.Store, serverID int32, collection string, id int64, factory func() entity.Entity) (entity.Entity, error) {
	key := strconv.FormatInt(id, 10)
	if e, err := c.Get(ctx, serverID, collection, key, nil); err == nil {
		return e, nil
	} else if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	if st == nil || factory == nil {
		return nil, store.ErrNotFound
	}
	found := factory()
	if err := st.Find(ctx, serverID, collection, "id", id, found); err != nil {
		return nil, err
	}
	e, err := c.Get(ctx, serverID, collection, found.CacheKey(), factory)
	if err != nil {
		return nil, err
	}
	c.Alias(e, key)
	return e, nil
}
