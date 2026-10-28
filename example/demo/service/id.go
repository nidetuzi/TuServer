package service

import (
	"context"
	"errors"
	"strconv"

	"tuserv/pkg/cache"
	core "tuserv/pkg/entity"
	"tuserv/pkg/store"
)

func idKey(id int64) string {
	return strconv.FormatInt(id, 10)
}

// remember 先按自增号找内存。没有再按 id 字段读库，并用 ObjectID 放进缓存。
// 直接用自增号去 Load 会把它当成 _id，文档在库里对不上。
func remember(ctx context.Context, c *cache.Cache, st store.Store, serverID int32, collection string, id int64, factory func() core.Entity) (core.Entity, error) {
	key := idKey(id)
	if e, err := c.Get(ctx, serverID, collection, key, nil); err == nil {
		return e, nil
	} else if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, err
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
