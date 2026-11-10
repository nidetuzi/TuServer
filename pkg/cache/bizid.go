package cache

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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

// GetByBizIDOf 按业务编号读取并断言为指针实体类型 T（例如 *Guild）。
func GetByBizIDOf[T entity.Entity](ctx context.Context, c *Cache, st store.Store, serverID int32, collection string, id int64) (T, error) {
	var zero T
	rt := reflect.TypeOf(zero)
	if rt == nil || rt.Kind() != reflect.Pointer {
		return zero, errors.New("cache: GetByBizIDOf requires pointer entity type")
	}
	e, err := c.GetByBizID(ctx, st, serverID, collection, id, func() entity.Entity {
		return reflect.New(rt.Elem()).Interface().(entity.Entity)
	})
	if err != nil {
		return zero, err
	}
	out, ok := e.(T)
	if !ok {
		return zero, fmt.Errorf("cache: entity type %T is not %T", e, zero)
	}
	return out, nil
}
