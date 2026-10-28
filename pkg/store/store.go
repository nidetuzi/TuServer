package store

import (
	"context"
	"errors"
)

// ErrNotFound 表示库里没有这条记录。缓存的 Get 把它交给 service，由 service 决定是否创建。
var ErrNotFound = errors.New("store: not found")

// Store 按区服、集合和主键读写文档。实现可以是 Mongo，测试里用内存。
type Store interface {
	Load(ctx context.Context, serverID int32, collection, key string, dest any) error
	Save(ctx context.Context, serverID int32, collection, key string, doc any) error
	Find(ctx context.Context, serverID int32, collection, field string, value any, dest any) error
	// NextID 从该区服的 uuid 计数器取下一个业务编号。多进程靠同一次原子自增，不在内存里各自计数。
	NextID(ctx context.Context, serverID int32, name string) (int64, error)
}
