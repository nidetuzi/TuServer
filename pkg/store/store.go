package store

import (
	"context"
	"errors"
)

// ErrNotFound 表示库里没有这条记录。缓存的 Get 把它交给 service，由 service 决定是否创建。
var ErrNotFound = errors.New("store: not found")

// ErrDuplicate 表示主键或唯一索引冲突。同名角色并发创建时，后写入的一方拿到它再改读已有文档。
var ErrDuplicate = errors.New("store: duplicate")

// ErrStale 表示库里的代数或任期已经比这份快照新。调用方必须丢掉快照，不能重试覆盖。
var ErrStale = errors.New("store: stale revision")

// Store 按区服、集合和主键读写文档。实现可以是 Mongo，测试里用内存。
type Store interface {
	Load(ctx context.Context, serverID int32, collection, key string, dest any) error
	Save(ctx context.Context, serverID int32, collection, key string, doc any) error
	// Insert 只在主键和唯一索引都空着时写入。已存在则返回 ErrDuplicate，不会覆盖。
	Insert(ctx context.Context, serverID int32, collection, key string, doc any) error
	// EnsureUnique 保证某个字段在该区服集合里唯一。重复调用同一字段应保持成功。
	EnsureUnique(ctx context.Context, serverID int32, collection, field string) error
	Find(ctx context.Context, serverID int32, collection, field string, value any, dest any) error
	// NextID 从该区服的 uuid 计数器取下一个业务编号。多进程靠同一次原子自增，不在内存里各自计数。
	NextID(ctx context.Context, serverID int32, name string) (int64, error)
}

// LeaseRec 是一把跨进程租约。Found 为 false 表示库里还没有这条记录。
type LeaseRec struct {
	Owner   string
	Term    uint64
	Until   int64
	Gate    string
	Session string
	Found   bool
}

// Locker 用比较并交换保存租约。fn 返回错误时不得写入。
type Locker interface {
	MutateLease(ctx context.Context, serverID int32, key string, fn func(LeaseRec) (LeaseRec, error)) (LeaseRec, error)
}

// Versioned 是带代数和任期的落库快照。没有实现该接口的文档仍按整篇覆盖写入。
type Versioned interface {
	DocVersion() (rev, fence uint64)
}

// DocVersion 读取快照上的代数和任期。ok 为 false 表示这份文档不参与条件写。
func DocVersion(doc any) (rev, fence uint64, ok bool) {
	v, ok := doc.(Versioned)
	if !ok || v == nil {
		return 0, 0, false
	}
	rev, fence = v.DocVersion()
	return rev, fence, true
}

// Stale 判断库里的版本是否已经比快照新。任一代数或任期领先都不能覆盖。
func Stale(dbRev, dbFence, snapRev, snapFence uint64) bool {
	return dbRev > snapRev || dbFence > snapFence
}
