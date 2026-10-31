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

// LeaseTouch 是一次只延长到期时间的续期。
// Owner 和 Term 是匹配条件：对不上就不动这条记录，续期不能把已经交出去的租约抢回来。
type LeaseTouch struct {
	ServerID int32
	Key      string
	Owner    string
	Term     uint64
	Until    int64
}

// LeaseMiss 是没有续上的一条。
// Owner 是库里现在的持有者，不存在或已释放时为空。Err 用来区分被别人持有和任期已经变了。
type LeaseMiss struct {
	ServerID int32
	Key      string
	Owner    string
	Err      error
}

// ErrLeaseHeld 表示租约不存在、已释放，或持有者不是这次给出的 owner。调用方应放弃本地会话，不能据此去抢。
var ErrLeaseHeld = errors.New("store: lease held")

// ErrLeaseTerm 表示租约仍由该 owner 持有，但已经不是这次记录的任期。不能当成被别人持有，否则会把本节点仍在线的会话踢掉。
var ErrLeaseTerm = errors.New("store: lease term changed")

// Locker 用比较并交换保存租约。fn 返回错误时不得写入。
// RenewMany 把多条续期合成一次写，只延长仍然由给定 owner 和 term 持有的租约。
type Locker interface {
	MutateLease(ctx context.Context, serverID int32, key string, fn func(LeaseRec) (LeaseRec, error)) (LeaseRec, error)
	RenewMany(ctx context.Context, items []LeaseTouch) ([]LeaseMiss, error)
}

// Versioned 是带代数和任期的落库快照。没有实现该接口的文档不做条件写。
// 两种文档都按字段更新，不整篇替换。
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
