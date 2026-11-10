package entity

import "go.mongodb.org/mongo-driver/bson/primitive"

// Identity 给可持久化实体嵌套 ObjectID 与条件写字段。
// 同时嵌入 State，避免业务再手写 CacheKey/NoteLoaded/SetFence/InitRev。
type Identity struct {
	State `bson:"-"`
	OID   primitive.ObjectID `bson:"_id"`
	Rev   uint64             `bson:"rev,omitempty"`
	Fence uint64             `bson:"fence,omitempty"`
}

// CacheKey 使用 ObjectID 十六进制，业务编号不能当 Mongo _id。
func (id *Identity) CacheKey() string { return id.OID.Hex() }

// NoteLoaded 用库里的代数恢复内存版本，条件写才不会被更旧快照盖掉。
func (id *Identity) NoteLoaded() { id.Restore(id.Rev) }

// SetFence 记下当前持有的写入任期。更小的任期不能覆盖刚抢到的任期。
func (id *Identity) SetFence(term uint64) {
	if term == 0 {
		return
	}
	id.Lock()
	if term > id.Fence {
		id.Fence = term
	}
	id.Unlock()
}

// InitRev 把新建文档的代数与库对齐，并同步写入 Rev 字段供下次加载。
func (id *Identity) InitRev(rev uint64) {
	id.State.InitRev(rev)
	if rev == 0 {
		rev = 1
	}
	id.Rev = rev
}
