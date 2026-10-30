package entity

import "sync"

// Placement 决定实体内存放在哪个节点。
// 角色跟会话走，公会必须按键落到唯一主人，否则两个节点会各写一份。
type Placement int

const (
	// PlacementLocal 表示实体只活在当前会话所在的节点。
	PlacementLocal Placement = iota
	// PlacementRouted 表示实体按键哈希到唯一节点，其他节点只能转发。
	PlacementRouted
)

// Entity 是缓存和刷盘认识的最小对象。
type Entity interface {
	CacheKey() string
	ServerID() int32
	Collection() string
	RouteKey() string
	Placement() Placement
	MarkDirty()
	Dirty() bool
	// BeginSave 返回本次快照代数。刷盘成功后用 EndSave 消掉脏标记。
	// 代数是为了避免刷盘过程中的新修改被误判成已经落库。
	BeginSave() uint64
	EndSave(gen uint64)
}

// State 给业务实体嵌套的脏标记。导出是为了业务包不用再复制这套代数逻辑。
type State struct {
	mu    sync.Mutex
	dirty bool
	gen   uint64
}

// MarkDirty 记录一次尚未落库的修改。
func (s *State) MarkDirty() {
	s.mu.Lock()
	s.gen++
	s.dirty = true
	s.mu.Unlock()
}

// Dirty 报告是否还有未落库的修改。
func (s *State) Dirty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dirty
}

// BeginSave 取走当前代数。调用方应在持有同一把业务锁时拷贝字段。
func (s *State) BeginSave() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gen
}

// Capture 在同一把锁里拷贝业务字段并取走代数。
// 刷盘若在拷贝后再上锁，会把写了一半的结构体写进数据库。
func (s *State) Capture(copy func()) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy()
	return s.gen
}

// EndSave 仅在刷盘期间没有新修改时清除脏标记。
func (s *State) EndSave(gen uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gen == gen {
		s.dirty = false
	}
}

// InitRev 把新建文档的代数设为 rev，并视为已经与库一致。
// 创建后立刻插入的角色要用它，避免下一次刷盘拿更小的代数去覆盖库里的版本。
func (s *State) InitRev(rev uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rev == 0 {
		rev = 1
	}
	s.gen = rev
	s.dirty = false
}

// Restore 用库里读到的代数恢复内存。
// 不恢复的话，重新加载后的修改会从 1 开始，条件写就挡不住更旧的快照。
func (s *State) Restore(rev uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gen = rev
	s.dirty = false
}

// Generation 返回当前代数。刷盘条件写要用同一把锁里读到的值。
func (s *State) Generation() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gen
}

// Lock 与业务字段快照共用，避免拷贝到写了一半的结构体。
func (s *State) Lock() { s.mu.Lock() }

// Unlock 释放快照锁。
func (s *State) Unlock() { s.mu.Unlock() }
