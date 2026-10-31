package memory

import (
	"context"
	"sync"

	"go.mongodb.org/mongo-driver/bson"

	"tuserv/pkg/store"
	"tuserv/pkg/store/internal/partial"
)

type record struct {
	raw bson.Raw
}

// Store 把文档按 bson 存进内存，使字段名和 Mongo 实现一致。
type Store struct {
	mu      sync.Mutex
	docs    map[string]record
	seqs    map[string]int64
	leases  map[string]store.LeaseRec
	uniques map[string][]string
	load    int
}

// New 创建空的内存库。
func New() *Store {
	return &Store{
		docs:    make(map[string]record),
		seqs:    make(map[string]int64),
		leases:  make(map[string]store.LeaseRec),
		uniques: make(map[string][]string),
	}
}

// Loads 返回 Load 被调用的次数，用来确认第二次 Get 没有再读库。
func (s *Store) Loads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.load
}

// Load 按主键读取。没有记录时返回 ErrNotFound，而不是空对象。
func (s *Store) Load(_ context.Context, serverID int32, collection, key string, dest any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.load++
	rec, ok := s.docs[id(serverID, collection, key)]
	if !ok {
		return store.ErrNotFound
	}
	return bson.Unmarshal(rec.raw, dest)
}

// Save 按本进程认识的字段更新。带版本的快照在库里代数或任期更高时返回 ErrStale。
// 已有文档只覆盖这次编码里出现的键，避免滚动升级时旧结构抹掉新字段；没有旧文档时仍整篇写入。
func (s *Store) Save(_ context.Context, serverID int32, collection, key string, doc any) error {
	fresh, err := bson.Marshal(doc)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := id(serverID, collection, key)
	var old bson.Raw
	if cur, exists := s.docs[k]; exists {
		if rev, fence, ok := store.DocVersion(doc); ok {
			dbRev, dbFence := versionFromRaw(cur.raw)
			if store.Stale(dbRev, dbFence, rev, fence) {
				return store.ErrStale
			}
		}
		old = cur.raw
	}
	raw, err := partial.MergeKnown(old, fresh)
	if err != nil {
		return err
	}
	if err := s.checkUniqueLocked(serverID, collection, key, raw); err != nil {
		return err
	}
	s.docs[k] = record{raw: raw}
	return nil
}

// Insert 拒绝已有主键或唯一索引冲突，避免同名创建互相覆盖。
func (s *Store) Insert(_ context.Context, serverID int32, collection, key string, doc any) error {
	raw, err := bson.Marshal(doc)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := id(serverID, collection, key)
	if _, exists := s.docs[k]; exists {
		return store.ErrDuplicate
	}
	if err := s.checkUniqueLocked(serverID, collection, key, raw); err != nil {
		return err
	}
	s.docs[k] = record{raw: raw}
	return nil
}

// EnsureUnique 记住该区服集合的唯一字段，之后的 Save 和 Insert 会检查。
func (s *Store) EnsureUnique(_ context.Context, serverID int32, collection, field string) error {
	if field == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := itoa(serverID) + "|" + collection
	for _, got := range s.uniques[k] {
		if got == field {
			return nil
		}
	}
	s.uniques[k] = append(s.uniques[k], field)
	return nil
}

// Find 按单个字段查找。登录按角色名找人时，主键是角色编号，名字不在主键上。
func (s *Store) Find(_ context.Context, serverID int32, collection, field string, value any, dest any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := id(serverID, collection, "")
	for k, rec := range s.docs {
		if len(k) < len(prefix) || k[:len(prefix)] != prefix {
			continue
		}
		var probe bson.M
		if err := bson.Unmarshal(rec.raw, &probe); err != nil {
			return err
		}
		if probe[field] == value {
			return bson.Unmarshal(rec.raw, dest)
		}
	}
	return store.ErrNotFound
}

// NextID 在本进程里模拟 uuid 计数器。每个区服、每个名字各自从 1 开始。
func (s *Store) NextID(_ context.Context, serverID int32, name string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := itoa(serverID) + "|" + name
	s.seqs[k]++
	return s.seqs[k], nil
}

func id(serverID int32, collection, key string) string {
	return collection + "|" + itoa(serverID) + "|" + key
}

// RenewMany 在同一把锁里续上仍然匹配的租约。
// 不插入新记录，也不修改持有者和任期，避免批量续期把别人刚接走的租约覆盖掉。
func (s *Store) RenewMany(_ context.Context, items []store.LeaseTouch) ([]store.LeaseMiss, error) {
	if len(items) == 0 {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leases == nil {
		s.leases = make(map[string]store.LeaseRec)
	}
	var misses []store.LeaseMiss
	for _, it := range items {
		k := itoa(it.ServerID) + "|" + it.Key
		cur, ok := s.leases[k]
		// 不存在、已释放或持有者不同，都还不是自己的租约。零值 Found 为 false，和库里没有是同一条路。
		if !ok || !cur.Found || cur.Owner != it.Owner {
			owner := ""
			if ok && cur.Found {
				owner = cur.Owner
			}
			misses = append(misses, store.LeaseMiss{ServerID: it.ServerID, Key: it.Key, Owner: owner, Err: store.ErrLeaseHeld})
			continue
		}
		// 旧任期不能把本节点后来拿到的新任期续长，否则断线时按旧任期释放会放空。
		if it.Term == 0 || cur.Term != it.Term {
			misses = append(misses, store.LeaseMiss{ServerID: it.ServerID, Key: it.Key, Owner: cur.Owner, Err: store.ErrLeaseTerm})
			continue
		}
		cur.Until = it.Until
		cur.Found = true
		s.leases[k] = cur
	}
	if len(misses) == 0 {
		return nil, nil
	}
	return misses, nil
}

// MutateLease 在锁内完成租约的读改写，fn 返回错误时保持原值。
func (s *Store) MutateLease(_ context.Context, serverID int32, key string, fn func(store.LeaseRec) (store.LeaseRec, error)) (store.LeaseRec, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leases == nil {
		s.leases = make(map[string]store.LeaseRec)
	}
	k := itoa(serverID) + "|" + key
	cur := s.leases[k]
	next, err := fn(cur)
	if err != nil {
		return cur, err
	}
	next.Found = true
	s.leases[k] = next
	return next, nil
}

func (s *Store) checkUniqueLocked(serverID int32, collection, key string, raw bson.Raw) error {
	fields := s.uniques[itoa(serverID)+"|"+collection]
	if len(fields) == 0 {
		return nil
	}
	var probe bson.M
	if err := bson.Unmarshal(raw, &probe); err != nil {
		return err
	}
	self := id(serverID, collection, key)
	prefix := id(serverID, collection, "")
	for _, field := range fields {
		val, ok := probe[field]
		if !ok || emptyUnique(val) {
			continue
		}
		for k, rec := range s.docs {
			if k == self || len(k) < len(prefix) || k[:len(prefix)] != prefix {
				continue
			}
			var other bson.M
			if err := bson.Unmarshal(rec.raw, &other); err != nil {
				return err
			}
			if sameValue(other[field], val) {
				return store.ErrDuplicate
			}
		}
	}
	return nil
}

func versionFromRaw(raw bson.Raw) (rev, fence uint64) {
	var probe bson.M
	if err := bson.Unmarshal(raw, &probe); err != nil {
		return 0, 0
	}
	return asUint(probe["rev"]), asUint(probe["fence"])
}

func asUint(v any) uint64 {
	switch n := v.(type) {
	case int32:
		if n > 0 {
			return uint64(n)
		}
	case int64:
		if n > 0 {
			return uint64(n)
		}
	case uint32:
		return uint64(n)
	case uint64:
		return n
	case float64:
		if n > 0 {
			return uint64(n)
		}
	}
	return 0
}

func emptyUnique(v any) bool {
	switch n := v.(type) {
	case nil:
		return true
	case string:
		return n == ""
	case int32:
		return n == 0
	case int64:
		return n == 0
	case float64:
		return n == 0
	default:
		return false
	}
}

func sameValue(a, b any) bool {
	if a == nil || b == nil {
		return false
	}
	return asText(a) == asText(b)
}

func asText(v any) string {
	switch n := v.(type) {
	case string:
		return n
	case int32:
		return strconvI64(int64(n))
	case int64:
		return strconvI64(n)
	case uint64:
		return strconvI64(int64(n))
	case float64:
		return strconvI64(int64(n))
	default:
		return ""
	}
}

func strconvI64(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	neg := n < 0
	u := uint64(n)
	if neg {
		u = uint64(-n)
	}
	for u > 0 {
		i--
		b[i] = byte('0' + u%10)
		u /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

func itoa(n int32) string {
	if n == 0 {
		return "0"
	}
	var b [12]byte
	i := len(b)
	u := uint32(n)
	neg := n < 0
	if neg {
		u = uint32(-n)
	}
	for u > 0 {
		i--
		b[i] = byte('0' + u%10)
		u /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
