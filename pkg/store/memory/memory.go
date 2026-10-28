package memory

import (
	"context"
	"sync"

	"go.mongodb.org/mongo-driver/bson"

	"tuserv/pkg/store"
)

type record struct {
	raw bson.Raw
}

// Store 把文档按 bson 存进内存，使字段名和 Mongo 实现一致。
type Store struct {
	mu   sync.Mutex
	docs map[string]record
	load int
}

// New 创建空的内存库。
func New() *Store {
	return &Store{docs: make(map[string]record)}
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

// Save 覆盖主键对应的文档。
func (s *Store) Save(_ context.Context, serverID int32, collection, key string, doc any) error {
	raw, err := bson.Marshal(doc)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.docs[id(serverID, collection, key)] = record{raw: raw}
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

func id(serverID int32, collection, key string) string {
	return collection + "|" + itoa(serverID) + "|" + key
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
