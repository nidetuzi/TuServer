package mongo

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	drv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"tuserv/pkg/store"
)

// Store 是一个 Mongo 客户端，库名形如 game_1。
type Store struct {
	client *drv.Client
	prefix string
}

// Open 连接 Mongo。prefix 为空时使用 game_，和配置文件的默认值保持一致。
func Open(ctx context.Context, uri, prefix string) (*Store, error) {
	if prefix == "" {
		prefix = "game_"
	}
	client, err := drv.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	return &Store{client: client, prefix: prefix}, nil
}

// Close 断开客户端。进程退出时调用，避免留下后台监控协程。
func (s *Store) Close(ctx context.Context) error {
	return s.client.Disconnect(ctx)
}

// Load 从对应区服的集合读取主键。
func (s *Store) Load(ctx context.Context, serverID int32, collection, key string, dest any) error {
	err := s.coll(serverID, collection).FindOne(ctx, bson.M{"_id": key}).Decode(dest)
	if errors.Is(err, drv.ErrNoDocuments) {
		return store.ErrNotFound
	}
	return err
}

// Save 用替换写入，保证脏文档整体覆盖旧文档，而不是只更新部分字段。
func (s *Store) Save(ctx context.Context, serverID int32, collection, key string, doc any) error {
	_, err := s.coll(serverID, collection).ReplaceOne(ctx, bson.M{"_id": key}, doc, options.Replace().SetUpsert(true))
	return err
}

// Find 按字段查找。角色登录用名字查，主键仍然是角色编号。
func (s *Store) Find(ctx context.Context, serverID int32, collection, field string, value any, dest any) error {
	err := s.coll(serverID, collection).FindOne(ctx, bson.M{field: value}).Decode(dest)
	if errors.Is(err, drv.ErrNoDocuments) {
		return store.ErrNotFound
	}
	return err
}

func (s *Store) coll(serverID int32, collection string) *drv.Collection {
	name := fmt.Sprintf("%s%d", s.prefix, serverID)
	return s.client.Database(name).Collection(collection)
}
