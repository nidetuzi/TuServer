package mongo

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
	err := s.coll(serverID, collection).FindOne(ctx, bson.M{"_id": idValue(key)}).Decode(dest)
	if errors.Is(err, drv.ErrNoDocuments) {
		return store.ErrNotFound
	}
	return err
}

// Save 用替换写入。带版本的快照只在库里的代数和任期都不比快照新时覆盖，避免旧刷盘打回新数据。
func (s *Store) Save(ctx context.Context, serverID int32, collection, key string, doc any) error {
	coll := s.coll(serverID, collection)
	id := idValue(key)
	rev, fence, ok := store.DocVersion(doc)
	if !ok {
		_, err := coll.ReplaceOne(ctx, bson.M{"_id": id}, doc, options.Replace().SetUpsert(true))
		return err
	}
	res, err := coll.ReplaceOne(ctx, versionFilter(id, rev, fence), doc)
	if err != nil {
		return err
	}
	if res.MatchedCount > 0 {
		return nil
	}
	n, err := coll.CountDocuments(ctx, bson.M{"_id": id})
	if err != nil {
		return err
	}
	if n == 0 {
		_, err = coll.InsertOne(ctx, doc)
		if drv.IsDuplicateKeyError(err) {
			return store.ErrStale
		}
		return err
	}
	return store.ErrStale
}

// Insert 只插入新文档。主键或唯一索引冲突时返回 ErrDuplicate。
func (s *Store) Insert(ctx context.Context, serverID int32, collection, key string, doc any) error {
	_, err := s.coll(serverID, collection).InsertOne(ctx, doc)
	if drv.IsDuplicateKeyError(err) {
		return store.ErrDuplicate
	}
	return err
}

// EnsureUnique 为区服集合创建唯一索引。已存在的同名索引再次创建会成功返回。
func (s *Store) EnsureUnique(ctx context.Context, serverID int32, collection, field string) error {
	if field == "" {
		return nil
	}
	_, err := s.coll(serverID, collection).Indexes().CreateOne(ctx, drv.IndexModel{
		Keys:    bson.D{{Key: field, Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	return err
}

func versionFilter(id any, rev, fence uint64) bson.M {
	return bson.M{
		"_id": id,
		"$and": []bson.M{
			{"$or": []bson.M{
				{"rev": bson.M{"$lte": rev}},
				{"rev": bson.M{"$exists": false}},
			}},
			{"$or": []bson.M{
				{"fence": bson.M{"$lte": fence}},
				{"fence": bson.M{"$exists": false}},
			}},
		},
	}
}

// Find 按字段查找。登录用名字，按业务编号读取用 id，两者都不是文档的 _id。
func (s *Store) Find(ctx context.Context, serverID int32, collection, field string, value any, dest any) error {
	err := s.coll(serverID, collection).FindOne(ctx, bson.M{field: value}).Decode(dest)
	if errors.Is(err, drv.ErrNoDocuments) {
		return store.ErrNotFound
	}
	return err
}

// NextID 用 uuid 集合做区服内自增。findOneAndUpdate 默认返回自增前的文档，所以要加一才是这次分到的号。
// 文档还不存在时走 upsert。并发下可能撞主键，最多再试三次，和计数器争用失败时的常见处理一样。
func (s *Store) NextID(ctx context.Context, serverID int32, name string) (int64, error) {
	coll := s.coll(serverID, "uuid")
	var doc struct {
		Count int64 `bson:"count"`
	}
	err := coll.FindOneAndUpdate(ctx, bson.M{"_id": name}, bson.M{"$inc": bson.M{"count": int64(1)}}).Decode(&doc)
	if err == nil {
		return doc.Count + 1, nil
	}
	update := bson.M{
		"$inc":         bson.M{"count": int64(1)},
		"$setOnInsert": bson.M{"_id": name},
	}
	opts := options.FindOneAndUpdate().SetUpsert(true)
	var last error
	for i := 0; i < 3; i++ {
		err = coll.FindOneAndUpdate(ctx, bson.M{"_id": name}, update, opts).Decode(&doc)
		if err == nil {
			return doc.Count + 1, nil
		}
		if errors.Is(err, drv.ErrNoDocuments) {
			return 1, nil
		}
		last = err
	}
	if last == nil {
		last = errors.New("store: next id")
	}
	return 0, last
}

// idValue 把 ObjectID 的十六进制还原成主键类型。缓存键是字符串，按字符串查会找不到 ObjectID。
func idValue(key string) any {
	if oid, err := primitive.ObjectIDFromHex(key); err == nil {
		return oid
	}
	return key
}

func (s *Store) coll(serverID int32, collection string) *drv.Collection {
	name := fmt.Sprintf("%s%d", s.prefix, serverID)
	return s.client.Database(name).Collection(collection)
}
