package mongo

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/bson"
	drv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"tuserv/pkg/store"
)

type leaseDoc struct {
	ID      string `bson:"_id"`
	Owner   string `bson:"owner"`
	Term    uint64 `bson:"term"`
	Until   int64  `bson:"until"`
	Gate    string `bson:"gate"`
	Session string `bson:"session"`
}

// MutateLease 用任期和持有者做乐观锁。fn 返回错误时不写库，冲突时重试几次。
func (s *Store) MutateLease(ctx context.Context, serverID int32, key string, fn func(store.LeaseRec) (store.LeaseRec, error)) (store.LeaseRec, error) {
	coll := s.coll(serverID, "lease")
	var last error
	for i := 0; i < 8; i++ {
		cur, found, err := loadLease(ctx, coll, key)
		if err != nil {
			return store.LeaseRec{}, err
		}
		next, ferr := fn(cur)
		if ferr != nil {
			return cur, ferr
		}
		doc := leaseDoc{ID: key, Owner: next.Owner, Term: next.Term, Until: next.Until, Gate: next.Gate, Session: next.Session}
		if !found {
			_, err = coll.InsertOne(ctx, doc)
			if drv.IsDuplicateKeyError(err) {
				last = err
				continue
			}
			if err != nil {
				return store.LeaseRec{}, err
			}
			next.Found = true
			return next, nil
		}
		res, err := coll.ReplaceOne(ctx, bson.M{"_id": key, "term": cur.Term, "owner": cur.Owner, "until": cur.Until}, doc)
		if err != nil {
			return store.LeaseRec{}, err
		}
		if res.MatchedCount == 1 {
			next.Found = true
			return next, nil
		}
		last = store.ErrStale
	}
	if last == nil {
		last = errors.New("lease: conflict")
	}
	return store.LeaseRec{}, last
}

// RenewMany 按区服各做一次 BulkWrite。每条更新都带 owner 和 term，对不上的不写。
// 一条 UpdateMany 无法为不同任期分别设条件，所以用无序 BulkWrite 把多次条件更新合成一次往返。
// 只改 until。改持有者或任期会把刚刚被别的节点接走的租约覆盖回去。
func (s *Store) RenewMany(ctx context.Context, items []store.LeaseTouch) ([]store.LeaseMiss, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	groups := make(map[int32][]store.LeaseTouch)
	for _, it := range items {
		groups[it.ServerID] = append(groups[it.ServerID], it)
	}
	var misses []store.LeaseMiss
	for serverID, batch := range groups {
		part, err := s.renewServer(ctx, serverID, batch)
		if err != nil {
			if ctx.Err() != nil {
				return nil, err
			}
			for _, it := range batch {
				misses = append(misses, store.LeaseMiss{ServerID: it.ServerID, Key: it.Key, Err: err})
			}
			continue
		}
		misses = append(misses, part...)
	}
	if len(misses) == 0 {
		return nil, nil
	}
	return misses, nil
}

func (s *Store) renewServer(ctx context.Context, serverID int32, batch []store.LeaseTouch) ([]store.LeaseMiss, error) {
	var misses []store.LeaseMiss
	send := make([]store.LeaseTouch, 0, len(batch))
	for _, it := range batch {
		if it.Key == "" {
			misses = append(misses, store.LeaseMiss{ServerID: it.ServerID, Key: it.Key, Err: store.ErrLeaseHeld})
			continue
		}
		send = append(send, it)
	}
	if len(send) == 0 {
		return misses, nil
	}
	models := make([]drv.WriteModel, len(send))
	ids := make([]string, len(send))
	for i, it := range send {
		filter, update := leaseRenewSpec(it)
		models[i] = drv.NewUpdateOneModel().SetFilter(filter).SetUpdate(update)
		ids[i] = it.Key
	}
	coll := s.coll(serverID, "lease")
	res, err := coll.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		failed, pending, ok := classifyBulkWriteError(err, res, send)
		if !ok {
			return nil, err
		}
		for _, item := range failed {
			misses = append(misses, store.LeaseMiss{ServerID: item.item.ServerID, Key: item.item.Key, Err: item.err})
		}
		if len(pending) == 0 {
			return misses, nil
		}
		pendingIDs := make([]string, len(pending))
		for i, it := range pending {
			pendingIDs[i] = it.Key
		}
		got, qerr := loadRenewed(ctx, coll, pendingIDs)
		if qerr != nil {
			return nil, err
		}
		misses = append(misses, renewMisses(pending, got)...)
		return misses, nil
	}
	if res != nil && res.MatchedCount == int64(len(send)) {
		return misses, nil
	}
	// BulkWrite 只给出匹配条数。部分没对上时再读一次，才能指出是哪几条没有续上。
	got, err := loadRenewed(ctx, coll, ids)
	if err != nil {
		return nil, err
	}
	misses = append(misses, renewMisses(send, got)...)
	return misses, nil
}

type bulkWriteFailure struct {
	item store.LeaseTouch
	err  error
}

// classifyBulkWriteError 保留明确写失败，并只对没有明确结果的条目做状态核验。
func classifyBulkWriteError(err error, res *drv.BulkWriteResult, items []store.LeaseTouch) ([]bulkWriteFailure, []store.LeaseTouch, bool) {
	var bwe drv.BulkWriteException
	if !errors.As(err, &bwe) || res == nil || bwe.WriteConcernError != nil || len(bwe.WriteErrors) == 0 {
		return nil, nil, false
	}
	failed := make([]bulkWriteFailure, 0, len(bwe.WriteErrors))
	seen := make(map[int]struct{}, len(bwe.WriteErrors))
	for _, bulkErr := range bwe.WriteErrors {
		if bulkErr.Index < 0 || bulkErr.Index >= len(items) {
			return nil, nil, false
		}
		if _, ok := seen[bulkErr.Index]; ok {
			continue
		}
		seen[bulkErr.Index] = struct{}{}
		failed = append(failed, bulkWriteFailure{item: items[bulkErr.Index], err: bulkErr})
	}
	pending := make([]store.LeaseTouch, 0, len(items)-len(failed))
	for i, item := range items {
		if _, ok := seen[i]; !ok {
			pending = append(pending, item)
		}
	}
	return failed, pending, true
}

// leaseRenewSpec 是单条续期的过滤和更新。过滤必须带上持有者和任期，更新不能改这两项。
func leaseRenewSpec(it store.LeaseTouch) (filter, update bson.D) {
	filter = bson.D{
		{Key: "_id", Value: it.Key},
		{Key: "owner", Value: it.Owner},
		{Key: "term", Value: it.Term},
	}
	update = bson.D{{Key: "$set", Value: bson.D{{Key: "until", Value: it.Until}}}}
	return filter, update
}

func renewMisses(items []store.LeaseTouch, got map[string]leaseDoc) []store.LeaseMiss {
	var misses []store.LeaseMiss
	for _, it := range items {
		doc, ok := got[it.Key]
		if !ok || doc.Owner != it.Owner {
			owner := ""
			if ok {
				owner = doc.Owner
			}
			misses = append(misses, store.LeaseMiss{ServerID: it.ServerID, Key: it.Key, Owner: owner, Err: store.ErrLeaseHeld})
			continue
		}
		// 持有者还是自己但任期不同：不能写成被别人持有，上层会据此踢掉仍在线的会话。
		if it.Term == 0 || doc.Term != it.Term {
			misses = append(misses, store.LeaseMiss{ServerID: it.ServerID, Key: it.Key, Owner: doc.Owner, Err: store.ErrLeaseTerm})
		}
	}
	return misses
}

func loadRenewed(ctx context.Context, coll *drv.Collection, ids []string) (map[string]leaseDoc, error) {
	cur, err := coll.Find(ctx, bson.M{"_id": bson.M{"$in": ids}}, options.Find().SetProjection(bson.M{
		"owner": 1, "term": 1, "until": 1,
	}))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	got := make(map[string]leaseDoc, len(ids))
	for cur.Next(ctx) {
		var doc leaseDoc
		if err := cur.Decode(&doc); err != nil {
			return nil, err
		}
		got[doc.ID] = doc
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}
	return got, nil
}

func loadLease(ctx context.Context, coll *drv.Collection, key string) (store.LeaseRec, bool, error) {
	var doc leaseDoc
	err := coll.FindOne(ctx, bson.M{"_id": key}).Decode(&doc)
	if errors.Is(err, drv.ErrNoDocuments) {
		return store.LeaseRec{}, false, nil
	}
	if err != nil {
		return store.LeaseRec{}, false, err
	}
	return store.LeaseRec{Owner: doc.Owner, Term: doc.Term, Until: doc.Until, Gate: doc.Gate, Session: doc.Session, Found: true}, true, nil
}
