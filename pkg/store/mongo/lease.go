package mongo

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/bson"
	drv "go.mongodb.org/mongo-driver/mongo"

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
