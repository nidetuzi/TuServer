package mongo

import (
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	drv "go.mongodb.org/mongo-driver/mongo"

	"tuserv/pkg/store"
)

func TestLeaseRenewSpecKeepsOwnerAndTerm(t *testing.T) {
	filter, update := leaseRenewSpec(store.LeaseTouch{
		Key: "player/1/9", Owner: "logic-a", Term: 4, Until: 99,
	})
	raw, err := bson.Marshal(filter)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		ID    string `bson:"_id"`
		Owner string `bson:"owner"`
		Term  uint64 `bson:"term"`
	}
	if err := bson.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.ID != "player/1/9" || got.Owner != "logic-a" || got.Term != 4 {
		t.Fatalf("过滤条件应带上持有者和任期: %+v", got)
	}
	raw, err = bson.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	set := bson.Raw(raw).Lookup("$set")
	if set.Type == 0 {
		t.Fatal("更新应为 $set")
	}
	doc := set.Document()
	if doc.Lookup("until").Int64() != 99 {
		t.Fatalf("应写下一次到期时间: %v", doc.Lookup("until"))
	}
	if doc.Lookup("owner").Type != 0 || doc.Lookup("term").Type != 0 {
		t.Fatal("续期不能改持有者或任期")
	}
}

func TestClassifyBulkWriteErrorPreservesIndexedWriteErrors(t *testing.T) {
	items := []store.LeaseTouch{{Key: "first"}, {Key: "second"}, {Key: "third"}}
	writeErr := drv.BulkWriteError{WriteError: drv.WriteError{Index: 1, Code: 11000, Message: "duplicate"}}
	err := drv.BulkWriteException{WriteErrors: []drv.BulkWriteError{writeErr}}
	failed, pending, ok := classifyBulkWriteError(err, &drv.BulkWriteResult{MatchedCount: 1}, items)
	if !ok || len(failed) != 1 || failed[0].item.Key != "second" || len(pending) != 2 {
		t.Fatalf("应保留写失败并待核验其余条目: ok=%v failed=%+v pending=%+v", ok, failed, pending)
	}
	if failed[0].err.Error() != writeErr.Error() {
		t.Fatalf("应返回原始 WriteError: got=%v want=%v", failed[0].err, writeErr)
	}
}

func TestClassifyBulkWriteErrorLeavesUnmatchedItemsForLookup(t *testing.T) {
	items := []store.LeaseTouch{{Key: "first"}, {Key: "second"}, {Key: "third"}}
	err := drv.BulkWriteException{
		WriteErrors: []drv.BulkWriteError{{WriteError: drv.WriteError{Index: 2, Message: "write failed"}}},
	}
	failed, pending, ok := classifyBulkWriteError(err, &drv.BulkWriteResult{MatchedCount: 1}, items)
	if !ok || len(failed) != 1 || len(pending) != 2 || pending[0].Key != "first" || pending[1].Key != "second" {
		t.Fatalf("未报写错误的条目必须待查询: ok=%v failed=%+v pending=%+v", ok, failed, pending)
	}
}

func TestBulkWriteFailedItemsRejectsUncertainResults(t *testing.T) {
	items := []store.LeaseTouch{{Key: "first"}, {Key: "second"}}
	cases := []struct {
		name string
		err  error
		res  *drv.BulkWriteResult
	}{
		{name: "network", err: errors.New("network"), res: &drv.BulkWriteResult{MatchedCount: 1}},
		{name: "nil result", err: drv.BulkWriteException{WriteErrors: []drv.BulkWriteError{{WriteError: drv.WriteError{Index: 1}}}}},
		{name: "write concern", err: drv.BulkWriteException{
			WriteConcernError: &drv.WriteConcernError{Message: "timeout"},
			WriteErrors:       []drv.BulkWriteError{{WriteError: drv.WriteError{Index: 1}}},
		}, res: &drv.BulkWriteResult{MatchedCount: 1}},
		{name: "invalid index", err: drv.BulkWriteException{WriteErrors: []drv.BulkWriteError{{WriteError: drv.WriteError{Index: 2}}}}, res: &drv.BulkWriteResult{MatchedCount: 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			failed, pending, ok := classifyBulkWriteError(tc.err, tc.res, items)
			if ok || failed != nil || pending != nil {
				t.Fatalf("不确定结果不能分类: ok=%v failed=%+v pending=%+v", ok, failed, pending)
			}
		})
	}
}

func TestRenewMissesSplitsHitAndMiss(t *testing.T) {
	items := []store.LeaseTouch{
		{ServerID: 1, Key: "ok", Owner: "node-a", Term: 3, Until: 50},
		{ServerID: 1, Key: "same", Owner: "node-a", Term: 3, Until: 99},
		{ServerID: 1, Key: "stale", Owner: "node-a", Term: 3, Until: 50},
		{ServerID: 1, Key: "term", Owner: "node-a", Term: 3, Until: 50},
		{ServerID: 1, Key: "gone", Owner: "node-a", Term: 3, Until: 50},
	}
	got := map[string]leaseDoc{
		"ok":    {ID: "ok", Owner: "node-a", Term: 3, Until: 50},
		"same":  {ID: "same", Owner: "node-a", Term: 3, Until: 10},
		"stale": {ID: "stale", Owner: "node-b", Term: 4, Until: 10},
		"term":  {ID: "term", Owner: "node-a", Term: 8, Until: 10},
	}
	misses := renewMisses(items, got)
	if len(misses) != 3 {
		t.Fatalf("应标出三条: %+v", misses)
	}
	if misses[0].Key != "stale" || misses[0].Err != store.ErrLeaseHeld || misses[0].Owner != "node-b" {
		t.Fatalf("别人持有应分开: %+v", misses[0])
	}
	if misses[1].Key != "term" || misses[1].Err != store.ErrLeaseTerm || misses[1].Owner != "node-a" {
		t.Fatalf("任期变化不能当成被别人持有: %+v", misses[1])
	}
	if misses[2].Key != "gone" || misses[2].Err != store.ErrLeaseHeld || misses[2].Owner != "" {
		t.Fatalf("不存在应是被持有且没有 owner: %+v", misses[2])
	}
}
