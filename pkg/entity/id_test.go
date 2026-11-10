package entity

import (
	"context"
	"testing"

	"tuserv/pkg/store/memory"
)

func TestWithServer(t *testing.T) {
	id, err := WithServer(3, 1)
	if err != nil || id != 300001 {
		t.Fatalf("区服编号不符: %d %v", id, err)
	}
	if _, err := WithServer(1, IDSpan); err != ErrIDRange {
		t.Fatal("自增号达到区服宽度时应拒绝")
	}
}

func TestNextBizID(t *testing.T) {
	st := memory.New()
	id, err := NextBizID(context.Background(), st, 2, "player")
	if err != nil || id != 200001 {
		t.Fatalf("NextBizID: %d %v", id, err)
	}
	id2, err := NextBizID(context.Background(), st, 2, "player")
	if err != nil || id2 != 200002 {
		t.Fatalf("NextBizID second: %d %v", id2, err)
	}
}
