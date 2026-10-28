package entity

import "testing"

func TestWithServer(t *testing.T) {
	id, err := WithServer(3, 1)
	if err != nil || id != 300001 {
		t.Fatalf("区服编号不符: %d %v", id, err)
	}
	if _, err := WithServer(1, IDSpan); err != ErrIDRange {
		t.Fatal("自增号达到区服宽度时应拒绝")
	}
}
