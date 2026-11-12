package main

import (
	"testing"

	"tuserv/pkg/logic"
	"tuserv/pkg/owner"
)

func TestOwnerReleaseKeepsEntityKind(t *testing.T) {
	key := owner.Key(3, "account", 99)
	got := ownerRelease(key)
	want := logic.Release{Kind: "account", ServerID: 3, ID: 99}
	if got != want {
		t.Fatalf("ownerRelease=%+v want=%+v", got, want)
	}
	if got.Kind == logic.KindGuild {
		t.Fatal("不能把非公会实体交接写死为 guild")
	}
}
