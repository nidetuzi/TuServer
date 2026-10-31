package entity

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

func TestPlayerNewDay(t *testing.T) {
	loc := time.FixedZone("Asia/Shanghai", 8*60*60)
	today := time.Date(2026, 11, 1, 0, 0, 0, 0, loc)
	for _, tc := range []struct {
		name    string
		login   int64
		offline int64
		crossed bool
	}{
		{"首次登录", 0, 0, false},
		{"离线同日", 0, today.Unix(), false},
		{"离线跨日", 0, today.Add(-time.Second).Unix(), true},
		{"登录跨月", today.Add(-time.Second).Unix(), 0, true},
		{"同日重登", today.Unix(), today.Add(-time.Second).Unix(), false},
		{"时间回拨", today.AddDate(0, 0, 1).Unix(), today.Unix(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := NewPlayer(1, "ann")
			p.LastLoginAt, p.LastOfflineAt = tc.login, tc.offline
			if crossed := p.CheckNewDay(today, loc); crossed != tc.crossed {
				t.Fatalf("跨天状态错误: %v", crossed)
			}
			if p.CheckNewDay(today.Add(time.Second), loc) {
				t.Fatal("同一日期重复触发")
			}
			if got := p.Snapshot(); got.LastLoginAt != tc.login || got.LastOfflineAt != tc.offline {
				t.Fatal("在线检查改写了登录或离线时间")
			}
		})
	}
}

func TestLoginOfflineFieldsPersist(t *testing.T) {
	p := NewPlayer(1, "ann")
	p.NoteLogin(time.Unix(80, 0))
	p.NoteLogin(time.Unix(70, 0))
	p.NoteOffline(time.Unix(100, 0))
	p.NoteOffline(time.Unix(90, 0))
	p.MarkDirty()
	_, doc := p.Persist()
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	var loaded Player
	if err := bson.Unmarshal(raw, &loaded); err != nil {
		t.Fatal(err)
	}
	if got := loaded.Snapshot(); got.LastOfflineAt != 100 || got.LastLoginAt != 80 {
		t.Fatalf("时间字段未持久化: %+v", got)
	}
	var fields bson.M
	if err := bson.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"daily_day", "daily_exp", "lastdaycheck"} {
		if _, ok := fields[key]; ok {
			t.Fatalf("不应持久化字段 %s", key)
		}
	}
	loc := time.FixedZone("Asia/Shanghai", 8*60*60)
	loaded.NoteLogin(time.Date(2026, 12, 31, 23, 59, 59, 0, loc))
	if !loaded.CheckNewDay(time.Date(2027, 1, 1, 0, 0, 0, 0, loc), loc) {
		t.Fatal("跨年未触发")
	}
}
