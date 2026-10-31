package config

import (
	"testing"
	"time"
)

func TestCachePolicyParse(t *testing.T) {
	var f File
	f.Cache.MaxItems = -3
	f.Cache.Idle = "bogus"
	if f.CacheMaxItems() != 0 || f.CacheIdle() != 0 {
		t.Fatalf("非法配置应关闭淘汰: items=%d idle=%s", f.CacheMaxItems(), f.CacheIdle())
	}
	f.Cache.Idle = "-1s"
	if f.CacheIdle() != 0 {
		t.Fatal("负数空闲时间应关闭")
	}
	f.Cache.MaxItems = 5
	f.Cache.Idle = "3s"
	if f.CacheMaxItems() != 5 || f.CacheIdle() != 3*time.Second {
		t.Fatalf("解析结果不符: items=%d idle=%s", f.CacheMaxItems(), f.CacheIdle())
	}
	got, err := Load("../../config/logic.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if got.CacheMaxItems() != 20000 || got.CacheIdle() != 10*time.Minute {
		t.Fatalf("logic.yaml 默认淘汰配置不符: items=%d idle=%s", got.CacheMaxItems(), got.CacheIdle())
	}
}
