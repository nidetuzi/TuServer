package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestClusterKeyEnvOverridesFile(t *testing.T) {
	t.Setenv("TUSERV_CLUSTER_SECRET", "from-env")
	got, err := (File{ClusterSecret: "from-file"}).ClusterKey()
	if err != nil || got != "from-env" {
		t.Fatalf("环境变量应覆盖文件: got=%q err=%v", got, err)
	}
}

func TestClusterKeyFromFileAndMissing(t *testing.T) {
	t.Setenv("TUSERV_CLUSTER_SECRET", "")
	if _, err := (File{}).ClusterKey(); err == nil {
		t.Fatal("密钥为空时应失败")
	}
	got, err := (File{ClusterSecret: "  yaml-secret  "}).ClusterKey()
	if err != nil || got != "yaml-secret" {
		t.Fatalf("应使用文件中的密钥: got=%q err=%v", got, err)
	}
}

func TestLoadClusterSecret(t *testing.T) {
	t.Setenv("TUSERV_CLUSTER_SECRET", "")
	dir := t.TempDir()
	path := filepath.Join(dir, "c.yaml")
	if err := os.WriteFile(path, []byte("cluster_secret: abc\nlisten_addr: 127.0.0.1:1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := f.ClusterKey()
	if err != nil || got != "abc" || f.ListenAddr != "127.0.0.1:1" {
		t.Fatalf("yaml 密钥未读出: file=%+v got=%q err=%v", f, got, err)
	}
}

func TestRenewEveryAndReapParallel(t *testing.T) {
	var empty File
	if empty.RenewEvery() != 5*time.Second || empty.ReapParallel() != 8 {
		t.Fatalf("缺省续租或清理并发不对: renew=%s parallel=%d", empty.RenewEvery(), empty.ReapParallel())
	}
	bad := File{RenewInterval: "1m", GateReapParallel: 0}
	if bad.RenewEvery() != 5*time.Second || bad.ReapParallel() != 8 {
		t.Fatal("不小于 TTL 的续租间隔和非法并发应退回默认")
	}
	ok := File{RenewInterval: "4s", GateReapParallel: 3}
	if ok.RenewEvery() != 4*time.Second || ok.ReapParallel() != 3 {
		t.Fatalf("合法配置应保留: renew=%s parallel=%d", ok.RenewEvery(), ok.ReapParallel())
	}
}

func TestClientIdleAndGateProbe(t *testing.T) {
	var empty File
	if empty.ClientIdle() != 60*time.Second || empty.GateProbeEvery() != 5*time.Second || empty.GateMissLimit() != 2 {
		t.Fatalf("缺省值不对: idle=%s probe=%s miss=%d", empty.ClientIdle(), empty.GateProbeEvery(), empty.GateMissLimit())
	}
	off := File{IdleTimeout: "0s", GateProbeInterval: "0s", GateMisses: 0}
	if off.ClientIdle() != 0 {
		t.Fatal("显式 0 应关闭读空闲")
	}
	if off.GateProbeEvery() != 5*time.Second || off.GateMissLimit() != 2 {
		t.Fatal("非法探活配置应退回默认")
	}
	bad := File{IdleTimeout: "soon"}
	if bad.ClientIdle() != 60*time.Second {
		t.Fatal("非法空闲时间应退回默认")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "gate.yaml")
	if err := os.WriteFile(path, []byte("idle_timeout: 12s\ngate_probe_interval: 3s\ngate_misses: 4\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if f.ClientIdle() != 12*time.Second || f.GateProbeEvery() != 3*time.Second || f.GateMissLimit() != 4 {
		t.Fatalf("yaml 没有读到: idle=%s probe=%s miss=%d", f.ClientIdle(), f.GateProbeEvery(), f.GateMissLimit())
	}
}
