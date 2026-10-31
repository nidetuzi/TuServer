package config

import (
	"os"
	"path/filepath"
	"testing"
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
