package log

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRotateByDaySizeAndAge(t *testing.T) {
	dir := t.TempDir()
	day := time.Date(2026, 10, 28, 12, 0, 0, 0, time.Local)
	cfg := Config{
		Level:      "info",
		Format:     "text",
		Dir:        dir,
		Name:       "gate",
		MaxAgeDays: 7,
		AlsoStdout: false,
		now:        func() time.Time { return day },
		maxBytes:   80,
	}
	if err := Init(cfg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	Info("session bind", "server_id", 1, "logic", "127.0.0.1:9101")
	if err := Sync(); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(dir, "gate-2026-10-28.log"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	if !strings.Contains(text, "server_id=1") || !strings.Contains(text, "logic=127.0.0.1:9101") {
		t.Fatalf("日志缺少 With 字段: %s", text)
	}

	for i := 0; i < 20; i++ {
		Info("pad", "n", i, "payload", strings.Repeat("x", 40))
	}
	if err := Sync(); err != nil {
		t.Fatal(err)
	}
	names, err := writer.files()
	if err != nil {
		t.Fatal(err)
	}
	if !hasSuffix(names, "gate-2026-10-28.1.log") {
		t.Fatalf("超过大小后应出现序号文件: %v", names)
	}

	day = day.AddDate(0, 0, 1)
	Info("next day")
	if err := Sync(); err != nil {
		t.Fatal(err)
	}
	names, err = writer.files()
	if err != nil {
		t.Fatal(err)
	}
	if !hasSuffix(names, "gate-2026-10-29.log") {
		t.Fatalf("跨天应换新文件: %v", names)
	}

	old := time.Date(2026, 10, 1, 0, 0, 0, 0, time.Local)
	if err := os.WriteFile(filepath.Join(dir, "gate-2026-10-01.log"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	day = old.AddDate(0, 0, 30)
	Info("cleanup")
	if err := Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "gate-2026-10-01.log")); !os.IsNotExist(err) {
		t.Fatal("超龄文件应被删除")
	}
}

func hasSuffix(names []string, want string) bool {
	for _, name := range names {
		if name == want {
			return true
		}
	}
	return false
}
