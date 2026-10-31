package config

import (
	"testing"
	"time"
)

func TestSchedulerConfig(t *testing.T) {
	var f File
	cfg, err := f.SchedulerConfig()
	if err != nil || cfg.Timeout != 0 || cfg.MaxConcurrent != 0 {
		t.Fatalf("默认配置应交给调度器: %+v %v", cfg, err)
	}
	f.Scheduler = Scheduler{Timezone: "Asia/Shanghai", MaxConcurrent: 8, TaskTimeout: "5s"}
	cfg, err = f.SchedulerConfig()
	if err != nil || cfg.Timeout != 5*time.Second || cfg.MaxConcurrent != 8 || cfg.Timezone != "Asia/Shanghai" {
		t.Fatalf("配置未正确转换: %+v %v", cfg, err)
	}
	for _, timeout := range []string{"bad", "0s", "-1s"} {
		f.Scheduler.TaskTimeout = timeout
		if _, err := f.SchedulerConfig(); err == nil {
			t.Fatalf("非法超时被接受: %s", timeout)
		}
	}
	f.Scheduler.TaskTimeout = ""
	f.Scheduler.MaxConcurrent = -1
	if _, err := f.SchedulerConfig(); err == nil {
		t.Fatal("非法并发被接受")
	}
}
