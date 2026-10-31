package config

import (
	"fmt"
	"time"

	"tuserv/pkg/scheduler"
)

// Scheduler 将业务调度的资源限制集中在节点配置中。
type Scheduler struct {
	Timezone      string `yaml:"timezone"`
	MaxConcurrent int    `yaml:"max_concurrent"`
	TaskTimeout   string `yaml:"task_timeout"`
}

// SchedulerConfig 对非法配置直接报错，避免任务在意外时区或无限超时下运行。
func (f File) SchedulerConfig() (scheduler.Config, error) {
	cfg := scheduler.Config{Timezone: f.Scheduler.Timezone, MaxConcurrent: f.Scheduler.MaxConcurrent}
	if f.Scheduler.TaskTimeout != "" {
		d, err := time.ParseDuration(f.Scheduler.TaskTimeout)
		if err != nil || d <= 0 {
			return cfg, fmt.Errorf("config: invalid scheduler task_timeout %q", f.Scheduler.TaskTimeout)
		}
		cfg.Timeout = d
	}
	if cfg.MaxConcurrent < 0 {
		return cfg, fmt.Errorf("config: invalid scheduler max_concurrent %d", cfg.MaxConcurrent)
	}
	return cfg, nil
}
