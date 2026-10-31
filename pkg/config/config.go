package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"tuserv/pkg/log"
)

// File 是进程启动配置。TCP 和 WebSocket 地址留空表示不监听该入口。
type File struct {
	ServiceName   string `yaml:"service_name"`
	Weight        int    `yaml:"weight"`
	ListenAddr    string `yaml:"listen_addr"`
	AdvertiseAddr string `yaml:"advertise_addr"`
	TCPAddr       string `yaml:"tcp_addr"`
	WSAddr        string `yaml:"ws_addr"`
	Etcd          struct {
		Endpoints []string `yaml:"endpoints"`
	} `yaml:"etcd"`
	Mongo struct {
		URI      string `yaml:"uri"`
		DBPrefix string `yaml:"db_prefix"`
	} `yaml:"mongo"`
	FlushInterval string `yaml:"flush_interval"`
	// Cache 控制逻辑节点内存里的实体数量。两项都留空或为 0 时不淘汰。
	Cache struct {
		MaxItems int    `yaml:"max_items"`
		Idle     string `yaml:"idle"`
	} `yaml:"cache"`
	Log Log `yaml:"log"`
}

// Log 对应 pkg/log 的配置，单独列出是为了 yaml 名字和代码字段对齐。
type Log struct {
	Level      string `yaml:"level"`
	Format     string `yaml:"format"`
	Dir        string `yaml:"dir"`
	Name       string `yaml:"name"`
	MaxSizeMB  int    `yaml:"max_size_mb"`
	MaxAgeDays int    `yaml:"max_age_days"`
	AlsoStdout bool   `yaml:"also_stdout"`
}

// Load 从路径读取配置。文件不存在时直接失败，避免用空地址启动后才发现连不上。
func Load(path string) (File, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return File{}, err
	}
	var f File
	if err := yaml.Unmarshal(b, &f); err != nil {
		return File{}, err
	}
	return f, nil
}

// LogConfig 转成日志包使用的结构。
func (f File) LogConfig() log.Config {
	return log.Config{
		Level:      f.Log.Level,
		Format:     f.Log.Format,
		Dir:        f.Log.Dir,
		Name:       f.Log.Name,
		MaxSizeMB:  f.Log.MaxSizeMB,
		MaxAgeDays: f.Log.MaxAgeDays,
		AlsoStdout: f.Log.AlsoStdout,
	}
}

// CacheMaxItems 是内存里不同实体的上限。负数或 0 表示不按容量淘汰。
func (f File) CacheMaxItems() int {
	if f.Cache.MaxItems < 0 {
		return 0
	}
	return f.Cache.MaxItems
}

// CacheIdle 是实体空闲多久后可以离开内存。空字符串、非法值或负数表示不按空闲淘汰。
func (f File) CacheIdle() time.Duration {
	if f.Cache.Idle == "" {
		return 0
	}
	d, err := time.ParseDuration(f.Cache.Idle)
	if err != nil || d < 0 {
		return 0
	}
	return d
}

// FlushEvery 解析刷盘间隔。空字符串按 1 秒，避免逻辑节点从不落库。
func (f File) FlushEvery() time.Duration {
	if f.FlushInterval == "" {
		return time.Second
	}
	d, err := time.ParseDuration(f.FlushInterval)
	if err != nil || d <= 0 {
		return time.Second
	}
	return d
}
