package config

import (
	"errors"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"tuserv/pkg/lease"
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
	// ClusterSecret 是节点间 Kitex 的共享密钥。环境变量 TUSERV_CLUSTER_SECRET 优先，避免生产密钥进仓库。
	ClusterSecret string `yaml:"cluster_secret"`
	Etcd          struct {
		Endpoints []string `yaml:"endpoints"`
	} `yaml:"etcd"`
	Mongo struct {
		URI      string `yaml:"uri"`
		DBPrefix string `yaml:"db_prefix"`
	} `yaml:"mongo"`
	FlushInterval string `yaml:"flush_interval"`
	// RenewInterval 是在线角色续租间隔，应小于租约 TTL，并且不跟刷盘共用一个循环。
	RenewInterval string `yaml:"renew_interval"`
	IdleTimeout   string `yaml:"idle_timeout"`
	// GateTimeout 是 Gate 到 Logic 的单次 RPC 超时时间。
	GateTimeout string `yaml:"gate_timeout"`
	// GateProbeInterval 是对照注册中心网关列表的间隔。
	GateProbeInterval string `yaml:"gate_probe_interval"`
	GateMisses        int    `yaml:"gate_misses"`
	// GateReapParallel 是一台失联网关同时清理的会话数。
	GateReapParallel int `yaml:"gate_reap_parallel"`
	// Cache 控制逻辑节点内存里的实体数量。两项都留空或为 0 时不淘汰。
	Cache struct {
		MaxItems int    `yaml:"max_items"`
		Idle     string `yaml:"idle"`
	} `yaml:"cache"`
	// Scheduler 单独配置业务调度，不改变续租和刷盘的基础循环。
	Scheduler Scheduler `yaml:"scheduler"`
	Log       Log       `yaml:"log"`
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

// ClusterKey 返回节点间共享密钥。环境变量优先于 yaml。
// 两者都空时返回错误，调用方应拒绝启动，而不是带着空密钥对外监听。
func (f File) ClusterKey() (string, error) {
	if v := strings.TrimSpace(os.Getenv("TUSERV_CLUSTER_SECRET")); v != "" {
		return v, nil
	}
	if v := strings.TrimSpace(f.ClusterSecret); v != "" {
		return v, nil
	}
	return "", errors.New("config: 缺少集群密钥，请设置 TUSERV_CLUSTER_SECRET 或 cluster_secret")
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

// RenewEvery 解析续租间隔。空字符串、非法值、非正数或不小于租约 TTL 时约为 TTL 的三分之一。
// 间隔一旦不小于 TTL，别的节点就能在下一次续租前合法接手。
func (f File) RenewEvery() time.Duration {
	if f.RenewInterval == "" {
		return lease.TTL / 3
	}
	d, err := time.ParseDuration(f.RenewInterval)
	if err != nil || d <= 0 || d >= lease.TTL {
		return lease.TTL / 3
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

// ClientIdle 是客户端读空闲上限。留空按 60 秒，让没配心跳的进程也会收掉半开连接。
// 显式写成 0 或负数表示关闭，非法字符串退回默认值，避免配错之后连接永远不超时。
func (f File) ClientIdle() time.Duration {
	if f.IdleTimeout == "" {
		return 60 * time.Second
	}
	d, err := time.ParseDuration(f.IdleTimeout)
	if err != nil {
		return 60 * time.Second
	}
	if d <= 0 {
		return 0
	}
	return d
}

// GateRPC是网关调用逻辑节点的单次超时。留空、非法或非正数按 5 秒，避免 RPC 卡住时无法发送断线通知。
func (f File) GateRPC() time.Duration {
	if f.GateTimeout == "" {
		return 5 * time.Second
	}
	d, err := time.ParseDuration(f.GateTimeout)
	if err != nil || d <= 0 {
		return 5 * time.Second
	}
	return d
}

// GateProbeEvery 是逻辑节点对照注册中心网关列表的间隔。
// 留空、非法或非正数都按 5 秒，和 etcd 注册租约同一量级，崩溃后不必等一整分钟。
func (f File) GateProbeEvery() time.Duration {
	if f.GateProbeInterval == "" {
		return 5 * time.Second
	}
	d, err := time.ParseDuration(f.GateProbeInterval)
	if err != nil || d <= 0 {
		return 5 * time.Second
	}
	return d
}

// GateMissLimit 是网关地址连续缺席多少次才清理会话。
// 小于等于 0 按 2 次，单次发现抖动不会把还活着的网关上的角色踢掉。
func (f File) GateMissLimit() int {
	if f.GateMisses <= 0 {
		return 2
	}
	return f.GateMisses
}

// ReapParallel 是清理同一台失联网关时同时处理的会话数。
// 小于等于 0 按 8。串行刷几千个会话会把探测循环占住，续租已经拆走，这里只是把清理本身缩短。
func (f File) ReapParallel() int {
	if f.GateReapParallel <= 0 {
		return 8
	}
	return f.GateReapParallel
}
