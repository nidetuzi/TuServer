package log

import (
	"io"
	"log/slog"
	"os"
	"sync"
	"time"
)

// Config 决定日志写到哪里、保留多久。
// 三个进程各自 Init 一次，避免网关和逻辑节点的文件混在一起。
type Config struct {
	Level      string
	Format     string
	Dir        string
	Name       string
	MaxSizeMB  int
	MaxAgeDays int
	AlsoStdout bool
	now        func() time.Time
	maxBytes   int64
}

// 默认值让未写配置的进程仍能启动，而不是在打第一条日志时失败。
const (
	defaultLevel     = "info"
	defaultFormat    = "text"
	defaultDir       = "logs"
	defaultName      = "app"
	defaultMaxSizeMB = 100
	defaultMaxAge    = 7
)

var (
	mu      sync.RWMutex
	current *slog.Logger
	writer  *rotator
)

func init() {
	current = slog.New(slog.NewTextHandler(os.Stdout, nil))
}

// Init 安装进程级日志。
// 必须在监听端口之前调用，否则启动失败和接入日志会落到不同的目的地。
func Init(cfg Config) error {
	if cfg.Level == "" {
		cfg.Level = defaultLevel
	}
	if cfg.Format == "" {
		cfg.Format = defaultFormat
	}
	if cfg.Dir == "" {
		cfg.Dir = defaultDir
	}
	if cfg.Name == "" {
		cfg.Name = defaultName
	}
	if cfg.MaxSizeMB <= 0 {
		cfg.MaxSizeMB = defaultMaxSizeMB
	}
	if cfg.MaxAgeDays <= 0 {
		cfg.MaxAgeDays = defaultMaxAge
	}
	rot, err := newRotator(cfg)
	if err != nil {
		return err
	}
	var out io.Writer = rot
	if cfg.AlsoStdout {
		out = io.MultiWriter(rot, os.Stdout)
	}
	level := parseLevel(cfg.Level)
	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: level}
	if cfg.Format == "json" {
		handler = slog.NewJSONHandler(out, opts)
	} else {
		handler = slog.NewTextHandler(out, opts)
	}
	logger := slog.New(handler)
	mu.Lock()
	if writer != nil {
		_ = writer.Close()
	}
	writer = rot
	current = logger
	mu.Unlock()
	slog.SetDefault(logger)
	return nil
}

// Debug 写调试日志。线上默认级别是 info，这条不会落盘。
func Debug(msg string, args ...any) {
	logger().Debug(msg, args...)
}

// Info 写正常运行日志。成功的普通协议包不要走这里，避免把磁盘打满。
func Info(msg string, args ...any) {
	logger().Info(msg, args...)
}

// Warn 写可恢复的异常，例如一次转发失败后准备重选节点。
func Warn(msg string, args ...any) {
	logger().Warn(msg, args...)
}

// Error 写需要人看的失败，例如刷盘或注册中心失败。
func Error(msg string, args ...any) {
	logger().Error(msg, args...)
}

// With 返回带固定字段的日志器。
// 会话上下文用它带上 server_id、player_id，避免每条业务日志重复拼这些字段。
func With(args ...any) *slog.Logger {
	return logger().With(args...)
}

// Handler 把同一条输出链路交给 Kitex 的 klog。
// 节点内部日志和框架 RPC 日志必须进同一批文件，否则排障时对不齐。
func Handler() slog.Handler {
	return logger().Handler()
}

func logger() *slog.Logger {
	mu.RLock()
	defer mu.RUnlock()
	return current
}

func parseLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// SetNow 只给测试替换时钟，用来验证跨天切文件。
func SetNow(now func() time.Time) {
	mu.Lock()
	defer mu.Unlock()
	if writer != nil {
		writer.now = now
	}
}

// Close 关闭当前日志文件。测试结束时调用，避免临时目录删不掉仍被占用的文件。
func Close() error {
	mu.Lock()
	defer mu.Unlock()
	if writer == nil {
		return nil
	}
	err := writer.Close()
	writer = nil
	return err
}

// Sync 把缓冲刷到磁盘。测试在读取日志文件前调用，避免读到空内容。
func Sync() error {
	mu.RLock()
	defer mu.RUnlock()
	if writer == nil {
		return nil
	}
	return writer.file.Sync()
}
