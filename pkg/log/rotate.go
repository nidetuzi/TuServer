package log

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// rotator 按天再按大小切开日志。
// 游戏服会长时间运行，单个文件不切开的话，过几天就无法打开和清理。
type rotator struct {
	dir      string
	name     string
	maxBytes int64
	maxAge   time.Duration
	now      func() time.Time

	mu   sync.Mutex
	file *os.File
	day  string
	size int64
}

func newRotator(cfg Config) (*rotator, error) {
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	maxBytes := cfg.maxBytes
	if maxBytes <= 0 {
		maxBytes = int64(cfg.MaxSizeMB) * 1024 * 1024
	}
	now := cfg.now
	if now == nil {
		now = time.Now
	}
	r := &rotator{
		dir:      cfg.Dir,
		name:     cfg.Name,
		maxBytes: maxBytes,
		maxAge:   time.Duration(cfg.MaxAgeDays) * 24 * time.Hour,
		now:      now,
	}
	if err := r.rotate(r.now(), false); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *rotator) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	day := now.Format("2006-01-02")
	// 按即将写入后的大小判断，避免一条日志跨过上限后仍留在主文件里。
	force := day == r.day && r.maxBytes > 0 && r.size > 0 && r.size+int64(len(p)) > r.maxBytes
	if r.file == nil || day != r.day || force {
		if err := r.rotate(now, force); err != nil {
			return 0, err
		}
	}
	n, err := r.file.Write(p)
	r.size += int64(n)
	if err == nil {
		r.cleanup(now)
	}
	return n, err
}

func (r *rotator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	return err
}

// rotate 保证正在写的文件始终是当天的主文件。
// 超过大小时把主文件让给序号文件，这样 tail 当天文件就能看到最新日志。
func (r *rotator) rotate(now time.Time, force bool) error {
	day := now.Format("2006-01-02")
	if r.file != nil && day == r.day && force {
		if err := r.file.Close(); err != nil {
			return err
		}
		r.file = nil
		if err := r.shift(day); err != nil {
			return err
		}
	} else if r.file != nil && day != r.day {
		if err := r.file.Close(); err != nil {
			return err
		}
		r.file = nil
	}
	path := r.activePath(day)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	r.file = f
	r.day = day
	r.size = info.Size()
	return nil
}

func (r *rotator) activePath(day string) string {
	return filepath.Join(r.dir, fmt.Sprintf("%s-%s.log", r.name, day))
}

func (r *rotator) shift(day string) error {
	active := r.activePath(day)
	next, err := r.nextIndex(day)
	if err != nil {
		return err
	}
	dst := filepath.Join(r.dir, fmt.Sprintf("%s-%s.%d.log", r.name, day, next))
	return os.Rename(active, dst)
}

func (r *rotator) nextIndex(day string) (int, error) {
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return 0, err
	}
	max := 0
	prefix := r.name + "-" + day + "."
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".log") {
			continue
		}
		mid := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".log")
		n, err := strconv.Atoi(mid)
		if err != nil {
			continue
		}
		if n > max {
			max = n
		}
	}
	return max + 1, nil
}

var datedFile = regexp.MustCompile(`^.+-(\d{4}-\d{2}-\d{2})(?:\.\d+)?\.log$`)

func (r *rotator) cleanup(now time.Time) {
	if r.maxAge <= 0 {
		return
	}
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return
	}
	cutoff := now.Add(-r.maxAge)
	prefix := r.name + "-"
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		m := datedFile.FindStringSubmatch(name)
		if m == nil {
			continue
		}
		day, err := time.ParseInLocation("2006-01-02", m[1], now.Location())
		if err != nil {
			continue
		}
		if day.Before(cutoff) {
			_ = os.Remove(filepath.Join(r.dir, name))
		}
	}
}

// files 按名字排序，测试用它确认切分结果，不依赖目录遍历顺序。
func (r *rotator) files() ([]string, error) {
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), r.name+"-") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}
