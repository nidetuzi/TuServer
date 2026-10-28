package log

import (
	"context"
	"fmt"
	"io"

	"github.com/cloudwego/kitex/pkg/klog"
)

type kitexLogger struct{}

// BindKitex 让 Kitex 的内部日志进入同一条滚动文件。
// 否则 RPC 失败和业务日志会分成两套输出，排障时对不齐。
func BindKitex() {
	klog.SetLogger(kitexLogger{})
}

func (kitexLogger) Trace(v ...interface{})  { Debug(fmt.Sprint(v...)) }
func (kitexLogger) Debug(v ...interface{})  { Debug(fmt.Sprint(v...)) }
func (kitexLogger) Info(v ...interface{})   { Info(fmt.Sprint(v...)) }
func (kitexLogger) Notice(v ...interface{}) { Info(fmt.Sprint(v...)) }
func (kitexLogger) Warn(v ...interface{})   { Warn(fmt.Sprint(v...)) }
func (kitexLogger) Error(v ...interface{})  { Error(fmt.Sprint(v...)) }
func (kitexLogger) Fatal(v ...interface{})  { Error(fmt.Sprint(v...)) }

func (kitexLogger) Tracef(format string, v ...interface{})  { Debug(fmt.Sprintf(format, v...)) }
func (kitexLogger) Debugf(format string, v ...interface{})  { Debug(fmt.Sprintf(format, v...)) }
func (kitexLogger) Infof(format string, v ...interface{})   { Info(fmt.Sprintf(format, v...)) }
func (kitexLogger) Noticef(format string, v ...interface{}) { Info(fmt.Sprintf(format, v...)) }
func (kitexLogger) Warnf(format string, v ...interface{})   { Warn(fmt.Sprintf(format, v...)) }
func (kitexLogger) Errorf(format string, v ...interface{})  { Error(fmt.Sprintf(format, v...)) }
func (kitexLogger) Fatalf(format string, v ...interface{})  { Error(fmt.Sprintf(format, v...)) }

func (k kitexLogger) CtxTracef(ctx context.Context, format string, v ...interface{}) {
	k.Tracef(format, v...)
}
func (k kitexLogger) CtxDebugf(ctx context.Context, format string, v ...interface{}) {
	k.Debugf(format, v...)
}
func (k kitexLogger) CtxInfof(ctx context.Context, format string, v ...interface{}) {
	k.Infof(format, v...)
}
func (k kitexLogger) CtxNoticef(ctx context.Context, format string, v ...interface{}) {
	k.Noticef(format, v...)
}
func (k kitexLogger) CtxWarnf(ctx context.Context, format string, v ...interface{}) {
	k.Warnf(format, v...)
}
func (k kitexLogger) CtxErrorf(ctx context.Context, format string, v ...interface{}) {
	k.Errorf(format, v...)
}
func (k kitexLogger) CtxFatalf(ctx context.Context, format string, v ...interface{}) {
	k.Fatalf(format, v...)
}

// SetLevel 不改变 pkg/log 的级别。级别由进程配置决定，避免 Kitex 启动时把它改掉。
func (kitexLogger) SetLevel(klog.Level) {}

// SetOutput 不切换输出。滚动文件已经在 Init 里接好。
func (kitexLogger) SetOutput(io.Writer) {}
