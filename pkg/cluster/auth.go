package cluster

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"time"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/server"

	"tuserv/pkg/log"
)

const (
	// authSkew 允许机器时钟偏差。窗口再大，抓到的包就能重放更久。
	authSkew = 5 * time.Minute
	metaTs   = "tuserv-ts"
	metaFlag = "tuserv-internal"
	metaSig  = "tuserv-sig"
)

var (
	// ErrMissingSecret 表示没有配置节点间共享密钥。
	ErrMissingSecret = errors.New("cluster: missing shared secret")
	// ErrUnauthenticated 表示节点间调用的签名缺失、过期或被改过。
	ErrUnauthenticated = errors.New("cluster: unauthenticated")
)

type callKindKey struct{}

type callKind struct {
	internal bool
}

type tokenKey struct{}

type tokenFields struct {
	ts   string
	flag string
	sig  string
}

// WithInternalCall 标记这一跳是不是内部命令。
// 服务端只在验过签名之后写入；调用方上下文里的同名标记过不了网络。
func WithInternalCall(ctx context.Context, internal bool) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, callKindKey{}, callKind{internal: internal})
}

// IsInternalCall 报告当前上下文是否已确认是内部命令。
func IsInternalCall(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	kind, _ := ctx.Value(callKindKey{}).(callKind)
	return kind.internal
}

// authClientOpts 给节点客户端带上签名。没装这组选项的调用会被对端拒绝。
func authClientOpts(secret string) []client.Option {
	return []client.Option{
		client.WithMetaHandler(tokenMeta{}),
		client.WithMiddleware(clientAuth(secret, time.Now)),
	}
}

// authServerOpts 在业务处理之前验签。密钥错误或缺失时请求进不了 Dispatch、Push 或 Ping。
func authServerOpts(secret string) []server.Option {
	return []server.Option{
		server.WithMetaHandler(tokenMeta{}),
		server.WithMiddleware(serverAuth(secret, time.Now)),
	}
}

func clientAuth(secret string, now func() time.Time) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req, resp interface{}) error {
			method, ok := rpcMethod(ctx)
			if !ok {
				return ErrUnauthenticated
			}
			flag := "0"
			if IsInternalCall(ctx) {
				flag = "1"
			}
			ts := strconv.FormatInt(now().Unix(), 10)
			ctx = context.WithValue(ctx, tokenKey{}, tokenFields{
				ts:   ts,
				flag: flag,
				sig:  sign(secret, ts, flag, method),
			})
			return next(ctx, req, resp)
		}
	}
}

func serverAuth(secret string, now func() time.Time) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req, resp interface{}) error {
			method, ok := rpcMethod(ctx)
			fields, has := ctx.Value(tokenKey{}).(tokenFields)
			if !ok || !has {
				log.Warn("cluster auth", "err", ErrUnauthenticated)
				return ErrUnauthenticated
			}
			if err := verify(secret, fields.ts, fields.flag, method, fields.sig, now()); err != nil {
				log.Warn("cluster auth", "err", err, "method", method)
				return err
			}
			return next(WithInternalCall(ctx, fields.flag == "1"), req, resp)
		}
	}
}

func rpcMethod(ctx context.Context) (string, bool) {
	ri := rpcinfo.GetRPCInfo(ctx)
	if ri == nil || ri.To() == nil {
		return "", false
	}
	method := ri.To().Method()
	if method == "" {
		return "", false
	}
	return method, true
}

func sign(secret, ts, flag, method string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(ts))
	_, _ = mac.Write([]byte{'\n'})
	_, _ = mac.Write([]byte(flag))
	_, _ = mac.Write([]byte{'\n'})
	_, _ = mac.Write([]byte(method))
	return hex.EncodeToString(mac.Sum(nil))
}

func verify(secret, ts, flag, method, sig string, now time.Time) error {
	if secret == "" || ts == "" || (flag != "0" && flag != "1") || method == "" || sig == "" {
		return ErrUnauthenticated
	}
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return ErrUnauthenticated
	}
	delta := now.Sub(time.Unix(sec, 0))
	if delta < -authSkew || delta > authSkew {
		return ErrUnauthenticated
	}
	got, err := hex.DecodeString(sig)
	if err != nil {
		return ErrUnauthenticated
	}
	want, err := hex.DecodeString(sign(secret, ts, flag, method))
	if err != nil || !hmac.Equal(got, want) {
		return ErrUnauthenticated
	}
	return nil
}

// tokenMeta 把签名放进传输头。不放进业务 protobuf，是为了不改已生成的 RPC 结构。
type tokenMeta struct{}

func (tokenMeta) WriteMeta(ctx context.Context, msg remote.Message) (context.Context, error) {
	if msg == nil || msg.MessageType() != remote.Call {
		return ctx, nil
	}
	fields, ok := ctx.Value(tokenKey{}).(tokenFields)
	if !ok || msg.TransInfo() == nil {
		return ctx, nil
	}
	src := msg.TransInfo().TransStrInfo()
	dst := make(map[string]string, len(src)+3)
	for k, v := range src {
		dst[k] = v
	}
	dst[metaTs] = fields.ts
	dst[metaFlag] = fields.flag
	dst[metaSig] = fields.sig
	msg.TransInfo().PutTransStrInfo(dst)
	return ctx, nil
}

func (tokenMeta) ReadMeta(ctx context.Context, msg remote.Message) (context.Context, error) {
	if msg == nil || msg.TransInfo() == nil {
		return ctx, nil
	}
	src := msg.TransInfo().TransStrInfo()
	fields := tokenFields{ts: src[metaTs], flag: src[metaFlag], sig: src[metaSig]}
	if fields.ts == "" && fields.flag == "" && fields.sig == "" {
		return ctx, nil
	}
	return context.WithValue(ctx, tokenKey{}, fields), nil
}
