package owner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"sync"

	"tuserv/pkg/actor"
	"tuserv/pkg/lease"
)

// EntityKey 用区服、实体种类和编号共同确定唯一主人及邮箱。
type EntityKey struct {
	// ServerID 隔离不同区服中相同编号的实体。
	ServerID int32 `json:"server_id"`
	// Kind 隔离不同业务实体的编号空间。
	Kind string `json:"kind"`
	// ID 在区服与实体种类范围内标识实体。
	ID int64 `json:"id"`
}

// Key 避免业务自行拼接可能发生冲突的实体路由键。
func Key(serverID int32, kind string, id int64) EntityKey {
	return EntityKey{ServerID: serverID, Kind: kind, ID: id}
}

// String 返回带 owner 命名空间且转义实体种类的稳定邮箱键。
func (k EntityKey) String() string {
	return "owner/" + strconv.FormatInt(int64(k.ServerID), 10) + "/" + url.PathEscape(k.Kind) + "/" + strconv.FormatInt(k.ID, 10)
}

// Resolver 查询实体的当前主人；失败时运行时不会回退到本节点。
type Resolver func(context.Context, EntityKey) (string, error)

// Transport 将编码后的请求送往指定主人，返回编码后的响应。
type Transport func(context.Context, string, []byte) ([]byte, error)

// CodeError 保留可跨节点比较的错误码，Message 仅用于诊断。
type CodeError struct {
	// Code 是跨节点保持稳定的错误身份。
	Code string `json:"code"`
	// Message 保留诊断信息，不参与错误身份比较。
	Message string `json:"message"`
}

// Error 保留错误码以便日志能够定位错误类别。
func (e *CodeError) Error() string { return e.Code + ": " + e.Message }

// Is 只比较错误码，使本地与远程业务错误具有一致的匹配语义。
func (e *CodeError) Is(target error) bool {
	other, ok := target.(*CodeError)
	return ok && e != nil && other != nil && e.Code == other.Code
}

// NewError 创建可以通过 errors.Is 按错误码匹配的业务错误。
func NewError(code, message string) error { return &CodeError{Code: code, Message: message} }

type operation func(context.Context, EntityKey, json.RawMessage) (json.RawMessage, error)

// Runtime 将操作集中到主人节点的实体邮箱，注册表允许并发读取和注册。
type Runtime struct {
	self       string
	actors     *actor.System
	resolve    Resolver
	transport  Transport
	mu         sync.RWMutex
	operations map[string]operation
	// Claim 在确认本节点仍是主人后申请任期。失败时操作不会执行。
	Claim func(context.Context, EntityKey) (uint64, error)
}

// New 创建显式配置的主人运行时；缺失依赖会在调用时返回配置错误。
func New(self string, actors *actor.System, resolve Resolver, transport Transport) *Runtime {
	return &Runtime{self: self, actors: actors, resolve: resolve, transport: transport, operations: make(map[string]operation)}
}

// NewLocal 显式选择单节点模式，不依赖注册中心或远程传输。
func NewLocal(actors *actor.System) *Runtime {
	return New("local", actors, func(context.Context, EntityKey) (string, error) { return "local", nil }, nil)
}

// Register 用类型化 DTO 包装操作；重复名称返回错误，避免静默覆盖业务处理函数。
func Register[Req, Resp any](r *Runtime, name string, fn func(context.Context, EntityKey, Req) (Resp, error)) error {
	if r == nil || name == "" || fn == nil {
		return NewError("owner.invalid", "runtime, operation and handler are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.operations[name]; exists {
		return NewError("owner.duplicate", "operation already registered: "+name)
	}
	if r.operations == nil {
		r.operations = make(map[string]operation)
	}
	r.operations[name] = func(ctx context.Context, key EntityKey, body json.RawMessage) (json.RawMessage, error) {
		var req Req
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %v", NewError("owner.decode", "invalid arguments"), err)
		}
		resp, err := fn(ctx, key, req)
		if err != nil {
			return nil, err
		}
		data, err := json.Marshal(resp)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", NewError("owner.encode", "invalid response"), err)
		}
		return data, nil
	}
	return nil
}

type request struct {
	Operation string          `json:"operation"`
	Key       EntityKey       `json:"key"`
	Args      json.RawMessage `json:"args"`
	Hops      int             `json:"hops"`
}

type response struct {
	Result json.RawMessage `json:"result,omitempty"`
	Error  *CodeError      `json:"error,omitempty"`
}

type executionMarker struct{}
type executionFrame struct {
	runtime *Runtime
	parent  *executionFrame
}

func checkContext(ctx context.Context, r *Runtime) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for f, _ := ctx.Value(executionMarker{}).(*executionFrame); f != nil; f = f.parent {
		if f.runtime == r {
			return ctx, NewError("owner.reentry", "synchronous owner reentry is forbidden")
		}
	}
	return ctx, ctx.Err()
}

// Call 无论主人是否在本地都复制 DTO，调用者不会取得邮箱内的实体指针。
func Call[Resp any](ctx context.Context, r *Runtime, key EntityKey, operation string, args any) (Resp, error) {
	var zero Resp
	ctx, err := checkContext(ctx, r)
	if err != nil {
		return zero, err
	}
	if r == nil {
		return zero, NewError("owner.invalid", "runtime is required")
	}
	body, err := json.Marshal(args)
	if err != nil {
		return zero, fmt.Errorf("%w: %v", NewError("owner.encode", "invalid arguments"), err)
	}
	req := request{Operation: operation, Key: key, Args: body}
	destination, err := r.destination(ctx, key)
	if err != nil {
		return zero, err
	}
	var result response
	if destination == r.self {
		result, err = r.execute(ctx, req)
	} else {
		if r.transport == nil {
			return zero, NewError("owner.config", "transport is required for remote owner")
		}
		req.Hops = 1
		wire, marshalErr := json.Marshal(req)
		if marshalErr != nil {
			return zero, marshalErr
		}
		var reply []byte
		reply, err = r.transport(ctx, destination, wire)
		if err == nil {
			err = json.Unmarshal(reply, &result)
		}
	}
	if err != nil {
		return zero, err
	}
	if result.Error != nil {
		return zero, result.Error
	}
	var out Resp
	if err := json.Unmarshal(result.Result, &out); err != nil {
		return zero, fmt.Errorf("%w: %v", NewError("owner.decode", "invalid response"), err)
	}
	return out, nil
}

// Handle 在进入实体邮箱前校验远程请求，不会转发已迁移的主人或增加网络跳数。
// 调用方必须在 actor 外使用该入口，否则同步等待实体邮箱可能造成死锁。
func (r *Runtime) Handle(ctx context.Context, body []byte) ([]byte, error) {
	ctx, err := checkContext(ctx, r)
	if err != nil {
		return encodeError(err)
	}
	if r == nil {
		return encodeError(NewError("owner.invalid", "runtime is required"))
	}
	var req request
	if err := json.Unmarshal(body, &req); err != nil {
		return encodeError(NewError("owner.decode", "invalid request: "+err.Error()))
	}
	if req.Hops != 1 {
		return encodeError(NewError("owner.hops", "remote request must have exactly one network hop"))
	}
	result, err := r.execute(ctx, req)
	if err != nil {
		return encodeError(err)
	}
	return json.Marshal(result)
}

func (r *Runtime) destination(ctx context.Context, key EntityKey) (string, error) {
	if r.self == "" || r.resolve == nil || r.actors == nil {
		return "", NewError("owner.config", "self, actor system and resolver are required")
	}
	destination, err := r.resolve(ctx, key)
	if err != nil {
		return "", fmt.Errorf("resolve owner: %w", err)
	}
	if destination == "" {
		return "", NewError("owner.resolve", "resolver returned an empty owner")
	}
	return destination, nil
}

func (r *Runtime) execute(ctx context.Context, req request) (response, error) {
	if r.actors == nil {
		return response{}, NewError("owner.config", "actor system is required")
	}
	if req.Operation == "" || req.Key.Kind == "" || !json.Valid(req.Args) {
		return response{}, NewError("owner.invalid", "operation, entity kind and JSON arguments are required")
	}
	// 结果只经缓冲通道发布；调用上下文提前取消时，不与仍在执行的邮箱协程共享可变结果。
	completed := make(chan response, 1)
	err := r.actors.Do(ctx, req.Key.String(), func() {
		result := response{}
		defer func() {
			if rec := recover(); rec != nil {
				result = response{Error: errorDTO(NewError("owner.panic", fmt.Sprintf("operation panicked: %v", rec)))}
			}
			completed <- result
		}()
		destination, err := r.destination(ctx, req.Key)
		if err != nil {
			result.Error = errorDTO(err)
			return
		}
		if destination != r.self {
			result.Error = errorDTO(NewError("owner.moved", "entity owner changed"))
			return
		}
		r.mu.RLock()
		fn := r.operations[req.Operation]
		r.mu.RUnlock()
		if fn == nil {
			result.Error = errorDTO(NewError("owner.unknown_operation", "unknown operation: "+req.Operation))
			return
		}
		parent, _ := ctx.Value(executionMarker{}).(*executionFrame)
		executionCtx := context.WithValue(ctx, executionMarker{}, &executionFrame{runtime: r, parent: parent})
		if r.Claim != nil {
			term, cerr := r.Claim(executionCtx, req.Key)
			if cerr != nil {
				result.Error = errorDTO(cerr)
				return
			}
			executionCtx = lease.WithTerm(executionCtx, term)
		}
		result.Result, err = fn(executionCtx, req.Key, req.Args)
		if err != nil {
			result = response{Error: errorDTO(err)}
		}
	})
	if err != nil {
		return response{}, err
	}
	return <-completed, nil
}

func errorDTO(err error) *CodeError {
	var coded *CodeError
	if errors.As(err, &coded) && coded != nil {
		return &CodeError{Code: coded.Code, Message: err.Error()}
	}
	return &CodeError{Code: "owner.internal", Message: err.Error()}
}

func encodeError(err error) ([]byte, error) { return json.Marshal(response{Error: errorDTO(err)}) }
