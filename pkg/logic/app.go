package logic

import (
	"context"
	"sync"

	"tuserv/pkg/actor"
	"tuserv/pkg/handler"
	"tuserv/pkg/router"
)

// Request 是网关转发到逻辑节点的一帧。字段与内部 RPC 对齐，但这里不依赖生成代码。
type Request struct {
	GateID    string
	SessionID string
	ServerID  int32
	PlayerID  int64
	CmdID     int32
	Body      []byte
}

// Response 带回角色编号，网关才能在登录后把 playerId 记到会话上。
type Response struct {
	PlayerID int64
	CmdID    int32
	Body     []byte
}

// Session 是本节点上的一条在线会话，区服广播时用 GateAddr 推回去。
type Session struct {
	ID       string
	ServerID int32
	PlayerID int64
	GateAddr string
}

// App 把协议送进 actor 并维护本节点会话表。
type App struct {
	Router *router.Router[int32]
	Actors *actor.System
	Load   handler.Loader

	mu       sync.Mutex
	sessions map[string]Session
	// Internal 处理框架保留的 cmdId 0，用来把公会操作转到主人节点上的 actor。
	Internal func(context.Context, Request) (Response, error)
}

// New 创建逻辑节点运行时。
func New(r *router.Router[int32], load handler.Loader) *App {
	return &App{
		Router:   r,
		Actors:   actor.NewSystem(),
		Load:     load,
		sessions: make(map[string]Session),
	}
}

// Handle 在该会话的 actor 里执行协议。cmdId 小于 0 表示网关通知连接已断开。
func (a *App) Handle(ctx context.Context, req Request) (Response, error) {
	if req.CmdID < 0 {
		a.mu.Lock()
		delete(a.sessions, req.SessionID)
		a.mu.Unlock()
		return Response{}, nil
	}
	if req.CmdID == 0 && a.Internal != nil {
		return a.handleInternal(ctx, req)
	}
	var (
		resp Response
		err  error
	)
	key := req.SessionID
	if key == "" {
		key = "player"
	}
	runErr := a.Actors.Do(ctx, key, func() {
		hctx := handler.New(ctx, handler.Meta{
			ServerID:  req.ServerID,
			PlayerID:  req.PlayerID,
			SessionID: req.SessionID,
			GateID:    req.GateID,
			CmdID:     req.CmdID,
		}).WithLoader(a.Load).WithBind(func(id int64) {
			req.PlayerID = id
		})
		body, dispatchErr := a.Router.Dispatch(req.CmdID, hctx, req.Body)
		if dispatchErr != nil {
			err = dispatchErr
			return
		}
		resp = Response{PlayerID: hctx.PlayerID, CmdID: req.CmdID, Body: body}
		if req.SessionID != "" {
			a.mu.Lock()
			a.sessions[req.SessionID] = Session{
				ID:       req.SessionID,
				ServerID: req.ServerID,
				PlayerID: hctx.PlayerID,
				GateAddr: req.GateID,
			}
			a.mu.Unlock()
		}
	})
	if runErr != nil {
		return Response{}, runErr
	}
	return resp, err
}

func (a *App) handleInternal(ctx context.Context, req Request) (Response, error) {
	key := req.SessionID
	if key == "" {
		key = "internal"
	}
	var (
		resp Response
		err  error
	)
	runErr := a.Actors.Do(ctx, key, func() {
		resp, err = a.Internal(ctx, req)
	})
	if runErr != nil {
		return Response{}, runErr
	}
	return resp, err
}

// Sessions 返回本节点上某个区服的在线会话。广播不能把别的区服一起推出去。
func (a *App) Sessions(serverID int32) []Session {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []Session
	for _, s := range a.sessions {
		if s.ServerID == serverID {
			out = append(out, s)
		}
	}
	return out
}

// Stop 关闭 actor 邮箱。
func (a *App) Stop() {
	if a.Actors != nil {
		a.Actors.Stop()
	}
}
