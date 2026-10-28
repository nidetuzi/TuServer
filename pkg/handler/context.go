package handler

import (
	"context"
	"errors"

	"tuserv/pkg/log"
)

// ErrNotLoggedIn 表示会话还没有绑定角色。登录协议完成前不能读角色。
var ErrNotLoggedIn = errors.New("handler: player not logged in")

// Loader 按区服和角色编号读取角色。实现必须走缓存的 Get。
type Loader func(ctx context.Context, serverID int32, playerID int64) (any, error)

// Meta 是从帧和会话上复制来的身份，处理函数用它选库和打日志。
type Meta struct {
	ServerID  int32
	PlayerID  int64
	SessionID string
	GateID    string
	CmdID     int32
}

// Context 是一次协议处理的上下文。它在玩家 actor 里使用，不要传到别的 actor 再改角色。
type Context struct {
	context.Context
	Meta
	load Loader
	bind func(playerID int64)
}

// New 创建上下文。load 为空时 GetPlayer 会失败，登录以外的协议都需要它。
func New(ctx context.Context, meta Meta) *Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return &Context{Context: ctx, Meta: meta}
}

// WithLoader 注入角色读取。放在构造之后，是为了网关侧测试可以不提供角色。
func (c *Context) WithLoader(load Loader) *Context {
	c.load = load
	return c
}

// WithBind 在登录成功后把角色编号交回会话，响应里才能带上 playerId。
func (c *Context) WithBind(bind func(playerID int64)) *Context {
	c.bind = bind
	return c
}

// GetPlayer 读取当前会话的角色。未登录时返回 ErrNotLoggedIn，避免把 0 号角色当成合法数据。
func (c *Context) GetPlayer() (any, error) {
	if c.PlayerID == 0 {
		return nil, ErrNotLoggedIn
	}
	if c.load == nil {
		return nil, errors.New("handler: player loader is not set")
	}
	return c.load(c.Context, c.ServerID, c.PlayerID)
}

// BindPlayer 记录本次请求产生的角色编号，并通知外层会话。
func (c *Context) BindPlayer(playerID int64) {
	c.PlayerID = playerID
	if c.bind != nil {
		c.bind(playerID)
	}
}

// Logger 带上区服、角色和协议号。同一条请求里的日志才能对上同一次处理。
func (c *Context) Logger() interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
} {
	return log.With(
		"server_id", c.ServerID,
		"player_id", c.PlayerID,
		"cmd_id", c.CmdID,
	)
}
