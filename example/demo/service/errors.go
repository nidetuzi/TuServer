package service

import "tuserv/pkg/errcode"

// 这些哨兵把参数错误和库存不足分开。客户端只看错误码，服务端仍用 errors.Is 区分同码的不同原因。
var (
	// ErrBadUsername 表示渠道用户名为空或超过 256 字节。
	ErrBadUsername = errcode.New(errcode.BadArg, "service: bad username")
	// ErrBadName 表示角色或公会名字为空，或超过 16 字节。
	ErrBadName = errcode.New(errcode.BadArg, "service: bad name")
	// ErrBadExp 表示经验不是正数。
	ErrBadExp = errcode.New(errcode.BadArg, "service: bad exp")
	// ErrBadArg 表示道具号、数量或公会号不合法。
	ErrBadArg = errcode.New(errcode.BadArg, "service: bad arg")
	// ErrBadText 表示聊天正文长度不在允许范围内。
	ErrBadText = errcode.New(errcode.BadArg, "service: bad text")
	// ErrNotEnough 表示背包数量不足，本次不会扣减。
	ErrNotEnough = errcode.New(errcode.NotEnough, "service: not enough items")
	// ErrAlreadyInGuild 表示角色已经有公会。
	ErrAlreadyInGuild = errcode.New(errcode.AlreadyInGuild, "service: already in guild")
)
