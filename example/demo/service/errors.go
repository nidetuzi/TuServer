package service

import "errors"

// 这些错误返回给调用方，用来区分参数不合法和库存不足，而不是都变成一次内部失败。
var (
	ErrBadName        = errors.New("service: bad name")
	ErrBadExp         = errors.New("service: bad exp")
	ErrBadArg         = errors.New("service: bad arg")
	ErrBadText        = errors.New("service: bad text")
	ErrNotEnough      = errors.New("service: not enough items")
	ErrAlreadyInGuild = errors.New("service: already in guild")
)
