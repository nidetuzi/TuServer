// Package handler 提供协议处理函数看到的上下文。
// GetPlayer 从这里进入，是为了让业务只能走缓存的 Get，而不能自己 Load 数据库。
package handler
