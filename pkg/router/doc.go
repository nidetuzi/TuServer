// Package router 按协议键把包交给处理函数。
// 核心只认 Cmd 与 []byte；带结构体的注册通过 BodyCodec 注入，不绑死 protobuf。
package router
