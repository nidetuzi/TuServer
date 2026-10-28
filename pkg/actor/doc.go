// Package actor 用邮箱把同一个玩家的协议串行化。
// 这样角色内存不用再加业务锁，也避免两个请求同时改同一份脏数据。
package actor
