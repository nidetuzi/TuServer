// Package config 读取三个进程共用的 yaml 结构。
// 字段放在一起，是为了网关和逻辑节点不要各写一套日志与 etcd 配置。
// 节点间密钥可由环境变量覆盖，缺省时由调用方拒绝启动。
package config
