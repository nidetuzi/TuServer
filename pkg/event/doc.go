// Package event 提供进程内的同步事件。
// 监听者要和发布者在同一个 actor 里改数据，所以 Publish 不能改成异步。
package event
