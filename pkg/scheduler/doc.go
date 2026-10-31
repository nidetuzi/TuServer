// Package scheduler 提供基于 cron 的进程内周期调度。
// 调度只决定何时触发，实体写权和 actor 串行由调用方的运行时保证；任务不持久化也不自动补跑。
package scheduler
