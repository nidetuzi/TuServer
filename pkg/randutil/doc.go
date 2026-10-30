// Package randutil 提供游戏后端常用的随机工具。
//
// 设计上刻意不依赖 math/rand/v2 的全局源，而是通过 Rand 结构体持有独立的
// 随机源，方便按需注入种子做复现，也避免全局锁竞争。
package randutil
