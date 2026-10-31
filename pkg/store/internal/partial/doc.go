// Package partial 把落库快照收成按字段合并。
// 整篇替换会在滚动升级时丢掉旧进程不认识的键，所以保存只覆盖这次编码里出现的字段。
package partial
