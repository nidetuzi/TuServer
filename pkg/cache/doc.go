// Package cache 把实体留在本节点内存里，并用脏标记延迟落库。
// Get 在未命中时才会读库；Load 不导出，是为了阻止业务包绕过内存。
// 容量和空闲时间由 Policy 限制。脏实体必须先落库成功才会被淘汰。
package cache
