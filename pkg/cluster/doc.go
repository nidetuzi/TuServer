// Package cluster 负责节点发现、加权选点和定向调用。
// 加权随机只用于新会话；选中之后由网关记住地址，避免同一角色的脏缓存出现在多台节点上。
package cluster
