package logic

import "strconv"

const (
	// CmdKick 是逻辑节点要求网关关掉一条连接时使用的推送号。
	// 它只出现在节点到网关的 Push 里，不作为客户端请求的业务协议。
	CmdKick int32 = -2
	// CmdRelinquish 是节点之间交出租约的内部帧。客户端伪造时按断线处理。
	CmdRelinquish int32 = -3
	// InternalCaller 标记节点自己发出的租约交接，网关转发的客户端帧不会带这个值。
	InternalCaller = "internal"
	// KindPlayer 和 KindGuild 区分交接的是角色还是公会。
	KindPlayer = "player"
	KindGuild  = "guild"
)

// Release 是交出租约时带给旧持有者的说明。
// Session 与本次接手的连接相同时不踢线，避免网关重选节点把还在用的连接关掉。
type Release struct {
	Kind     string `json:"kind"`
	ServerID int32  `json:"server_id"`
	ID       int64  `json:"id"`
	Term     uint64 `json:"term"`
	Gate     string `json:"gate"`
	Session  string `json:"session"`
}

func playerActorKey(serverID int32, playerID int64) string {
	return "player/" + strconv.FormatInt(int64(serverID), 10) + "/" + strconv.FormatInt(playerID, 10)
}

func playerLeaseKey(serverID int32, playerID int64) string {
	return playerActorKey(serverID, playerID)
}
