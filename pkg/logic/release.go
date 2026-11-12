package logic

import "strconv"

const (
	// CmdOwner 是节点之间转发主人调用的内部协议号。
	// 客户端帧不能使用它，是否内部调用由 Request.Internal 决定，而不是协议号本身。
	CmdOwner int32 = 0
	// CmdDisconnect 是网关在连接真正关闭时通知逻辑节点的内部协议号。
	CmdDisconnect int32 = -1
	// CmdKick 是逻辑节点要求网关关掉一条连接时使用的推送号。
	// 它只出现在节点到网关的 Push 里，不作为客户端请求的业务协议。
	CmdKick int32 = -2
	// CmdRelinquish 是节点之间交出租约的内部帧。
	CmdRelinquish int32 = -3
	// InternalCaller 是租约交接请求上的占位 Gate 地址，避免被当成某台网关去回推。
	// 它不是鉴权凭证。内部调用必须另带 Request.Internal，并由节点间验签确认。
	InternalCaller = "internal"
	// KindPlayer 表示交接角色会话租约；其它 kind 走共享实体主人交接（如 guild）。
	KindPlayer = "player"
	// KindGuild 是公会共享实体的 kind，仅作约定常量，交接体以 Release.Kind 为准。
	KindGuild = "guild"
)

// Release 是交出租约时带给旧持有者的说明。
// Kind 必须与租约键上的实体种类一致；Session 与本次接手的连接相同时不踢线，避免网关重选节点把还在用的连接关掉。
// Term 用于挡住迟到的旧交接：目标节点已持有更新任期时不得清掉新会话。
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
