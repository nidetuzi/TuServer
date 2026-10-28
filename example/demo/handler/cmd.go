package handler

// 协议号使用 int32，和帧头的 cmdId 一致。聊天推送单独编号，避免和请求的回包混淆。
const (
	CmdLogin       int32 = 1001
	CmdLevelUp     int32 = 1002
	CmdUseItem     int32 = 1003
	CmdChat        int32 = 1004
	CmdCreateGuild int32 = 1005
	CmdJoinGuild   int32 = 1006
)
