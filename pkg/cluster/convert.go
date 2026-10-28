package cluster

import node "tuserv/kitex_gen/node"

import "tuserv/pkg/logic"

func toDispatch(req logic.Request) *node.DispatchReq {
	return &node.DispatchReq{
		GateId:    req.GateID,
		SessionId: req.SessionID,
		ServerId:  req.ServerID,
		PlayerId:  req.PlayerID,
		CmdId:     req.CmdID,
		Body:      req.Body,
	}
}

func toNotify(serverID, cmdID int32, body []byte) *node.NotifyReq {
	return &node.NotifyReq{ServerId: serverID, CmdId: cmdID, Body: body}
}

func nodePush(sessionID string, cmdID int32, body []byte) *node.PushReq {
	return &node.PushReq{SessionId: sessionID, CmdId: cmdID, Body: body}
}
