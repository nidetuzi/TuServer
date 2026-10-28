package main

import (
	"context"

	"tuserv/pkg/actor"
	"tuserv/pkg/cluster"
	"tuserv/pkg/logic"
	"tuserv/pkg/owner"
)

// newOwners 只组装节点发现和传输，业务模块不需要知道 Kitex 消息格式。
func newOwners(self string, actors *actor.System, nodes *cluster.Nodes) *owner.Runtime {
	return owner.New(self, actors, func(ctx context.Context, key owner.EntityKey) (string, error) {
		list, err := nodes.List(ctx, "Logic")
		if err != nil {
			return "", err
		}
		inst, err := cluster.Owner(list, key.String())
		if err != nil {
			return "", err
		}
		return inst.Addr, nil
	}, func(ctx context.Context, host string, body []byte) ([]byte, error) {
		resp, err := nodes.Dispatch(ctx, host, logic.Request{CmdID: 0, Body: body})
		return resp.Body, err
	})
}
