package main

import (
	"context"
	"encoding/json"
	"time"

	"tuserv/pkg/actor"
	"tuserv/pkg/cluster"
	"tuserv/pkg/lease"
	"tuserv/pkg/logic"
	"tuserv/pkg/owner"
	"tuserv/pkg/store"
)

// newOwners 只组装节点发现和传输，业务模块不需要知道 Kitex 消息格式。
// 主人选定后还要拿到租约任期，发现视图分裂时第二个节点不能把公会写进去。
func newOwners(self string, actors *actor.System, nodes *cluster.Nodes, leases store.Locker) *owner.Runtime {
	rt := owner.New(self, actors, func(ctx context.Context, key owner.EntityKey) (string, error) {
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
		resp, err := nodes.Dispatch(ctx, host, logic.Request{Internal: true, CmdID: logic.CmdOwner, Body: body})
		return resp.Body, err
	})
	rt.Claim = func(ctx context.Context, key owner.EntityKey) (uint64, error) {
		rec, err := claimLease(ctx, leases, nodes, self, key.ServerID, key.String(), logic.Release{
			Kind: logic.KindGuild, ServerID: key.ServerID, ID: key.ID,
		})
		if err != nil {
			return 0, err
		}
		return rec.Term, nil
	}
	return rt
}

func claimLease(ctx context.Context, leases store.Locker, nodes *cluster.Nodes, self string, serverID int32, key string, rel logic.Release) (store.LeaseRec, error) {
	now := time.Now()
	rec, err := lease.Claim(ctx, leases, serverID, key, self, rel.Gate, rel.Session, now, lease.TTL)
	if err == nil {
		return rec, nil
	}
	held, ok := err.(*lease.HeldError)
	if !ok || nodes == nil || held.Owner == "" || held.Owner == self {
		if ok {
			return store.LeaseRec{}, owner.NewError("owner.lease", held.Error())
		}
		return store.LeaseRec{}, err
	}
	rel.Term = held.Term
	body, mErr := json.Marshal(rel)
	if mErr != nil {
		return store.LeaseRec{}, mErr
	}
	if _, rerr := nodes.Dispatch(ctx, held.Owner, logic.Request{
		GateID: logic.InternalCaller, CmdID: logic.CmdRelinquish, Internal: true, ServerID: serverID, Body: body,
	}); rerr != nil {
		return store.LeaseRec{}, owner.NewError("owner.lease", held.Error())
	}
	rec, err = lease.Claim(ctx, leases, serverID, key, self, rel.Gate, rel.Session, now, lease.TTL)
	if err != nil {
		if held, ok := err.(*lease.HeldError); ok {
			return store.LeaseRec{}, owner.NewError("owner.lease", held.Error())
		}
		return store.LeaseRec{}, err
	}
	return rec, nil
}
