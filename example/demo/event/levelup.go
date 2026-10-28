package event

import "context"

import "tuserv/example/demo/entity"

// PlayerLevelUp 在角色每次升级时发布。Player 指向正在处理的那一份内存。
type PlayerLevelUp struct {
	Player *entity.Player
	Level  int32
}

// OnLevelUpReward 给背包加一个奖励道具。
// 它在发布者的 actor 里同步执行，所以可以直接改这份角色。
func OnLevelUpReward(_ context.Context, ev PlayerLevelUp) {
	if ev.Player == nil {
		return
	}
	ev.Player.BagAdd(entity.RewardItemID, 1)
	ev.Player.MarkDirty()
}
