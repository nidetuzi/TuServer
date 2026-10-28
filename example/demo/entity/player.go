package entity

import (
	"strconv"

	"tuserv/pkg/entity"
)

// RewardItemID 是升级事件发给背包的演示道具。
const RewardItemID int32 = 9001

// Player 是会话本地实体。脏标记嵌在 State 里，业务只调用 MarkDirty。
type Player struct {
	entity.State `bson:"-"`
	ID           string          `bson:"_id"`
	PlayerID     int64           `bson:"player_id"`
	Server       int32           `bson:"server_id"`
	Name         string          `bson:"name"`
	Level        int32           `bson:"level"`
	Exp          int32           `bson:"exp"`
	Bag          map[int32]int32 `bson:"bag"`
	GuildID      int64           `bson:"guild_id"`
}

// NewPlayer 创建 1 级角色。编号由调用方分配，缓存主键用编号而不是名字。
func NewPlayer(serverID int32, playerID int64, name string) *Player {
	return &Player{
		ID:       strconv.FormatInt(playerID, 10),
		PlayerID: playerID,
		Server:   serverID,
		Name:     name,
		Level:    1,
		Bag:      map[int32]int32{},
	}
}

// CacheKey 使用角色编号。名字只是登录时的别名，不能当主键，否则改名会丢掉存档。
func (p *Player) CacheKey() string { return p.ID }

// ServerID 决定落在哪个区服库。
func (p *Player) ServerID() int32 { return p.Server }

// Collection 是 Mongo 集合名。
func (p *Player) Collection() string { return "player" }

// RouteKey 对角色没有跨节点意义，返回编号只是为了满足接口。
func (p *Player) RouteKey() string { return p.ID }

// Placement 声明角色留在会话节点。
func (p *Player) Placement() entity.Placement { return entity.PlacementLocal }

// AddExp 增加经验并在达到阈值时升级。调用方负责发升级事件。
func (p *Player) AddExp(exp int32) (levels []int32) {
	p.Lock()
	defer p.Unlock()
	p.Exp += exp
	for p.Exp >= NeedExp(p.Level) {
		p.Exp -= NeedExp(p.Level)
		p.Level++
		levels = append(levels, p.Level)
	}
	return levels
}

// BagCount 返回背包中的数量。
func (p *Player) BagCount(itemID int32) int32 {
	p.Lock()
	defer p.Unlock()
	return p.Bag[itemID]
}

// BagAdd 增减背包数量。扣到 0 时删掉键，避免刷出一堆数量为 0 的道具。
func (p *Player) BagAdd(itemID, delta int32) int32 {
	p.Lock()
	defer p.Unlock()
	if p.Bag == nil {
		p.Bag = map[int32]int32{}
	}
	n := p.Bag[itemID] + delta
	if n <= 0 {
		delete(p.Bag, itemID)
		return 0
	}
	p.Bag[itemID] = n
	return n
}

// Persist 在一把锁里拷贝落库字段，避免刷盘读到写一半的背包。
func (p *Player) Persist() (uint64, any) {
	doc := playerDoc{}
	gen := p.Capture(func() {
		bag := make(map[int32]int32, len(p.Bag))
		for k, v := range p.Bag {
			bag[k] = v
		}
		doc = playerDoc{
			ID: p.ID, PlayerID: p.PlayerID, Server: p.Server, Name: p.Name,
			Level: p.Level, Exp: p.Exp, Bag: bag, GuildID: p.GuildID,
		}
	})
	return gen, doc
}

// NeedExp 是升到下一级所需经验。等级越高越贵，演示用线性公式即可。
func NeedExp(level int32) int32 {
	if level < 1 {
		level = 1
	}
	return 100 * level
}

type playerDoc struct {
	ID       string          `bson:"_id"`
	PlayerID int64           `bson:"player_id"`
	Server   int32           `bson:"server_id"`
	Name     string          `bson:"name"`
	Level    int32           `bson:"level"`
	Exp      int32           `bson:"exp"`
	Bag      map[int32]int32 `bson:"bag"`
	GuildID  int64           `bson:"guild_id"`
}
