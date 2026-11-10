package entity

import (
	"go.mongodb.org/mongo-driver/bson/primitive"

	"tuserv/pkg/entity"
)

// RewardItemID 是升级事件发给背包的演示道具。
const RewardItemID int32 = 9001

// Player 是会话本地实体。脏标记嵌在 Identity 里，业务只调用 MarkDirty。
type Player struct {
	entity.Identity
	PlayerID int64 `bson:"id"`
	Server   int32 `bson:"server_id"`
	// Username 保存渠道账号身份，独立于可展示和修改的角色昵称。
	Username string          `bson:"username"`
	Name     string          `bson:"name"`
	Level    int32           `bson:"level"`
	Exp      int32           `bson:"exp"`
	Bag      map[int32]int32 `bson:"bag"`
	GuildID  int64           `bson:"guild_id"`
	// LastOfflineAt 保存服务器确认离线的 Unix 秒，不把进程崩溃伪装成正常断线。
	LastOfflineAt int64 `bson:"last_offline_at,omitempty"`
	// LastLoginAt 保存最近一次成功上线的 Unix 秒，在线检查不修改登录时间。
	LastLoginAt  int64 `bson:"last_login_at,omitempty"`
	lastDayCheck int64
}

// Snapshot 是一次加锁读到的角色视图，回包和广播不要在锁外拆字段。
type Snapshot struct {
	PlayerID int64
	Name     string
	Level    int32
	Exp      int32
	GuildID  int64
	// LastOfflineAt 供登录流程读取上次确认离线时间。
	LastOfflineAt int64
	// LastLoginAt 供业务读取最近一次成功上线时间。
	LastLoginAt int64
}

// NewPlayer 创建 1 级角色。_id 在插入前生成，这样还没刷盘也能放进缓存。业务编号由存储的计数器另行分配。
func NewPlayer(serverID int32, name string) *Player {
	return &Player{
		Identity: entity.Identity{OID: primitive.NewObjectID()},
		Server:   serverID,
		Name:     name,
		Level:    1,
		Bag:      map[int32]int32{},
	}
}

// ServerID 决定落在哪个区服库。
func (p *Player) ServerID() int32 { return p.Server }

// Collection 是 Mongo 集合名。
func (p *Player) Collection() string { return "player" }

// RouteKey 对角色没有跨节点意义，返回 _id 只是为了满足接口。
func (p *Player) RouteKey() string { return p.OID.Hex() }

// Placement 声明角色留在会话节点。
func (p *Player) Placement() entity.Placement { return entity.PlacementLocal }

// Snapshot 在锁内拷贝回包需要的字段。
func (p *Player) Snapshot() Snapshot {
	p.Lock()
	defer p.Unlock()
	return Snapshot{PlayerID: p.PlayerID, Name: p.Name, Level: p.Level, Exp: p.Exp, GuildID: p.GuildID, LastOfflineAt: p.LastOfflineAt, LastLoginAt: p.LastLoginAt}
}

// Guild 返回角色当前公会编号。
func (p *Player) Guild() int64 {
	p.Lock()
	defer p.Unlock()
	return p.GuildID
}

// SetGuildID 在锁内修改公会编号，避免和刷盘快照并发读写。
func (p *Player) SetGuildID(id int64) {
	p.Lock()
	p.GuildID = id
	p.Unlock()
}

// TryUse 在同一把锁里检查并扣减道具。两次加锁之间不能再插进一次扣减。
func (p *Player) TryUse(itemID, count int32) (left int32, ok bool) {
	if itemID <= 0 || count <= 0 {
		return 0, false
	}
	p.Lock()
	defer p.Unlock()
	if p.Bag == nil || p.Bag[itemID] < count {
		if p.Bag == nil {
			return 0, false
		}
		return p.Bag[itemID], false
	}
	n := p.Bag[itemID] - count
	if n == 0 {
		delete(p.Bag, itemID)
		return 0, true
	}
	p.Bag[itemID] = n
	return n, true
}

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

// Persist 深拷贝落库字段，避免刷盘读到写一半的背包。
func (p *Player) Persist() (uint64, any) {
	return entity.PersistBSON(&p.State, p)
}

// NeedExp 是升到下一级所需经验。等级越高越贵，演示用线性公式即可。
func NeedExp(level int32) int32 {
	if level < 1 {
		level = 1
	}
	return 100 * level
}
