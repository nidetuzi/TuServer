package entity

import (
	"go.mongodb.org/mongo-driver/bson/primitive"

	"tuserv/pkg/entity"
)

// RewardItemID 是升级事件发给背包的演示道具。
const RewardItemID int32 = 9001

// Player 是会话本地实体。脏标记嵌在 State 里，业务只调用 MarkDirty。
type Player struct {
	entity.State `bson:"-"`
	OID          primitive.ObjectID `bson:"_id"`
	PlayerID     int64              `bson:"id"`
	Server       int32              `bson:"server_id"`
	Name         string             `bson:"name"`
	Level        int32              `bson:"level"`
	Exp          int32              `bson:"exp"`
	Bag          map[int32]int32    `bson:"bag"`
	GuildID      int64              `bson:"guild_id"`
	Rev          uint64             `bson:"rev,omitempty"`
	Fence        uint64             `bson:"fence,omitempty"`
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
		OID:    primitive.NewObjectID(),
		Server: serverID,
		Name:   name,
		Level:  1,
		Bag:    map[int32]int32{},
	}
}

// CacheKey 使用 ObjectID。自增号会改查询条件，不能拿来当 Mongo 的 _id。
func (p *Player) CacheKey() string { return p.OID.Hex() }

// ServerID 决定落在哪个区服库。
func (p *Player) ServerID() int32 { return p.Server }

// Collection 是 Mongo 集合名。
func (p *Player) Collection() string { return "player" }

// RouteKey 对角色没有跨节点意义，返回 _id 只是为了满足接口。
func (p *Player) RouteKey() string { return p.OID.Hex() }

// Placement 声明角色留在会话节点。
func (p *Player) Placement() entity.Placement { return entity.PlacementLocal }

// InitRev 标记这份新建角色已经和插入时的代数对齐。
func (p *Player) InitRev(rev uint64) {
	p.State.InitRev(rev)
	p.Rev = rev
}

// NoteLoaded 在缓存从库里装入之后恢复代数，条件写才不会用更小的版本覆盖。
func (p *Player) NoteLoaded() { p.Restore(p.Rev) }

// SetFence 记下当前持有的写入任期。更小的任期不能把刚抢到的任期盖掉。
func (p *Player) SetFence(term uint64) {
	if term == 0 {
		return
	}
	p.Lock()
	if term > p.Fence {
		p.Fence = term
	}
	p.Unlock()
}

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

// Persist 在一把锁里拷贝落库字段，避免刷盘读到写一半的背包。
func (p *Player) Persist() (uint64, any) {
	doc := playerDoc{}
	gen := p.Capture(func() {
		bag := make(map[int32]int32, len(p.Bag))
		for k, v := range p.Bag {
			bag[k] = v
		}
		doc = playerDoc{
			OID: p.OID, PlayerID: p.PlayerID, Server: p.Server, Name: p.Name,
			Level: p.Level, Exp: p.Exp, Bag: bag, GuildID: p.GuildID, Fence: p.Fence,
			LastOfflineAt: p.LastOfflineAt, LastLoginAt: p.LastLoginAt,
		}
	})
	doc.Rev = gen
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
	OID           primitive.ObjectID `bson:"_id"`
	PlayerID      int64              `bson:"id"`
	Server        int32              `bson:"server_id"`
	Name          string             `bson:"name"`
	Level         int32              `bson:"level"`
	Exp           int32              `bson:"exp"`
	Bag           map[int32]int32    `bson:"bag"`
	GuildID       int64              `bson:"guild_id"`
	Rev           uint64             `bson:"rev,omitempty"`
	Fence         uint64             `bson:"fence,omitempty"`
	LastOfflineAt int64              `bson:"last_offline_at,omitempty"`
	LastLoginAt   int64              `bson:"last_login_at,omitempty"`
}

// DocVersion 提供条件写使用的代数和任期。
func (d playerDoc) DocVersion() (uint64, uint64) { return d.Rev, d.Fence }
