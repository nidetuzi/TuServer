package entity

import (
	"strconv"

	"tuserv/pkg/entity"
)

// Guild 按实体键路由到唯一节点。成员列表不能在每个逻辑节点各缓存一份。
type Guild struct {
	entity.State `bson:"-"`
	ID           string  `bson:"_id"`
	GuildID      int64   `bson:"guild_id"`
	Server       int32   `bson:"server_id"`
	Name         string  `bson:"name"`
	Members      []int64 `bson:"members"`
}

// NewGuild 创建只有创建者一名成员的公会。
func NewGuild(serverID int32, guildID int64, name string, owner int64) *Guild {
	return &Guild{
		ID:      strconv.FormatInt(guildID, 10),
		GuildID: guildID,
		Server:  serverID,
		Name:    name,
		Members: []int64{owner},
	}
}

// CacheKey 使用公会编号。
func (g *Guild) CacheKey() string { return g.ID }

// ServerID 决定公会落在哪个区服库。
func (g *Guild) ServerID() int32 { return g.Server }

// Collection 是 Mongo 集合名。
func (g *Guild) Collection() string { return "guild" }

// RouteKey 带上区服，避免不同区的相同编号被哈希到一起后互相覆盖。
func (g *Guild) RouteKey() string {
	return strconv.FormatInt(int64(g.Server), 10) + ":" + g.ID
}

// Placement 声明公会必须按键找主人节点。
func (g *Guild) Placement() entity.Placement { return entity.PlacementRouted }

// AddMember 把角色写入成员列表。已经在会里则不重复添加。
func (g *Guild) AddMember(playerID int64) {
	g.Lock()
	defer g.Unlock()
	for _, id := range g.Members {
		if id == playerID {
			return
		}
	}
	g.Members = append(g.Members, playerID)
}

// HasMember 判断角色是否已在公会中。
func (g *Guild) HasMember(playerID int64) bool {
	g.Lock()
	defer g.Unlock()
	for _, id := range g.Members {
		if id == playerID {
			return true
		}
	}
	return false
}

// Persist 拷贝成员列表再落库，避免刷盘时和加入操作互相踩切片。
func (g *Guild) Persist() (uint64, any) {
	doc := guildDoc{}
	gen := g.Capture(func() {
		members := append([]int64(nil), g.Members...)
		doc = guildDoc{ID: g.ID, GuildID: g.GuildID, Server: g.Server, Name: g.Name, Members: members}
	})
	return gen, doc
}

type guildDoc struct {
	ID      string  `bson:"_id"`
	GuildID int64   `bson:"guild_id"`
	Server  int32   `bson:"server_id"`
	Name    string  `bson:"name"`
	Members []int64 `bson:"members"`
}
