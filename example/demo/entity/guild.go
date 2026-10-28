package entity

import (
	"go.mongodb.org/mongo-driver/bson/primitive"

	"tuserv/pkg/entity"
	"tuserv/pkg/owner"
)

// Guild 按实体键路由到唯一节点。成员列表不能在每个逻辑节点各缓存一份。
type Guild struct {
	entity.State `bson:"-"`
	OID          primitive.ObjectID `bson:"_id"`
	GuildID      int64              `bson:"id"`
	Server       int32              `bson:"server_id"`
	Name         string             `bson:"name"`
	Members      []int64            `bson:"members"`
}

// NewGuild 创建只有创建者一名成员的公会。_id 当场生成，自增号由调用方在计数器返回后写入。
func NewGuild(serverID int32, name string, owner int64) *Guild {
	return &Guild{
		OID:     primitive.NewObjectID(),
		Server:  serverID,
		Name:    name,
		Members: []int64{owner},
	}
}

// CacheKey 使用 ObjectID，和角色一样，避免把自增号写进 _id。
func (g *Guild) CacheKey() string { return g.OID.Hex() }

// ServerID 决定公会落在哪个区服库。
func (g *Guild) ServerID() int32 { return g.Server }

// Collection 是 Mongo 集合名。
func (g *Guild) Collection() string { return "guild" }

// RouteKey 与主人运行时共用实体键，避免缓存与转发把同一公会分到不同节点。
func (g *Guild) RouteKey() string {
	return owner.Key(g.Server, "guild", g.GuildID).String()
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
		doc = guildDoc{OID: g.OID, GuildID: g.GuildID, Server: g.Server, Name: g.Name, Members: members}
	})
	return gen, doc
}

type guildDoc struct {
	OID     primitive.ObjectID `bson:"_id"`
	GuildID int64              `bson:"id"`
	Server  int32              `bson:"server_id"`
	Name    string             `bson:"name"`
	Members []int64            `bson:"members"`
}
