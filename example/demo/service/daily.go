package service

import (
	"time"

	"tuserv/example/demo/entity"
	"tuserv/pkg/handler"
	"tuserv/pkg/log"
)

// WithDayClock 在接入请求前固定业务时区和时钟，登录与在线任务必须使用同一套日期规则。
func (s *PlayerService) WithDayClock(location *time.Location, now func() time.Time) *PlayerService {
	if location != nil {
		s.location = location
	}
	if now != nil {
		s.now = now
	}
	return s
}

// CheckNewDay 是登录和在线任务共用的跨天入口，只能在持有写权的角色 actor 中调用。
// 登录、离线时间提供持久化基准，在线期间用内存检查时间防止同一天重复触发。
func (s *PlayerService) CheckNewDay(ctx *handler.Context) (bool, error) {
	v, err := ctx.GetPlayer()
	if err != nil {
		return false, err
	}
	p := v.(*entity.Player)
	now := s.now()
	crossed := p.CheckNewDay(now, s.location)
	if crossed {
		// 后续日常重置在此扩展，实际修改实体后再打脏；跨天检测本身不新增存档字段。
		log.Info("player new day", "server_id", ctx.ServerID, "player_id", ctx.PlayerID, "day", now.In(s.location).Format("2006-01-02"))
	}
	return crossed, nil
}

// OnOnline 先检查旧登录和离线日期，再记录本次登录，避免覆盖比较基准。
func (s *PlayerService) OnOnline(ctx *handler.Context) error {
	if _, err := s.CheckNewDay(ctx); err != nil {
		return err
	}
	v, err := ctx.GetPlayer()
	if err != nil {
		return err
	}
	p := v.(*entity.Player)
	p.NoteLogin(s.now())
	p.MarkDirty()
	return nil
}

// OnOffline 在框架仍持有角色租约、缓存保护且尚未刷盘时记录离线时间。
func (s *PlayerService) OnOffline(ctx *handler.Context) error {
	v, err := ctx.GetPlayer()
	if err != nil {
		return err
	}
	p := v.(*entity.Player)
	p.NoteOffline(s.now())
	p.MarkDirty()
	return nil
}
