package entity

import "time"

// CheckNewDay 比较最近登录、离线和本次在线期间已检查的日期，避免周期检查重复触发。
// 在线检查基准只保存在内存中，不把定时检查时间冒充登录时间；新角色不触发跨天。
func (p *Player) CheckNewDay(now time.Time, location *time.Location) bool {
	p.Lock()
	defer p.Unlock()
	previous := max(p.LastOfflineAt, p.LastLoginAt, p.lastDayCheck)
	if now.Unix() <= previous {
		return false
	}
	p.lastDayCheck = now.Unix()
	return previous > 0 && dateKey(now.In(location)) > dateKey(time.Unix(previous, 0).In(location))
}

// NoteLogin 在跨天检查之后保存成功上线时间，确保本次检查仍能读到之前的登录日期。
func (p *Player) NoteLogin(now time.Time) {
	p.Lock()
	defer p.Unlock()
	if now.Unix() > p.LastLoginAt {
		p.LastLoginAt = now.Unix()
	}
}

// NoteOffline 在刷盘前记录服务器确认断线的时间，保留原始时间戳供下次登录比较。
func (p *Player) NoteOffline(now time.Time) {
	p.Lock()
	defer p.Unlock()
	if now.Unix() > p.LastOfflineAt {
		p.LastOfflineAt = now.Unix()
	}
}

func dateKey(t time.Time) int32 {
	y, m, d := t.Date()
	return int32(y*10000 + int(m)*100 + d)
}
