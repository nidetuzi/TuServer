package logic

import (
	"context"
	"sync"
	"time"
)

// Loops 是逻辑节点上互不阻塞的后台循环。
// 续租如果和刷盘、淘汰、失联清理挤在同一个 select 里，Mongo 一慢或一次清掉几千个会话，租约就会超过 TTL，别的节点可以合法接走，原节点最后一段修改会因 ErrStale 被丢掉。
type Loops struct {
	// RenewEvery 是续租间隔，应明显小于租约 TTL。
	RenewEvery time.Duration
	// FlushEvery 是刷盘和淘汰的间隔。
	FlushEvery time.Duration
	// ProbeEvery 是对照注册中心网关列表的间隔。
	ProbeEvery time.Duration
	// Renew 只续本节点仍持有的租约，每轮的截止时间为 RenewEvery，存储调用必须遵守 ctx。
	Renew func(context.Context)
	// Flush 刷盘并淘汰。这两步仍放在一起，因为它们都只动缓存，不负责续租。
	Flush func(context.Context)
	// Probe 发现失联网关并清理其会话。清理再慢也不应挡住 Renew。
	Probe func(context.Context)
}

// Run 跑到 stop 被关闭，并等已经开始的那一轮结束。
// 停机要先等它返回再排空和最终刷盘，否则定时刷盘会和退出刷盘交错。
func (l Loops) Run(stop <-chan struct{}) {
	if stop == nil {
		return
	}
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		l.loop(stop, l.RenewEvery, func(ctx context.Context) {
			if l.Renew == nil {
				return
			}
			every := l.RenewEvery
			if every <= 0 {
				every = time.Second
			}
			ctx, cancel := context.WithTimeout(ctx, every)
			defer cancel()
			l.Renew(ctx)
		})
	}()
	go func() {
		defer wg.Done()
		l.loop(stop, l.FlushEvery, l.Flush)
	}()
	go func() {
		defer wg.Done()
		l.loop(stop, l.ProbeEvery, l.Probe)
	}()
	wg.Wait()
}

func (l Loops) loop(stop <-chan struct{}, every time.Duration, fn func(context.Context)) {
	if every <= 0 {
		every = time.Second
	}
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			select {
			case <-stop:
				return
			default:
			}
			if fn != nil {
				fn(context.Background())
			}
		}
	}
}
