package logic

import "context"

// ShutdownSteps 固定停机顺序：先停接入和业务任务，再停基础定时器，排空邮箱，最后刷盘。
// 定时器如果还在跑就做最终 Flush，两份快照会交错落库。
type ShutdownSteps struct {
	StopAccept func(context.Context) error
	// StopTasks 在基础续租仍运行时等待业务任务退出，失败后禁止最终刷盘。
	StopTasks  func(context.Context) error
	StopTicker func()
	Drain      func(context.Context) error
	Flush      func(context.Context) error
}

// Run 按字段顺序执行。某一步失败时不再做后面的步骤，避免在还没收口时刷盘。
func (s ShutdownSteps) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.StopAccept != nil {
		if err := s.StopAccept(ctx); err != nil {
			return err
		}
	}
	if s.StopTasks != nil {
		if err := s.StopTasks(ctx); err != nil {
			return err
		}
	}
	if s.StopTicker != nil {
		s.StopTicker()
	}
	if s.Drain != nil {
		if err := s.Drain(ctx); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.Flush != nil {
		return s.Flush(ctx)
	}
	return nil
}
