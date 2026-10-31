package lease

import (
	"context"
	"errors"
	"time"

	"tuserv/pkg/store"
)

// TTL 是持有者不续约时，其他节点可以接手的等待时间。
// 比刷盘间隔长，是为了在线但短暂空闲的角色不会被另一台节点抢走半截脏数据。
const TTL = 15 * time.Second

// ErrHeld 表示租约还在别的节点手里且没有过期。
var ErrHeld = errors.New("lease: held")

// HeldError 带上当前持有者，调用方才能去通知它交出租约。
type HeldError struct {
	Owner   string
	Term    uint64
	Until   int64
	Gate    string
	Session string
}

// Error 保留持有者地址，日志里能看出卡在哪一台。
func (e *HeldError) Error() string {
	if e == nil {
		return ErrHeld.Error()
	}
	return "lease: held by " + e.Owner
}

// Is 让 errors.Is(err, ErrHeld) 对本地和远程包装都成立。
func (e *HeldError) Is(target error) bool { return target == ErrHeld }

type termKey struct{}

// WithTerm 把本次成功申请到的任期放进上下文，业务落库时写进快照。
func WithTerm(ctx context.Context, term uint64) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, termKey{}, term)
}

// Term 读取当前写入任期。没有申请过租约时 ok 为 false。
func Term(ctx context.Context) (uint64, bool) {
	if ctx == nil {
		return 0, false
	}
	term, ok := ctx.Value(termKey{}).(uint64)
	return term, ok && term > 0
}

// Claim 续约自己持有的租约，或在空闲、过期、已释放时接手并增加任期。
func Claim(ctx context.Context, backend store.Locker, serverID int32, key, self, gate, session string, now time.Time, ttl time.Duration) (store.LeaseRec, error) {
	if backend == nil {
		return store.LeaseRec{}, errors.New("lease: backend is required")
	}
	if ttl <= 0 {
		ttl = TTL
	}
	return backend.MutateLease(ctx, serverID, key, func(cur store.LeaseRec) (store.LeaseRec, error) {
		return decide(cur, self, gate, session, now.UnixMilli(), ttl)
	})
}

// RenewOwn 批量延长仍然属于这些 owner 和 term 的租约。
// 和 Claim 不同，这里不会在空闲或过期时接手。miss 里 ErrHeld 表示不存在或已被别人持有，其他错误表示任期变了或这次存储没写上。
// 两种失败都不该据此去要求别的节点交出。整批结果无法判断时才返回 error，此时不要把 misses 当成已经逐条核对过。
func RenewOwn(ctx context.Context, backend store.Locker, items []store.LeaseTouch) ([]store.LeaseMiss, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if backend == nil {
		return nil, errors.New("lease: backend is required")
	}
	misses, err := backend.RenewMany(ctx, items)
	if err != nil {
		return nil, err
	}
	for i := range misses {
		misses[i].Err = classifyRenewErr(misses[i])
	}
	return misses, nil
}

// classifyRenewErr 把存储层的两种没续上，收成和单条 Renew 相同的错误。
// 被别人持有要带上当前持有者；任期变化不能伪装成 ErrHeld，否则上层会把本节点的会话踢掉。
func classifyRenewErr(m store.LeaseMiss) error {
	switch {
	case errors.Is(m.Err, store.ErrLeaseHeld):
		return &HeldError{Owner: m.Owner}
	case errors.Is(m.Err, store.ErrLeaseTerm):
		return errTermChanged
	default:
		return m.Err
	}
}

// Release 交出租约。until 置 0 且清掉持有者，下一个 Claim 可以立刻接手。
// term 为 0 时只要自己还持有就释放；非 0 时任期不符说明已经被别人接走过，保持原样。
func Release(ctx context.Context, backend store.Locker, serverID int32, key, self string, term uint64) error {
	if backend == nil {
		return errors.New("lease: backend is required")
	}
	_, err := backend.MutateLease(ctx, serverID, key, func(cur store.LeaseRec) (store.LeaseRec, error) {
		if !cur.Found || cur.Owner != self {
			return cur, nil
		}
		if term != 0 && cur.Term != term {
			return cur, nil
		}
		cur.Owner = ""
		cur.Until = 0
		cur.Gate = ""
		cur.Session = ""
		return cur, nil
	})
	return err
}

// errTermChanged 表示租约还在本节点，但已经不是调用方记录的那一任期。
// 不能当成被别人持有，否则续约失败会把本节点仍在线的会话踢掉。
var errTermChanged = errors.New("lease: term changed")

// Renew 只延长 self 在该任期上仍持有的租约，不接手空闲、过期或别人的租约。
// 库里没有、已经释放，或持有者不是 self 时返回 ErrHeld。任期对不上时返回其他错误。
// 两种失败都不写库：内存和 Mongo 的 MutateLease 在 fn 报错时都保持原值。
func Renew(ctx context.Context, backend store.Locker, serverID int32, key, self string, term uint64, now time.Time, ttl time.Duration) (store.LeaseRec, error) {
	if backend == nil {
		return store.LeaseRec{}, errors.New("lease: backend is required")
	}
	if ttl <= 0 {
		ttl = TTL
	}
	return backend.MutateLease(ctx, serverID, key, func(cur store.LeaseRec) (store.LeaseRec, error) {
		return extendOwned(cur, self, term, now.UnixMilli(), ttl)
	})
}

func extendOwned(cur store.LeaseRec, self string, term uint64, now int64, ttl time.Duration) (store.LeaseRec, error) {
	if !cur.Found || cur.Owner != self {
		return cur, heldBy(cur)
	}
	// 旧任期不能把本节点后来拿到的新任期续长，否则断线时按旧任期释放会放空。
	if term == 0 || cur.Term != term {
		return cur, errTermChanged
	}
	cur.Until = now + ttl.Milliseconds()
	return cur, nil
}

func heldBy(cur store.LeaseRec) *HeldError {
	return &HeldError{Owner: cur.Owner, Term: cur.Term, Until: cur.Until, Gate: cur.Gate, Session: cur.Session}
}

func decide(cur store.LeaseRec, self, gate, session string, now int64, ttl time.Duration) (store.LeaseRec, error) {
	until := now + ttl.Milliseconds()
	if cur.Found && cur.Owner != "" && cur.Owner == self {
		cur.Until = until
		cur.Gate = gate
		cur.Session = session
		return cur, nil
	}
	if !cur.Found || cur.Until <= now || cur.Owner == "" {
		term := uint64(1)
		if cur.Found {
			term = cur.Term + 1
			if term == 0 {
				term = 1
			}
		}
		return store.LeaseRec{Owner: self, Term: term, Until: until, Gate: gate, Session: session, Found: true}, nil
	}
	return cur, heldBy(cur)
}
