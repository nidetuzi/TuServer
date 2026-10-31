package logic

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"tuserv/pkg/actor"
	"tuserv/pkg/entity"
	"tuserv/pkg/errcode"
	"tuserv/pkg/handler"
	"tuserv/pkg/lease"
	"tuserv/pkg/log"
	"tuserv/pkg/owner"
	"tuserv/pkg/router"
	"tuserv/pkg/store"
)

// Request 是网关转发到逻辑节点的一帧。字段与内部 RPC 对齐，但这里不依赖生成代码。
type Request struct {
	GateID    string
	SessionID string
	ServerID  int32
	PlayerID  int64
	CmdID     int32
	Body      []byte
	// Internal 为真时才允许 0 和负数协议号。
	// 客户端帧里没有这一位，网关转发时也不会从帧里抄出来。
	Internal bool
}

// ErrClientCmd 表示客户端请求使用了非正协议号。
// 它走统一错误帧而不是 RPC 错误，网关才不会把还活着的 Logic 拆掉。0 和负数只留给带 Internal 的内部调用。
var ErrClientCmd = errcode.New(errcode.BadArg, "logic.client_cmd")

// ErrServerMismatch 表示已登录请求的区服和角色编号不是同一个服。
// 复用 BadArg，因为帧头区服就是这次请求的参数，客户端按已有的参数错误处理即可。
var ErrServerMismatch = errcode.New(errcode.BadArg, "logic.server_mismatch")

// ErrInternalCmd 表示内部调用带了未知协议号。
var ErrInternalCmd = errors.New("logic: unknown internal cmd")

// Response 带回角色编号，网关才能在登录后把 playerId 记到会话上。
type Response struct {
	PlayerID int64
	CmdID    int32
	Body     []byte
}

// Session 是本节点上的一条在线会话，区服广播时用 GateAddr 推回去。
type Session struct {
	ID       string
	ServerID int32
	PlayerID int64
	GateAddr string
}

type binding struct {
	Session
	Term  uint64
	unpin func()
}

// Hooks 是角色单写和踢线用的节点能力。测试可以只填其中几项。
type Hooks struct {
	// Self 是本节点对外地址，租约持有者和它比较。
	Self string
	// Leases 保存跨节点任期。为空时只在本进程内按角色串行。
	Leases store.Locker
	// AskRelease 通知旧持有者刷盘并交出租约。
	AskRelease func(ctx context.Context, host string, body []byte) error
	// Touch 把任期写进已经在内存里的角色，下一份快照才能带上 fencing。
	Touch func(ctx context.Context, serverID int32, playerID int64, term uint64)
	// FlushID 刷掉并忘掉某个区服集合里按业务编号找到的内存实体。
	FlushID func(ctx context.Context, serverID int32, collection string, id int64) error
	// Kick 让网关关掉旧连接。
	Kick func(ctx context.Context, gateAddr, sessionID string)
	// Now 供测试固定租约时间。
	Now func() time.Time
	// Guard 钉住本次邮箱处理碰到的缓存实体。开始时调用，返回的函数在处理结束时放开。
	// 邮箱正在改实体时不能淘汰，否则调用方手里的指针会和重新加载的那份分开。
	Guard func() func()
	// PinPlayer 在角色保持在线时钉住缓存。断线或交出租约时调用 release。
	// 两次协议之间邮箱是空的，只靠 Guard 盖不住这段时间。
	PinPlayer func(serverID int32, playerID int64) (release func())
}

// App 把协议送进 actor 并维护本节点会话表。
type App struct {
	Router *router.Router[int32]
	Actors *actor.System
	Load   handler.Loader
	Hooks  Hooks

	mu       sync.Mutex
	sessions map[string]Session
	bound    map[string]binding
	// gateMiss 记录网关地址连续缺席的发现次数。回到列表里就清零，避免一次抖动误踢。
	gateMiss map[string]int
	// Owners 在实体自己的邮箱中处理内部调用，不能先进入会话邮箱再等待自己。
	Owners *owner.Runtime
}

// New 创建逻辑节点运行时。
func New(r *router.Router[int32], load handler.Loader) *App {
	return &App{
		Router:   r,
		Actors:   actor.NewSystem(),
		Load:     load,
		sessions: make(map[string]Session),
		bound:    make(map[string]binding),
		gateMiss: make(map[string]int),
	}
}

// Handle 在该角色的 actor 里执行协议。已登录的请求按角色编号串行，而不是按连接。
// 客户端只能走正数协议号。断线、主人调用和租约交接必须带 Internal，避免把协议号本身当成凭证。
// 玩法失败、非法协议号，以及已登录请求的区服和角色编号对不上，都会编成统一错误包，并返回 nil error。
// 非 nil 只表示这次请求没有在本节点正常结束，网关应重选。
func (a *App) Handle(ctx context.Context, req Request) (Response, error) {
	if req.Internal {
		return a.handleInternal(ctx, req)
	}
	if req.CmdID <= 0 {
		return clientError(req, ErrClientCmd), nil
	}
	// 编号高位是区服。对不上就在进 actor 之前回错误帧，否则会按新服申领租约并把会话改挂过去。
	if req.PlayerID > 0 && req.PlayerID/entity.IDSpan != int64(req.ServerID) {
		return clientError(req, ErrServerMismatch), nil
	}
	var (
		resp  Response
		biz   error
		fatal error
	)
	key := actorKey(req)
	runErr := a.Actors.Do(ctx, key, func() {
		// 玩法 panic 不能变成空包成功。邮箱自己也会吞掉 panic，所以要在回调里收成内部错误码。
		// recover 放在 Guard 外面，panic 时先放开缓存钉住，再回内部错误码。
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("actor panic", "actor", key, "panic", rec)
				fatal = nil
				biz = errPanic
				resp = Response{}
			}
		}()
		a.withGuard(func() { a.handleInActor(ctx, req, &resp, &biz, &fatal) })
	})
	if runErr != nil {
		return Response{}, runErr
	}
	if fatal != nil {
		return Response{}, fatal
	}
	if biz != nil {
		return clientError(req, biz), nil
	}
	return resp, nil
}

// handleInActor 在角色邮箱里跑完一条协议。拆出来是为了整段都处在 Guard 里面。
// 玩法错误写入 biz，租约失败写入 fatal。两者不能合成一个 error，否则网关会拆掉还活着的 Logic。
func (a *App) handleInActor(ctx context.Context, req Request, resp *Response, biz, fatal *error) {
	if req.PlayerID > 0 && a.stale(req) {
		a.kick(ctx, req.GateID, req.SessionID)
		return
	}
	term := uint64(0)
	if req.PlayerID > 0 {
		var err error
		term, err = a.enter(ctx, req)
		if err != nil {
			// 拿不到写权说明这台节点现在服务不了这个角色，调用方应按节点故障重选。
			*fatal = err
			return
		}
		ctx = lease.WithTerm(ctx, term)
	}
	hctx := handler.New(ctx, handler.Meta{
		ServerID:  req.ServerID,
		PlayerID:  req.PlayerID,
		SessionID: req.SessionID,
		GateID:    req.GateID,
		CmdID:     req.CmdID,
	}).WithLoader(a.Load).WithBind(func(id int64) {
		req.PlayerID = id
	})
	body, dispatchErr := a.Router.Dispatch(req.CmdID, hctx, req.Body)
	if dispatchErr != nil {
		*biz = dispatchErr
		return
	}
	if term == 0 && hctx.PlayerID > 0 {
		req.PlayerID = hctx.PlayerID
		var err error
		term, err = a.enter(ctx, req)
		if err != nil {
			*fatal = err
			return
		}
	}
	*resp = Response{PlayerID: hctx.PlayerID, CmdID: req.CmdID, Body: body}
	if hctx.PlayerID > 0 && req.SessionID != "" {
		a.bindOnline(ctx, req, hctx.PlayerID, term)
	}
}

func (a *App) withGuard(fn func()) {
	if a.Hooks.Guard == nil {
		fn()
		return
	}
	release := a.Hooks.Guard()
	defer release()
	fn()
}

// handleInternal 只接受节点自己发出的命令。协议号对不上就拒绝，不能落进客户端路由。
func (a *App) handleInternal(ctx context.Context, req Request) (Response, error) {
	switch req.CmdID {
	case CmdOwner:
		if a.Owners == nil {
			return Response{}, errors.New("logic: owner runtime unavailable")
		}
		body, err := a.Owners.Handle(ctx, req.Body)
		return Response{Body: body}, err
	case CmdDisconnect:
		return Response{}, a.onDisconnect(ctx, req)
	case CmdRelinquish:
		return Response{}, a.relinquish(ctx, req.Body)
	default:
		return Response{}, ErrInternalCmd
	}
}

// RenewLeases 只延长本节点仍持有的租约。
// 空闲连接不再发协议时也要续约，否则过期后别的节点会接走还没刷盘的修改。
// 不能走 enter：Claim 失败时它会 AskRelease。锁外拿着旧快照去 enter，会让新持有者刷盘交出租约并踢掉新会话，旧节点再 Claim 成功，接着发现绑定已经没了又把租约 Release 掉，角色两边都不在线。
func (a *App) RenewLeases(ctx context.Context) {
	if a.Hooks.Leases == nil {
		return
	}
	a.mu.Lock()
	list := make([]binding, 0, len(a.bound))
	for _, b := range a.bound {
		list = append(list, b)
	}
	a.mu.Unlock()
	now := time.Now()
	if a.Hooks.Now != nil {
		now = a.Hooks.Now()
	}
	for _, b := range list {
		a.renewBound(ctx, b, now)
	}
}

// renewBound 续一条快照。先在锁内确认绑定还在且没被换掉，再按持有者和任期续期。
func (a *App) renewBound(ctx context.Context, b binding, now time.Time) {
	if !a.bindingUnchanged(b) {
		return
	}
	key := playerActorKey(b.ServerID, b.PlayerID)
	rec, err := lease.Renew(ctx, a.Hooks.Leases, b.ServerID, playerLeaseKey(b.ServerID, b.PlayerID), a.Hooks.Self, b.Term, now, lease.TTL)
	if errors.Is(err, lease.ErrHeld) {
		// 已经被别人持有或已经释放。放弃本地会话，不要再 Claim，更不能 AskRelease。
		a.abandonLost(ctx, b, err)
		return
	}
	if err != nil {
		return
	}
	a.mu.Lock()
	cur, ok := a.bound[key]
	if ok && cur.ID == b.ID && cur.Term == b.Term && cur.GateAddr == b.GateAddr {
		cur.Term = rec.Term
		a.bound[key] = cur
		a.mu.Unlock()
		return
	}
	a.mu.Unlock()
	if ok {
		return
	}
	// 续约时绑定还在，写完发现断线或死网关已经把它删了。这次只延长了自己的任期，按同一任期还回去，避免没有会话的角色占着租约。
	_ = lease.Release(ctx, a.Hooks.Leases, b.ServerID, playerLeaseKey(b.ServerID, b.PlayerID), a.Hooks.Self, b.Term)
}

func (a *App) bindingUnchanged(b binding) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	cur, ok := a.bound[playerActorKey(b.ServerID, b.PlayerID)]
	return ok && cur.ID == b.ID && cur.Term == b.Term && cur.GateAddr == b.GateAddr
}

// abandonLost 在续约发现租约已经不在本节点时丢掉本地会话。
// 刷盘放在 withGuard 里，和断线、交接一样，避免淘汰把正在写的实体换掉。
// FlushID 走原来的条件写：对方任期更新后得到 ErrStale，这份内存会被丢掉，不会覆盖对方存档。
// 不 Release，也不 AskRelease。缓存钉住要等刷盘结束再放开，否则淘汰会和这次放弃抢同一份脏数据。
func (a *App) abandonLost(ctx context.Context, b binding, held error) {
	key := playerActorKey(b.ServerID, b.PlayerID)
	_ = a.Actors.Do(ctx, key, func() {
		a.withGuard(func() {
			a.mu.Lock()
			cur, ok := a.bound[key]
			if !ok || cur.ID != b.ID || cur.Term != b.Term || cur.GateAddr != b.GateAddr {
				a.mu.Unlock()
				return
			}
			unpin := cur.unpin
			delete(a.bound, key)
			delete(a.sessions, cur.ID)
			a.mu.Unlock()
			owner := ""
			var herr *lease.HeldError
			if errors.As(held, &herr) && herr != nil {
				owner = herr.Owner
			}
			log.Warn("renew lease lost", "server_id", cur.ServerID, "player_id", cur.PlayerID, "session_id", cur.ID, "term", cur.Term, "owner", owner)
			if a.Hooks.FlushID != nil {
				_ = a.Hooks.FlushID(ctx, cur.ServerID, KindPlayer, cur.PlayerID)
			}
			if unpin != nil {
				unpin()
			}
			a.kick(ctx, cur.GateAddr, cur.ID)
		})
	})
}

// ReapGates 按本轮仍注册着的网关地址清理失联会话。
// alive 必须来自一次成功的发现；空列表表示当前没有任何网关。发现失败时不要调用，否则会把注册中心故障当成网关全灭。
// 同一地址要连续缺席 miss 次才清理，避免一次列表抖动把在线角色全部踢掉。
// 清理进入该角色自己的邮箱，先刷盘再释放租约，和客户端断线同一条路，避免脏数据留在内存里。
func (a *App) ReapGates(ctx context.Context, alive []string, miss int) {
	if miss < 1 {
		miss = 1
	}
	live := make(map[string]struct{}, len(alive))
	for _, addr := range alive {
		if addr != "" {
			live[addr] = struct{}{}
		}
	}
	a.mu.Lock()
	present := make(map[string]struct{})
	var gone []string
	for _, s := range a.sessions {
		if s.GateAddr == "" {
			continue
		}
		if _, ok := present[s.GateAddr]; ok {
			continue
		}
		present[s.GateAddr] = struct{}{}
		if _, ok := live[s.GateAddr]; ok {
			delete(a.gateMiss, s.GateAddr)
			continue
		}
		n := a.gateMiss[s.GateAddr] + 1
		a.gateMiss[s.GateAddr] = n
		if n >= miss {
			gone = append(gone, s.GateAddr)
		}
	}
	for addr := range a.gateMiss {
		if _, ok := present[addr]; !ok {
			delete(a.gateMiss, addr)
		}
	}
	a.mu.Unlock()
	for _, addr := range gone {
		a.dropGate(ctx, addr)
		a.mu.Lock()
		if !a.hasGateLocked(addr) {
			delete(a.gateMiss, addr)
		}
		a.mu.Unlock()
	}
}

// Sessions 返回本节点上某个区服的在线会话。广播不能把别的区服一起推出去。
func (a *App) Sessions(serverID int32) []Session {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []Session
	for _, s := range a.sessions {
		if s.ServerID == serverID {
			out = append(out, s)
		}
	}
	return out
}

// Stop 关闭 actor 邮箱并等已排队的任务结束。
func (a *App) Stop() {
	if a.Actors != nil {
		a.Actors.Stop()
	}
}

// Shutdown 在超时内排空邮箱。停机时先停接入和定时器，再调用它，最后才刷盘。
func (a *App) Shutdown(ctx context.Context) error {
	if a.Actors == nil {
		return nil
	}
	return a.Actors.Shutdown(ctx)
}

func actorKey(req Request) string {
	if req.PlayerID > 0 {
		return playerActorKey(req.ServerID, req.PlayerID)
	}
	if req.SessionID == "" {
		return "session/player"
	}
	return "session/" + req.SessionID
}

func (a *App) stale(req Request) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	b, ok := a.bound[playerActorKey(req.ServerID, req.PlayerID)]
	return ok && b.ID != "" && req.SessionID != "" && b.ID != req.SessionID
}

func (a *App) bindOnline(ctx context.Context, req Request, playerID int64, term uint64) {
	key := playerActorKey(req.ServerID, playerID)
	sess := Session{ID: req.SessionID, ServerID: req.ServerID, PlayerID: playerID, GateAddr: req.GateID}
	a.mu.Lock()
	prev, exists := a.bound[key]
	needPin := !exists || prev.unpin == nil
	a.mu.Unlock()
	// 同一角色在线期间只钉一次。每次协议都钉，断线只放开一次，引用会计数漏掉。
	var pinned func()
	if needPin && a.Hooks.PinPlayer != nil {
		pinned = a.Hooks.PinPlayer(req.ServerID, playerID)
	}
	var drop []func()
	a.mu.Lock()
	a.sessions[req.SessionID] = sess
	prev, exists = a.bound[key]
	kickOld := exists && prev.ID != "" && prev.ID != req.SessionID
	if term == 0 {
		term = prev.Term
	}
	unpin := prev.unpin
	if unpin == nil {
		unpin = pinned
		pinned = nil
	}
	a.bound[key] = binding{Session: sess, Term: term, unpin: unpin}
	for k, v := range a.bound {
		if k != key && v.ID == req.SessionID {
			if v.unpin != nil {
				drop = append(drop, v.unpin)
			}
			delete(a.bound, k)
		}
	}
	if kickOld {
		delete(a.sessions, prev.ID)
	}
	a.mu.Unlock()
	if pinned != nil {
		drop = append(drop, pinned)
	}
	for _, fn := range drop {
		fn()
	}
	if kickOld {
		a.kick(ctx, prev.GateAddr, prev.ID)
	}
}

func (a *App) onDisconnect(ctx context.Context, req Request) error {
	if req.PlayerID <= 0 {
		if req.GateID != "" && !a.onGate(req.SessionID, req.GateID) {
			return nil
		}
		// 登录回包还没把 playerID 写回网关时连接已经断了，通知里会是 0。
		// 角色可能已经绑在这条 session 上，只删会话会把租约和缓存钉住留到被续租永久占住。
		if serverID, playerID, ok := a.boundPlayer(req.SessionID); ok {
			req.ServerID = serverID
			req.PlayerID = playerID
		} else {
			a.dropSession(req.SessionID)
			return nil
		}
	}
	return a.Actors.Do(ctx, playerActorKey(req.ServerID, req.PlayerID), func() {
		a.withGuard(func() {
			key := playerActorKey(req.ServerID, req.PlayerID)
			a.mu.Lock()
			b, ok := a.bound[key]
			a.mu.Unlock()
			// 带了网关地址却已经换到别的网关时，不能刷掉新会话，也不能放开还在用的缓存钉住。
			moved := ok && b.ID == req.SessionID && req.GateID != "" && b.GateAddr != req.GateID
			if !ok || b.ID != req.SessionID || moved {
				if req.GateID == "" || a.onGate(req.SessionID, req.GateID) {
					a.dropSession(req.SessionID)
				}
				return
			}
			if a.Hooks.FlushID != nil {
				_ = a.Hooks.FlushID(ctx, req.ServerID, "player", req.PlayerID)
			}
			if a.Hooks.Leases != nil {
				_ = lease.Release(ctx, a.Hooks.Leases, req.ServerID, playerLeaseKey(req.ServerID, req.PlayerID), a.Hooks.Self, b.Term)
			}
			var unpin func()
			a.mu.Lock()
			if cur, still := a.bound[key]; still && cur.ID == req.SessionID && (req.GateID == "" || cur.GateAddr == req.GateID) {
				unpin = cur.unpin
				delete(a.bound, key)
			}
			if req.GateID == "" {
				delete(a.sessions, req.SessionID)
			} else if s, still := a.sessions[req.SessionID]; still && s.GateAddr == req.GateID {
				delete(a.sessions, req.SessionID)
			}
			a.mu.Unlock()
			// 刷盘和 Forget 完成后再放开。提前放开的话，淘汰会和这次交接抢同一份脏数据。
			if unpin != nil {
				unpin()
			}
		})
	})
}

func (a *App) dropGate(ctx context.Context, gateAddr string) {
	a.mu.Lock()
	list := make([]Session, 0)
	for _, s := range a.sessions {
		if s.GateAddr == gateAddr {
			list = append(list, s)
		}
	}
	a.mu.Unlock()
	if len(list) == 0 {
		return
	}
	log.Info("gate offline", "gate", gateAddr, "sessions", len(list))
	for _, s := range list {
		// 必须走内部断线。不带 Internal 时负数协议号会被当成客户端非法帧，会话和缓存钉住都留着。
		_, _ = a.Handle(ctx, Request{
			GateID:    gateAddr,
			SessionID: s.ID,
			ServerID:  s.ServerID,
			PlayerID:  s.PlayerID,
			CmdID:     CmdDisconnect,
			Internal:  true,
		})
	}
}

// boundPlayer 按连接编号反查已经登录的角色。会话表和绑定是一起写上的，对不上就当没有。
func (a *App) boundPlayer(sessionID string) (serverID int32, playerID int64, ok bool) {
	if sessionID == "" {
		return 0, 0, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	s, found := a.sessions[sessionID]
	if !found || s.PlayerID <= 0 {
		return 0, 0, false
	}
	b, found := a.bound[playerActorKey(s.ServerID, s.PlayerID)]
	if !found || b.ID != sessionID || b.PlayerID <= 0 {
		return 0, 0, false
	}
	return b.ServerID, b.PlayerID, true
}

func (a *App) onGate(id, gate string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	s, ok := a.sessions[id]
	return ok && s.GateAddr == gate
}

func (a *App) hasGateLocked(gate string) bool {
	for _, s := range a.sessions {
		if s.GateAddr == gate {
			return true
		}
	}
	return false
}

func (a *App) dropSession(id string) {
	if id == "" {
		return
	}
	a.mu.Lock()
	delete(a.sessions, id)
	a.mu.Unlock()
}

func (a *App) enter(ctx context.Context, req Request) (uint64, error) {
	if a.Hooks.Leases == nil || req.PlayerID == 0 {
		return 0, nil
	}
	now := time.Now()
	if a.Hooks.Now != nil {
		now = a.Hooks.Now()
	}
	key := playerLeaseKey(req.ServerID, req.PlayerID)
	rec, err := lease.Claim(ctx, a.Hooks.Leases, req.ServerID, key, a.Hooks.Self, req.GateID, req.SessionID, now, lease.TTL)
	if err != nil {
		held, ok := err.(*lease.HeldError)
		if !ok || a.Hooks.AskRelease == nil || held.Owner == "" || held.Owner == a.Hooks.Self {
			return 0, err
		}
		body, mErr := json.Marshal(Release{
			Kind: KindPlayer, ServerID: req.ServerID, ID: req.PlayerID, Term: held.Term,
			Gate: req.GateID, Session: req.SessionID,
		})
		if mErr != nil {
			return 0, mErr
		}
		if rerr := a.Hooks.AskRelease(ctx, held.Owner, body); rerr != nil {
			return 0, err
		}
		rec, err = lease.Claim(ctx, a.Hooks.Leases, req.ServerID, key, a.Hooks.Self, req.GateID, req.SessionID, now, lease.TTL)
		if err != nil {
			return 0, err
		}
	}
	if a.Hooks.Touch != nil {
		a.Hooks.Touch(ctx, req.ServerID, req.PlayerID, rec.Term)
	}
	return rec.Term, nil
}

func (a *App) relinquish(ctx context.Context, body []byte) error {
	var rel Release
	if err := json.Unmarshal(body, &rel); err != nil {
		return err
	}
	if rel.Kind == KindGuild {
		key := owner.Key(rel.ServerID, KindGuild, rel.ID)
		return a.Actors.Do(ctx, key.String(), func() {
			a.withGuard(func() {
				if a.Hooks.FlushID != nil {
					_ = a.Hooks.FlushID(ctx, rel.ServerID, KindGuild, rel.ID)
				}
				if a.Hooks.Leases != nil {
					_ = lease.Release(ctx, a.Hooks.Leases, rel.ServerID, key.String(), a.Hooks.Self, rel.Term)
				}
			})
		})
	}
	if rel.ID == 0 {
		return nil
	}
	return a.Actors.Do(ctx, playerActorKey(rel.ServerID, rel.ID), func() {
		a.withGuard(func() {
			key := playerActorKey(rel.ServerID, rel.ID)
			a.mu.Lock()
			b, ok := a.bound[key]
			var unpin func()
			if ok {
				unpin = b.unpin
				delete(a.bound, key)
				delete(a.sessions, b.ID)
			}
			a.mu.Unlock()
			if a.Hooks.FlushID != nil {
				_ = a.Hooks.FlushID(ctx, rel.ServerID, KindPlayer, rel.ID)
			}
			if a.Hooks.Leases != nil {
				term := rel.Term
				if ok && b.Term != 0 {
					term = b.Term
				}
				_ = lease.Release(ctx, a.Hooks.Leases, rel.ServerID, playerLeaseKey(rel.ServerID, rel.ID), a.Hooks.Self, term)
			}
			if unpin != nil {
				unpin()
			}
			if ok && (b.ID != rel.Session || b.GateAddr != rel.Gate) {
				a.kick(ctx, b.GateAddr, b.ID)
			}
		})
	})
}

func (a *App) kick(ctx context.Context, gateAddr, sessionID string) {
	if a.Hooks.Kick == nil || sessionID == "" || gateAddr == "" {
		return
	}
	a.Hooks.Kick(ctx, gateAddr, sessionID)
}
