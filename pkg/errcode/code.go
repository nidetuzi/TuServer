package errcode

// 客户端只看这些数字。同一种失败可以有多条哨兵，靠指针区分，不靠文案。
const (
	// BadArg 表示参数、名字、正文不合法，或请求正文不是合法 protobuf。
	BadArg int32 = 1
	// NotLoggedIn 表示会话还没有绑定角色。
	NotLoggedIn int32 = 2
	// NotEnough 表示道具或资源不够，本次不应改数量。
	NotEnough int32 = 3
	// AlreadyInGuild 表示角色已经在公会里。
	AlreadyInGuild int32 = 4
	// NoHandler 表示没有这个协议号。
	NoHandler int32 = 5
	// NotFound 表示要操作的实体不存在。
	NotFound int32 = 6
	// Rejected 表示主人拒绝了操作。更细的原因放在错误包的 detail，而不是再占一个数字。
	Rejected int32 = 7
	// Internal 表示这次处理失败或发生了 panic，节点本身仍可继续服务。
	Internal int32 = 100
)

// Numbered 表示错误已经对应一个客户端错误码。框架不认识的错误回 Internal。
type Numbered interface {
	error
	Errno() int32
}

// Error 是带客户端错误码的业务错误。
// 同码不同原因必须是不同指针，调用方才能用 errors.Is 分开判断。
type Error struct {
	code int32
	text string
}

// New 创建一条哨兵。返回的指针应存成包级变量，不要每次请求都 New 一次再拿去比较。
func New(code int32, text string) *Error {
	return &Error{code: code, text: text}
}

// Error 返回稳定文案，供日志和错误包里的 detail 使用。
func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return e.text
}

// Errno 返回客户端错误码。
func (e *Error) Errno() int32 {
	if e == nil {
		return Internal
	}
	return e.code
}
