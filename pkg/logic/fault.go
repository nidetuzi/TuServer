package logic

import (
	"errors"
	"strings"

	"tuserv/pkg/errcode"
	"tuserv/pkg/handler"
	"tuserv/pkg/log"
	"tuserv/pkg/owner"
	"tuserv/pkg/protocol"
	"tuserv/pkg/router"
)

// errPanic 把玩法 panic 收成内部错误码。
// 具体原因只进日志，避免把临时字符串写进客户端帧。
var errPanic = errcode.New(errcode.Internal, "logic.panic")

// clientError 把一次玩法失败编成统一错误包。
// 返回的 error 必须是 nil：网关只在 RPC 失败时清掉 Logic 绑定。
func clientError(req Request, err error) Response {
	code, detail := clientFault(err)
	if code == 0 {
		code = errcode.Internal
		detail = "logic.internal"
	}
	// panic 和未分类错误已经在别处打过日志，这里只记客户端能看到的业务码。
	if !errors.Is(err, errPanic) && detail != "logic.internal" {
		log.Warn("client error", "cmd_id", req.CmdID, "code", code, "detail", detail)
	}
	return Response{
		CmdID: protocol.CmdError,
		Body: protocol.MarshalError(protocol.ErrorBody{
			Code:   code,
			ReqCmd: req.CmdID,
			Detail: detail,
		}),
	}
}

// clientFault 把框架和玩法错误收成客户端能分支的数字码。
// 认不出来的错误一律 Internal，且不把原始文案回给客户端，里面可能有地址或库名。
func clientFault(err error) (int32, string) {
	switch {
	case err == nil:
		return 0, ""
	case errors.Is(err, router.ErrNoHandler):
		return errcode.NoHandler, "router.no_handler"
	case errors.Is(err, router.ErrBadMessage):
		return errcode.BadArg, "router.bad_message"
	case errors.Is(err, handler.ErrNotLoggedIn):
		return errcode.NotLoggedIn, "handler.not_logged_in"
	}
	var numbered errcode.Numbered
	if errors.As(err, &numbered) && numbered != nil && numbered.Errno() != 0 {
		return numbered.Errno(), numbered.Error()
	}
	var coded *owner.CodeError
	if errors.As(err, &coded) && coded != nil && coded.Code != "" {
		return ownerClientCode(coded.Code), coded.Code
	}
	log.Error("unclassified handler error", "err", err)
	return errcode.Internal, "logic.internal"
}

// ownerClientCode 把主人错误的字符串码收成客户端数字。
// owner. 前缀是框架内部故障；玩法码按后缀归类，没有约定的后缀用 Rejected，具体码仍放在 detail。
func ownerClientCode(code string) int32 {
	switch {
	case strings.HasPrefix(code, "owner."):
		return errcode.Internal
	case strings.HasSuffix(code, ".not_found"):
		return errcode.NotFound
	case strings.HasSuffix(code, ".bad_arg"), strings.HasSuffix(code, ".bad_name"), strings.HasSuffix(code, ".bad_text"), strings.HasSuffix(code, ".bad_exp"):
		return errcode.BadArg
	case strings.HasSuffix(code, ".not_enough"):
		return errcode.NotEnough
	case strings.HasSuffix(code, ".already_in_guild"):
		return errcode.AlreadyInGuild
	default:
		return errcode.Rejected
	}
}
