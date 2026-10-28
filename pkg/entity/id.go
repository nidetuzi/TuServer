package entity

import "errors"

// IDSpan 是一个区服占用的编号宽度。特殊编号把区服放在高位，不同服的自增号才不会撞在一起。
const IDSpan int64 = 100000

// ErrIDRange 表示区服或自增号已经不能按区服编码。自增号跨过 IDSpan 后，高位不再是区服。
var ErrIDRange = errors.New("entity: id out of range")

// WithServer 把区服编进业务编号，结果是 serverID * IDSpan + seq。
// seq 是该区服计数器返回的自增号，从 1 开始，并且必须小于 IDSpan。
func WithServer(serverID int32, seq int64) (int64, error) {
	if serverID <= 0 || seq <= 0 || seq >= IDSpan {
		return 0, ErrIDRange
	}
	return int64(serverID)*IDSpan + seq, nil
}
