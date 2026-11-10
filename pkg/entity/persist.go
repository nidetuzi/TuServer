package entity

import (
	"fmt"
	"reflect"

	"go.mongodb.org/mongo-driver/bson"
)

// PersistDoc 是 PersistBSON 产出的落库快照，带条件写代数和任期。
type PersistDoc struct {
	raw   bson.Raw
	rev   uint64
	fence uint64
}

// DocVersion 提供条件写使用的代数和任期。
func (d PersistDoc) DocVersion() (uint64, uint64) { return d.rev, d.fence }

// MarshalBSON 直接写出 Capture 时拷贝好的文档字节。
func (d PersistDoc) MarshalBSON() ([]byte, error) { return d.raw, nil }

// PersistBSON 在同一把脏标记锁里深拷贝可导出 bson 字段，并写入本次代数。
// src 应是带 bson 标签的实体指针；State 等 bson:"-" 字段会被跳过。
// 调用方必须已持有与 state 相同的快照锁语义：本函数内部通过 Capture 加锁。
func PersistBSON(state *State, src any) (uint64, any) {
	var raw bson.Raw
	var fence uint64
	var marshalErr error
	gen := state.Capture(func() {
		// 锁内直接读字段，避免 bson.M 数字类型把 fence 解丢。
		fence = fenceHeld(src)
		raw, marshalErr = bson.Marshal(src)
		if marshalErr != nil {
			return
		}
		var doc bson.M
		if err := bson.Unmarshal(raw, &doc); err != nil {
			marshalErr = err
			return
		}
		doc["rev"] = state.gen
		raw, marshalErr = bson.Marshal(doc)
	})
	if marshalErr != nil {
		panic(fmt.Sprintf("entity: PersistBSON: %v", marshalErr))
	}
	return gen, PersistDoc{raw: raw, rev: gen, fence: fence}
}

// fenceHeld 在已持有业务锁时读取 Fence。嵌套 Identity 的实体也能找到该字段。
func fenceHeld(src any) uint64 {
	v := reflect.ValueOf(src)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return 0
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return 0
	}
	if f := v.FieldByName("Fence"); f.IsValid() && f.Kind() == reflect.Uint64 {
		return f.Uint()
	}
	return 0
}
