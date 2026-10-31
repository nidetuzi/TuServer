package partial

import "go.mongodb.org/mongo-driver/bson"

// KnownFields 取出这次快照编码后的顶层字段，并去掉 _id。
// Mongo 不允许改主键，主键由查询条件决定；这些字段交给 $set 后，库里多出来的键不会被删掉。
// omitempty 的零值不会出现在结果里，因此也不会把库里对应的旧值清掉。需要清掉的字段不要加 omitempty。
func KnownFields(doc any) (bson.D, error) {
	raw, err := bson.Marshal(doc)
	if err != nil {
		return nil, err
	}
	fields, err := topLevel(raw)
	if err != nil {
		return nil, err
	}
	out := make(bson.D, 0, len(fields))
	for _, el := range fields {
		if el.Key == "_id" {
			continue
		}
		out = append(out, el)
	}
	return out, nil
}

// MergeKnown 把 fresh 里的顶层字段盖到 old 上。
// old 为空表示文档还不存在，直接使用 fresh，让首次写入仍是整篇文档。
// 已有文档只覆盖 fresh 里出现的键，没出现的键原样保留，和 $set 一致。
func MergeKnown(old, fresh bson.Raw) (bson.Raw, error) {
	if len(old) == 0 {
		return fresh, nil
	}
	base, err := topLevel(old)
	if err != nil {
		return nil, err
	}
	patch, err := topLevel(fresh)
	if err != nil {
		return nil, err
	}
	index := make(map[string]int, len(base))
	for i, el := range base {
		index[el.Key] = i
	}
	for _, el := range patch {
		if i, ok := index[el.Key]; ok {
			base[i] = el
		} else {
			index[el.Key] = len(base)
			base = append(base, el)
		}
	}
	return bson.Marshal(base)
}

// topLevel 按原始字节取出顶层键，避免先解码成 Go 类型再编码时改掉未知字段的表示。
func topLevel(raw bson.Raw) (bson.D, error) {
	els, err := raw.Elements()
	if err != nil {
		return nil, err
	}
	out := make(bson.D, 0, len(els))
	for _, el := range els {
		out = append(out, bson.E{Key: el.Key(), Value: el.Value()})
	}
	return out, nil
}
