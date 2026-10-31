package partial

import (
	"bytes"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestMergeKeepsUnknownFieldBytes(t *testing.T) {
	id := primitive.NewObjectID()
	type full struct {
		ID    primitive.ObjectID `bson:"_id"`
		Name  string             `bson:"name"`
		Title string             `bson:"title"`
		Level int32              `bson:"level"`
	}
	type old struct {
		ID    primitive.ObjectID `bson:"_id"`
		Name  string             `bson:"name"`
		Level int32              `bson:"level"`
	}
	before, err := bson.Marshal(full{ID: id, Name: "ann", Title: "hero", Level: 4})
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := bson.Marshal(old{ID: id, Name: "bea", Level: 0})
	if err != nil {
		t.Fatal(err)
	}
	merged, err := MergeKnown(before, fresh)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bson.Raw(before).Lookup("title").Value, merged.Lookup("title").Value) {
		t.Fatal("未知字段的原始字节被改写")
	}
	var got full
	if err := bson.Unmarshal(merged, &got); err != nil {
		t.Fatal(err)
	}
	if got.ID != id || got.Name != "bea" || got.Title != "hero" || got.Level != 0 {
		t.Fatalf("合并结果不符: %+v", got)
	}
}

func TestMergeSkipsOmittedZero(t *testing.T) {
	type doc struct {
		ID    string `bson:"_id"`
		Name  string `bson:"name,omitempty"`
		Level int32  `bson:"level"`
	}
	before, err := bson.Marshal(doc{ID: "a", Name: "ann", Level: 2})
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := bson.Marshal(doc{ID: "a", Level: 3})
	if err != nil {
		t.Fatal(err)
	}
	merged, err := MergeKnown(before, fresh)
	if err != nil {
		t.Fatal(err)
	}
	var got doc
	if err := bson.Unmarshal(merged, &got); err != nil {
		t.Fatal(err)
	}
	if got.Name != "ann" || got.Level != 3 {
		t.Fatalf("omitempty 的零值不应清掉旧值: %+v", got)
	}
}

func TestMergeEmptyOldIsFresh(t *testing.T) {
	fresh, err := bson.Marshal(struct {
		ID   string `bson:"_id"`
		Name string `bson:"name"`
	}{ID: "a", Name: "ann"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := MergeKnown(nil, fresh)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, fresh) {
		t.Fatal("没有旧文档时应保持整篇编码")
	}
}

func TestKnownFieldsOmitIDAndKeepZero(t *testing.T) {
	fields, err := KnownFields(struct {
		ID    string `bson:"_id"`
		Name  string `bson:"name"`
		Level int32  `bson:"level"`
	}{ID: "a", Name: "ann", Level: 0})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := bson.Marshal(bson.D(fields))
	if err != nil {
		t.Fatal(err)
	}
	doc := bson.Raw(raw)
	if doc.Lookup("_id").Type != 0 {
		t.Fatal("字段列表不应包含 _id")
	}
	name, ok := doc.Lookup("name").StringValueOK()
	if !ok || name != "ann" {
		t.Fatalf("name=%q ok=%v", name, ok)
	}
	level, ok := doc.Lookup("level").Int32OK()
	if !ok || level != 0 {
		t.Fatalf("零值字段应写入: %d ok=%v", level, ok)
	}
}
