package mongo

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"

	"tuserv/pkg/store/internal/partial"
)

func TestFieldUpdateSetsKnownFieldsOnly(t *testing.T) {
	fields, err := partial.KnownFields(struct {
		ID    string `bson:"_id"`
		Name  string `bson:"name"`
		Level int32  `bson:"level"`
	}{ID: "aaaaaaaaaaaaaaaaaaaaaaaa", Name: "ann", Level: 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) == 0 {
		t.Fatal("应有可写字段")
	}
	raw, err := bson.Marshal(fieldUpdate(fields))
	if err != nil {
		t.Fatal(err)
	}
	set := bson.Raw(raw).Lookup("$set")
	if set.Type == 0 {
		t.Fatal("更新应为 $set")
	}
	doc := set.Document()
	if doc.Lookup("_id").Type != 0 {
		t.Fatal("不应通过 $set 修改 _id")
	}
	var got struct {
		Name  string `bson:"name"`
		Level int32  `bson:"level"`
	}
	if err := bson.Unmarshal(doc, &got); err != nil {
		t.Fatal(err)
	}
	if got.Name != "ann" || got.Level != 0 {
		t.Fatalf("已知字段应进入 $set，零值也要写: %+v", got)
	}
}
