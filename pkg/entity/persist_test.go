package entity

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type sampleEnt struct {
	Identity
	Name string         `bson:"name"`
	Bag  map[int32]int32 `bson:"bag"`
}

func TestPersistBSONCopiesAndVersions(t *testing.T) {
	e := &sampleEnt{
		Identity: Identity{OID: primitive.NewObjectID()},
		Name:     "a",
		Bag:      map[int32]int32{1: 2},
	}
	e.MarkDirty()
	e.SetFence(9)
	gen, doc := PersistBSON(&e.State, e)
	pd, ok := doc.(PersistDoc)
	if !ok {
		t.Fatalf("doc type %T", doc)
	}
	rev, fence := pd.DocVersion()
	if rev != gen || fence != 9 {
		t.Fatalf("version rev=%d fence=%d gen=%d", rev, fence, gen)
	}
	var m bson.M
	if err := bson.Unmarshal(pd.raw, &m); err != nil {
		t.Fatal(err)
	}
	if m["name"] != "a" || m["rev"] != int64(gen) {
		t.Fatalf("doc=%v", m)
	}
	bag, _ := m["bag"].(bson.M)
	if bag["1"] != int32(2) && bag["1"] != int64(2) {
		// bson map keys may decode differently; check via rematch
		rawBag, err := bson.Marshal(m["bag"])
		if err != nil {
			t.Fatal(err)
		}
		var bag2 map[int32]int32
		if err := bson.Unmarshal(rawBag, &bag2); err != nil || bag2[1] != 2 {
			t.Fatalf("bag=%v raw=%v", m["bag"], bag2)
		}
	}
	e.Bag[1] = 99
	var again bson.M
	_ = bson.Unmarshal(pd.raw, &again)
	rawBag, _ := bson.Marshal(again["bag"])
	var bagCopy map[int32]int32
	_ = bson.Unmarshal(rawBag, &bagCopy)
	if bagCopy[1] != 2 {
		t.Fatalf("snapshot mutated with live entity: %v", bagCopy)
	}
}

func TestIdentityInitRevAndNoteLoaded(t *testing.T) {
	id := &Identity{OID: primitive.NewObjectID()}
	id.InitRev(3)
	if id.Rev != 3 || id.Dirty() || id.Generation() != 3 {
		t.Fatalf("init rev=%d dirty=%v gen=%d", id.Rev, id.Dirty(), id.Generation())
	}
	id.MarkDirty()
	id.Rev = 7
	id.NoteLoaded()
	if id.Dirty() || id.Generation() != 7 {
		t.Fatalf("loaded dirty=%v gen=%d", id.Dirty(), id.Generation())
	}
}
