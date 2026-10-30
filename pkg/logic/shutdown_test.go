package logic

import (
	"context"
	"testing"
)

func TestShutdownOrder(t *testing.T) {
	var got []string
	err := ShutdownSteps{
		StopAccept: func(context.Context) error { got = append(got, "accept"); return nil },
		StopTicker: func() { got = append(got, "ticker") },
		Drain:      func(context.Context) error { got = append(got, "drain"); return nil },
		Flush:      func(context.Context) error { got = append(got, "flush"); return nil },
	}.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"accept", "ticker", "drain", "flush"}
	if len(got) != len(want) {
		t.Fatalf("顺序不符: %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("顺序不符: %v", got)
		}
	}
}
