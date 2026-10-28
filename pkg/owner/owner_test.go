package owner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"tuserv/pkg/actor"
)

type payload struct {
	Values []int `json:"values"`
}

func local(t *testing.T) *Runtime {
	t.Helper()
	actors := actor.NewSystem()
	t.Cleanup(actors.Stop)
	return NewLocal(actors)
}

func register[Req, Resp any](t *testing.T, r *Runtime, name string, fn func(context.Context, EntityKey, Req) (Resp, error)) {
	t.Helper()
	if err := Register(r, name, fn); err != nil {
		t.Fatal(err)
	}
}

func pair(t *testing.T) (*Runtime, *Runtime, *atomic.Int32) {
	t.Helper()
	a, b := actor.NewSystem(), actor.NewSystem()
	t.Cleanup(a.Stop)
	t.Cleanup(b.Stop)
	resolve := func(context.Context, EntityKey) (string, error) { return "b", nil }
	calls := &atomic.Int32{}
	remote := New("b", b, resolve, nil)
	caller := New("a", a, resolve, func(ctx context.Context, host string, wire []byte) ([]byte, error) {
		calls.Add(1)
		if host != "b" {
			t.Errorf("unexpected destination %q", host)
		}
		return remote.Handle(ctx, wire)
	})
	return caller, remote, calls
}

func TestKey(t *testing.T) {
	got := Key(12, "guild/one", 99).String()
	if got != "owner/12/guild%2Fone/99" {
		t.Fatal(got)
	}
	keys := []EntityKey{Key(1, "guild", 2), Key(2, "guild", 2), Key(1, "player", 2), Key(1, "guild", 3), Key(1, "a/b", 2), Key(1, "a%2Fb", 2)}
	seen := map[string]bool{}
	for _, key := range keys {
		if seen[key.String()] {
			t.Fatal("key collision")
		}
		seen[key.String()] = true
	}
}

func TestLocalAndRemoteIsolation(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(fmt.Sprint(remote), func(t *testing.T) {
			caller, destination := local(t), (*Runtime)(nil)
			destination = caller
			if remote {
				caller, destination, _ = pair(t)
			}
			state := payload{Values: []int{7}}
			register(t, destination, "update", func(_ context.Context, key EntityKey, req payload) (*payload, error) {
				if key != Key(1, "guild", 2) {
					t.Errorf("wrong key: %+v", key)
				}
				req.Values[0] = 9
				state.Values[0] += 1
				return &state, nil
			})
			input := payload{Values: []int{1}}
			output, err := Call[*payload](context.Background(), caller, Key(1, "guild", 2), "update", input)
			if err != nil {
				t.Fatal(err)
			}
			if input.Values[0] != 1 || output.Values[0] != 8 {
				t.Fatalf("input=%v output=%v", input, output)
			}
			output.Values[0] = 300
			if state.Values[0] != 8 {
				t.Fatal("response leaked entity alias")
			}
		})
	}
}

func TestErrorsAndPanics(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(fmt.Sprint(remote), func(t *testing.T) {
			caller := local(t)
			destination := caller
			if remote {
				caller, destination, _ = pair(t)
			}
			register(t, destination, "business", func(context.Context, EntityKey, int) (int, error) {
				return 0, fmt.Errorf("wrapped: %w", NewError("guild.full", "full"))
			})
			register(t, destination, "panic", func(context.Context, EntityKey, int) (int, error) { panic("broken") })
			for operation, code := range map[string]string{"missing": "owner.unknown_operation", "business": "guild.full", "panic": "owner.panic"} {
				_, err := Call[int](context.Background(), caller, Key(1, "guild", 2), operation, 1)
				if !errors.Is(err, NewError(code, "different message")) {
					t.Fatalf("%s: %v", operation, err)
				}
			}
			register(t, destination, "ok", func(context.Context, EntityKey, int) (int, error) { return 4, nil })
			if got, err := Call[int](context.Background(), caller, Key(1, "guild", 2), "ok", 0); err != nil || got != 4 {
				t.Fatalf("mailbox after panic: %d %v", got, err)
			}
		})
	}
}

func TestResolverFailureAndStaleOwner(t *testing.T) {
	caller, destination, calls := pair(t)
	sentinel := errors.New("registry down")
	caller.resolve = func(context.Context, EntityKey) (string, error) { return "", sentinel }
	_, err := Call[int](context.Background(), caller, Key(1, "guild", 2), "op", 0)
	if !errors.Is(err, sentinel) || calls.Load() != 0 {
		t.Fatalf("resolver fallback: %v calls=%d", err, calls.Load())
	}
	caller.resolve = func(context.Context, EntityKey) (string, error) { return "b", nil }
	destination.resolve = func(context.Context, EntityKey) (string, error) { return "c", nil }
	var executions atomic.Int32
	register(t, destination, "op", func(context.Context, EntityKey, int) (int, error) { executions.Add(1); return 1, nil })
	_, err = Call[int](context.Background(), caller, Key(1, "guild", 2), "op", 0)
	if !errors.Is(err, NewError("owner.moved", "")) || executions.Load() != 0 || calls.Load() != 1 {
		t.Fatalf("stale owner: %v", err)
	}
	destination.resolve = func(context.Context, EntityKey) (string, error) { return "", sentinel }
	_, err = Call[int](context.Background(), caller, Key(1, "guild", 2), "op", 0)
	if err == nil || executions.Load() != 0 {
		t.Fatalf("incoming resolver fallback: %v", err)
	}
}

func TestReentry(t *testing.T) {
	r := local(t)
	register(t, r, "inner", func(context.Context, EntityKey, int) (int, error) { t.Error("reentry executed"); return 0, nil })
	register(t, r, "outer", func(ctx context.Context, key EntityKey, _ int) (int, error) {
		_, err := Call[int](ctx, r, Key(key.ServerID, key.Kind, key.ID+1), "inner", 0)
		return 0, err
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := Call[int](ctx, r, Key(1, "guild", 2), "outer", 0)
	if !errors.Is(err, NewError("owner.reentry", "")) {
		t.Fatal(err)
	}
}

func TestSerializedExecution(t *testing.T) {
	r := local(t)
	count := 0
	var active atomic.Int32
	register(t, r, "increment", func(context.Context, EntityKey, int) (int, error) {
		if active.Add(1) != 1 {
			t.Error("parallel handlers")
		}
		defer active.Add(-1)
		count++
		return count, nil
	})
	const n = 100
	var wg sync.WaitGroup
	results := make(chan int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := Call[int](context.Background(), r, Key(1, "guild", 2), "increment", 0)
			if err != nil {
				t.Error(err)
				return
			}
			results <- got
		}()
	}
	wg.Wait()
	close(results)
	seen := map[int]bool{}
	for value := range results {
		seen[value] = true
	}
	if len(seen) != n || count != n {
		t.Fatalf("lost execution: %d %d", len(seen), count)
	}
}

func TestProtocolAndRegistrationValidation(t *testing.T) {
	r := local(t)
	fn := func(context.Context, EntityKey, int) (int, error) { return 1, nil }
	register(t, r, "op", fn)
	if err := Register(r, "op", fn); !errors.Is(err, NewError("owner.duplicate", "")) {
		t.Fatal(err)
	}
	if err := Register[int, int](r, "", nil); err == nil {
		t.Fatal("invalid registration accepted")
	}
	for _, wire := range []string{`{`, `{"hops":0}`, `{"hops":2}`} {
		body, err := r.Handle(context.Background(), []byte(wire))
		if err != nil {
			t.Fatal(err)
		}
		var result response
		if err := json.Unmarshal(body, &result); err != nil || result.Error == nil {
			t.Fatalf("bad wire accepted: %s %v", body, err)
		}
	}
	_, err := Call[int](context.Background(), r, Key(1, "guild", 2), "op", "not an int")
	if !errors.Is(err, NewError("owner.decode", "")) {
		t.Fatal(err)
	}
	_, err = Call[int](context.Background(), r, Key(1, "guild", 2), "op", make(chan int))
	if !errors.Is(err, NewError("owner.encode", "")) {
		t.Fatal(err)
	}
	var missing *Runtime
	if _, err := Call[int](context.Background(), missing, Key(1, "guild", 2), "op", 0); err == nil {
		t.Fatal("nil runtime accepted")
	}
}

func TestCancellationDoesNotPublishSharedResult(t *testing.T) {
	r := local(t)
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	register(t, r, "wait", func(context.Context, EntityKey, int) (int, error) {
		close(entered)
		<-release
		defer close(finished)
		return 1, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := Call[int](ctx, r, Key(1, "guild", 2), "wait", 0); done <- err }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(release)
	<-finished
}
