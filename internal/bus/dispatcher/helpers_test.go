package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

func makeEvent(
	aggregateID string,
	version int64,
) asynxmd.Event[int] {
	return asynxmd.Event[int]{
		ID:          fmt.Sprintf("%s-v%d", aggregateID, version),
		AggregateID: aggregateID,
		EventName:   "test.event",
		Version:     version,
		OccurredAt:  time.Now(),
	}
}

func newTestDispatcher[T any](
	bus asynxmd.Bus[T],
	opts ...testOpt[T],
) *dispatcher[T] {
	c := testConfig[T]{cfg: asynxmd.DispatcherConfig[T]{
		Bus:          bus,
		RetryInitial: 100 * time.Millisecond,
		RetryMax:     30 * time.Second,
		MaxAttempts:  20,
	}}
	for _, opt := range opts {
		opt(&c)
	}
	return newDispatcher(c.cfg, c.opts...)
}

func withIdleTimeout[T any](
	t time.Duration,
) testOpt[T] {
	return func(
		c *testConfig[T],
	) {
		c.opts = append(c.opts, WithIdleTimeout[T](t))
	}
}

func withDurableSubs[T any](
	subs []asynxmd.DurableSub[T],
) testOpt[T] {
	return func(
		c *testConfig[T],
	) {
		c.cfg.DurableSubs = subs
	}
}

func withRetryBackoff[T any](
	initial time.Duration,
	max time.Duration,
) testOpt[T] {
	return func(
		c *testConfig[T],
	) {
		c.cfg.RetryInitial, c.cfg.RetryMax = initial, max
	}
}

func withMaxAttempts[T any](
	n int,
) testOpt[T] {
	return func(
		c *testConfig[T],
	) {
		c.cfg.MaxAttempts = n
	}
}

func dispatch[T any](
	ctx context.Context,
	d *dispatcher[T],
	event asynxmd.Event[T],
	waitHandlers bool,
) error {
	t, err := d.Enqueue(ctx, asynxmd.DispatchRequest[T]{Event: event})
	if err != nil {
		return err
	}
	if waitHandlers {
		<-t.Settled
	}
	return nil
}

func enqueueEvent[T any](
	ctx context.Context,
	d *dispatcher[T],
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (<-chan struct{}, error) {
	t, err := enqueueTicket(ctx, d, event, ack)
	return t.Settled, err
}

func enqueueTicket[T any](
	ctx context.Context,
	d *dispatcher[T],
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (asynxmd.Ticket, error) {
	return d.Enqueue(ctx, asynxmd.DispatchRequest[T]{Event: event, Ack: ack, Kind: asynxmd.DispatchEvent})
}

func enqueueRedelivery[T any](
	ctx context.Context,
	d *dispatcher[T],
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (asynxmd.Ticket, error) {
	return d.Enqueue(ctx, asynxmd.DispatchRequest[T]{Event: event, Ack: ack, Kind: asynxmd.DispatchRedelivery})
}

func enqueueForget[T any](
	ctx context.Context,
	d *dispatcher[T],
	tombstone asynxmd.Event[T],
	ack func(context.Context) error,
) (asynxmd.Ticket, error) {
	return d.Enqueue(ctx, asynxmd.DispatchRequest[T]{Event: tombstone, Ack: ack, Kind: asynxmd.DispatchTombstone})
}

func halted[T any](
	d *dispatcher[T],
) []string {
	return slices.Sorted(maps.Keys(d.Parks()))
}

func resume[T any](
	d *dispatcher[T],
	agg string,
) bool {
	generation, parked := d.Parks()[agg]
	if !parked {
		return false
	}
	lifted, _ := d.ResumeUndelivered(agg, generation)
	return lifted
}

func always(string) bool { return true }

func newDurable(
	t *testing.T,
	subs []asynxmd.DurableSub[int],
	opts ...testOpt[int],
) *dispatcher[int] {
	t.Helper()
	all := append([]testOpt[int]{withDurableSubs(subs), withRetryBackoff[int](time.Millisecond, 5*time.Millisecond)}, opts...)
	d := newTestDispatcher[int](&recordingBus{}, all...)
	t.Cleanup(func() { d.Close(context.Background()) })
	return d
}

func waitFor(
	t *testing.T,
	what string,
	cond func() bool,
) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		failPast(t, deadline, what)
		runtime.Gosched()
	}
}

func recv[V any](
	t *testing.T,
	ch <-chan V,
	what string,
) V {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

func final(
	t *testing.T,
	tk asynxmd.Ticket,
	what string,
) error {
	t.Helper()
	recv(t, tk.Final, what)
	return tk.Err()
}

func ackInto(
	acked *sync.Map,
	e asynxmd.Event[int],
) func(context.Context) error {
	return func(context.Context) error { acked.Store(e.Version, true); return nil }
}

func failPast(
	t *testing.T,
	deadline time.Time,
	what string,
) {
	t.Helper()
	if time.Now().After(deadline) {
		t.Fatalf("timed out waiting for %s", what)
	}
}

func checkCallVersion(
	t *testing.T,
	i int,
	e asynxmd.Event[int],
) {
	t.Helper()
	if e.Version != int64(i+1) {
		t.Errorf("call[%d] version=%d, want %d", i, e.Version, i+1)
	}
}

func checkOrderAt(
	t *testing.T,
	i int,
	v int64,
) {
	t.Helper()
	if v != int64(i+1) {
		t.Fatalf("order broken at %d: %v", i, v)
	}
}

func enqueueOrFail(
	t *testing.T,
	d *dispatcher[int],
	v int64,
) {
	t.Helper()
	if _, err := enqueueEvent(context.Background(), d, makeEvent("a", v), nil); err != nil {
		t.Fatal(err)
	}
}

func checkUnrunParked(
	t *testing.T,
	d *dispatcher[int],
	v int64,
) {
	t.Helper()
	if err := d.Unrun("a", v); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Errorf("Unrun(a, %d) = %v, want ErrDeliveryParked", v, err)
	}
}
