package workers_test

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/bus/dispatcher"
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
) asynxmd.Dispatcher[T] {
	c := testConfig[T]{cfg: asynxmd.DispatcherConfig[T]{
		Bus:          bus,
		RetryInitial: 100 * time.Millisecond,
		RetryMax:     30 * time.Second,
		MaxAttempts:  20,
	}}
	for _, opt := range opts {
		opt(&c)
	}
	return dispatcher.New(c.cfg, c.opts...)
}

func withIdleTimeout[T any](
	t time.Duration,
) testOpt[T] {
	return func(
		c *testConfig[T],
	) {
		c.opts = append(c.opts, dispatcher.WithIdleTimeout[T](t))
	}
}

func withPublishErrorHandler[T any](
	fn asynxmd.PublishErrorHandler[T],
) testOpt[T] {
	return func(
		c *testConfig[T],
	) {
		c.cfg.OnPublishError = fn
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
	d asynxmd.Dispatcher[T],
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
	d asynxmd.Dispatcher[T],
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (<-chan struct{}, error) {
	t, err := enqueueTicket(ctx, d, event, ack)
	return t.Settled, err
}

func enqueueTicket[T any](
	ctx context.Context,
	d asynxmd.Dispatcher[T],
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (asynxmd.Ticket, error) {
	return d.Enqueue(ctx, asynxmd.DispatchRequest[T]{Event: event, Ack: ack, Kind: asynxmd.DispatchEvent})
}

func enqueueRedelivery[T any](
	ctx context.Context,
	d asynxmd.Dispatcher[T],
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (asynxmd.Ticket, error) {
	return d.Enqueue(ctx, asynxmd.DispatchRequest[T]{Event: event, Ack: ack, Kind: asynxmd.DispatchRedelivery})
}

func enqueueForget[T any](
	ctx context.Context,
	d asynxmd.Dispatcher[T],
	tombstone asynxmd.Event[T],
	ack func(context.Context) error,
) (asynxmd.Ticket, error) {
	return d.Enqueue(ctx, asynxmd.DispatchRequest[T]{Event: tombstone, Ack: ack, Kind: asynxmd.DispatchTombstone})
}

func halted[T any](
	d asynxmd.Dispatcher[T],
) []string {
	return slices.Sorted(maps.Keys(d.Parks()))
}

func always(string) bool { return true }

func newDurable(
	t *testing.T,
	subs []asynxmd.DurableSub[int],
	opts ...testOpt[int],
) asynxmd.Dispatcher[int] {
	t.Helper()
	all := append([]testOpt[int]{withDurableSubs(subs), withRetryBackoff[int](time.Millisecond, 5*time.Millisecond)}, opts...)
	d := newTestDispatcher[int](&recordingBus{}, all...)
	t.Cleanup(func() { d.Close(context.Background()) })
	return d
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

func dispatchOrFail(
	t *testing.T,
	d asynxmd.Dispatcher[int],
	i int64,
) {
	t.Helper()
	if err := dispatch(context.Background(), d, makeEvent("agg-1", i), false); err != nil {
		t.Fatalf("dispatch v%d: %v", i, err)
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

func dispatchToEach(
	ctx context.Context,
	d asynxmd.Dispatcher[int],
	v int64,
	aggs ...string,
) {
	for _, agg := range aggs {
		_ = dispatch(ctx, d, makeEvent(agg, v), false)
	}
}

func checkAggregateOrdered(
	t *testing.T,
	agg string,
	versions []int64,
) {
	t.Helper()
	if len(versions) != 5 {
		t.Errorf("aggregate %s: expected 5 events, got %d", agg, len(versions))
	}
	i := 1
	for i < len(versions) && versions[i] > versions[i-1] {
		i++
	}
	if i < len(versions) {
		t.Errorf("aggregate %s: out of order at index %d: %v", agg, i, versions)
	}
}

func checkNotSeen(
	t *testing.T,
	seen map[int64]bool,
	version int64,
) {
	t.Helper()
	if seen[version] {
		t.Errorf("duplicate version %d", version)
	}
}

func enqueueOnTheIdleDeadline(
	t *testing.T,
	d asynxmd.Dispatcher[int],
	i int64,
) {
	t.Helper()
	done, err := enqueueEvent(context.Background(), d, makeEvent("a", i), nil)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("event %d was stranded on a retired queue", i)
	}
}
