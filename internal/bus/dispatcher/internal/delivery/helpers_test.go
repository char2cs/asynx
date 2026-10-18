package delivery_test

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

func withDurableSubs[T any](
	subs []asynxmd.DurableSub[T],
) testOpt[T] {
	return func(
		c *testConfig[T],
	) {
		c.cfg.DurableSubs = subs
	}
}

func withDeliveryErrorHandler[T any](
	fn asynxmd.DeliveryErrorHandler[T],
) testOpt[T] {
	return func(
		c *testConfig[T],
	) {
		c.cfg.OnDeliveryError = fn
	}
}

func withDeliveryPanicHandler[T any](
	fn asynxmd.PanicHandler[T],
) testOpt[T] {
	return func(
		c *testConfig[T],
	) {
		c.cfg.OnPanic = fn
	}
}

func withDeadLetterHandler[T any](
	fn asynxmd.DeadLetterHandler[T],
) testOpt[T] {
	return func(
		c *testConfig[T],
	) {
		c.cfg.DeadLetter = fn
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

func unjoin(err error) []error {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		return joined.Unwrap()
	}
	return []error{err}
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

func deliveredBeforeA1(
	name string,
) bool {
	return name != "a1-fail" && name != "b1"
}

func checkClosed(
	t *testing.T,
	err error,
) {
	t.Helper()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func checkNotDelivered(
	t *testing.T,
	v int,
	got error,
) {
	t.Helper()
	if got == nil {
		t.Errorf("v%d ended as delivered", v)
	}
}

func checkNotAcked(
	t *testing.T,
	acked *sync.Map,
	v int64,
) {
	t.Helper()
	if _, ok := acked.Load(v); ok {
		t.Errorf("v%d was acknowledged although v1 never succeeded", v)
	}
}

func notParked(
	err error,
) bool {
	return !errors.Is(err, asynxmd.ErrDeliveryParked)
}

func appendSubscriber(
	names []string,
	part error,
) []string {
	var sub *asynxmd.SubscriberError
	if !errors.As(part, &sub) {
		return names
	}
	return append(names, sub.Subscriber)
}
