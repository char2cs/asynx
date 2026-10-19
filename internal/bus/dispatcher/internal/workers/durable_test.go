package workers_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

// A failing live subscriber must not skip the durable delivery: the next
// event of the aggregate would otherwise overtake this one.
func TestEnqueue_DurableDeliveryStillAcksWhenBusRejectsEvent(t *testing.T) {
	var acked atomic.Bool
	var reported atomic.Int32
	d := newTestDispatcher(
		asynxmd.Bus[int](&errBus{err: errors.New("bus closed")}),
		withPublishErrorHandler[int](func(
			context.Context,
			asynxmd.Event[int],
			error,
		) {
			reported.Add(1)
		}),
	)
	defer d.Close(context.Background())

	ticket, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error {
		acked.Store(true)
		return nil
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := final(t, ticket, "final"); err != nil {
		t.Fatalf("delivery ended with %v", err)
	}
	if !acked.Load() {
		t.Error("ack did not run after the bus rejected the event")
	}
	if reported.Load() != 1 {
		t.Errorf("publish errors reported = %d, want 1", reported.Load())
	}
}

func TestDurable_NonDurableJobsSkipDurableSubscribers(t *testing.T) {
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		t.Error("durable handler ran for a job without an ack")
		return nil
	}}})
	done, _ := enqueueEvent(context.Background(), d, makeEvent("a", 1), nil)
	<-done
}

func TestBusErrorDoesNotSkipTheDurableDelivery(t *testing.T) {
	var rec deliveries
	d := newTestDispatcher[int](&errBus{err: errors.New("bus down")},
		withDurableSubs([]asynxmd.DurableSub[int]{{Match: always, Handler: func(
			_ context.Context,
			e asynxmd.Event[int],
		) error {
			rec.add(e)
			return nil
		}}}),
	)
	defer d.Close(context.Background())

	var acked sync.Map
	var last asynxmd.Ticket
	for v := int64(1); v <= 3; v++ {
		last, _ = enqueueTicket(context.Background(), d, makeEvent("a", v), ackInto(&acked, makeEvent("a", v)))
	}
	if err := final(t, last, "final"); err != nil {
		t.Fatalf("final = %v", err)
	}
	got := rec.list()
	inOrder := len(got) == 3 && got[0] == "a/1" && got[2] == "a/3"
	if !inOrder {
		t.Fatalf("durable deliveries = %v, want a/1 a/2 a/3 in order", got)
	}
}

func TestEnqueueRedelivery_GoesToTheDurableSubscribersOnly(t *testing.T) {
	bus := &recordingBus{}
	var seen deliveries
	d := newTestDispatcher[int](bus, withDurableSubs([]asynxmd.DurableSub[int]{{Match: func(name string) bool { return name == "test.event" },
		Handler: func(
			_ context.Context,
			e asynxmd.Event[int],
		) error {
			seen.add(e)
			return nil
		}}}))
	t.Cleanup(func() { d.Close(context.Background()) })
	var acked atomic.Int32
	ack := func(context.Context) error { acked.Add(1); return nil }
	ctx := context.Background()

	tk, _ := enqueueRedelivery(ctx, d, makeEvent("a", 1), ack)
	final(t, tk, "event")
	marker := makeEvent("a", 2)
	marker.EventName = asynxmd.ForgetEventName
	tk, _ = enqueueRedelivery(ctx, d, marker, ack)
	final(t, tk, "marker")

	if got := len(bus.snapshot()); got != 0 {
		t.Fatalf("live subscribers got %d events from redeliveries", got)
	}
	if got := seen.list(); !slices.Equal(got, []string{"a/1", "a/2"}) {
		t.Fatalf("durable subscriber saw %v, want the event and the marker (whatever its pattern)", got)
	}
	if acked.Load() != 2 {
		t.Fatalf("acked %d, want 2", acked.Load())
	}
}

func TestBusPanicDoesNotSkipTheDurableDelivery(t *testing.T) {
	var acked atomic.Bool
	var reported atomic.Int32
	d := newTestDispatcher[int](&panicBus{}, withDurableSubs([]asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		return nil
	}}}),
		withPublishErrorHandler[int](func(
			context.Context,
			asynxmd.Event[int],
			error,
		) {
			reported.Add(1)
		}))
	t.Cleanup(func() { d.Close(context.Background()) })
	tk, _ := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	if err := final(t, tk, "delivery"); err != nil {
		t.Fatalf("final = %v", err)
	}
	if !acked.Load() || reported.Load() != 1 {
		t.Fatalf("acked=%v reported=%d, want the panic reported and the delivery completed", acked.Load(), reported.Load())
	}
}

func TestPublishErrorHandlerPanicDoesNotCrashTheWorker(t *testing.T) {
	d := newTestDispatcher[int](&errBus{err: errors.New("down")}, withPublishErrorHandler[int](func(
		context.Context,
		asynxmd.Event[int],
		error,
	) {
		panic("boom")
	}))
	t.Cleanup(func() { d.Close(context.Background()) })
	done, err := enqueueEvent(context.Background(), d, makeEvent("a", 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	recv(t, done, "settled")
	done, _ = enqueueEvent(context.Background(), d, makeEvent("a", 2), nil)
	recv(t, done, "the worker survives")
}

// A job popped just before a Forget supersedes its aggregate belongs to the old
// generation: it must not run its retry loop ahead of the marker.
func TestSupersede_AfterThePopStillSkipsTheSupersededJob(t *testing.T) {
	bus := &gateBus{entered: make(chan struct{}), release: make(chan struct{})}
	var oldRuns atomic.Int32
	d := newTestDispatcher[int](bus, withDurableSubs([]asynxmd.DurableSub[int]{{Match: always, Handler: func(
		_ context.Context,
		e asynxmd.Event[int],
	) error {
		if e.EventName != asynxmd.ForgetEventName {
			oldRuns.Add(1)
			return errors.New("down")
		}
		return nil
	}}}), withRetryBackoff[int](50*time.Millisecond, 50*time.Millisecond), withMaxAttempts[int](5))
	t.Cleanup(func() { d.Close(context.Background()) })

	ctx := context.Background()
	enqueueTicket(ctx, d, makeEvent("a", 1), func(context.Context) error { return nil })
	recv(t, bus.entered, "v1 popped and publishing")

	d.Supersede("a")
	marker := makeEvent("a", 2)
	marker.EventName = asynxmd.ForgetEventName
	tk, _ := enqueueForget(ctx, d, marker, func(context.Context) error { return nil })
	close(bus.release)

	start := time.Now()
	if err := final(t, tk, "marker"); err != nil {
		t.Fatalf("marker = %v", err)
	}
	if time.Since(start) > 40*time.Millisecond || oldRuns.Load() != 0 {
		t.Fatalf("the superseded job ran %d attempts and held the marker back %v", oldRuns.Load(), time.Since(start))
	}
	if got := halted(d); len(got) != 0 {
		t.Fatalf("halted = %v", got)
	}
}

func TestAckFailure_TheAggregateIsParkedBeforeThePublishErrorHandlerIsTold(t *testing.T) {
	var d asynxmd.Dispatcher[int]
	parkedWhenTold := make(chan []string, 1)
	d = newDurable(t,
		[]asynxmd.DurableSub[int]{{Match: always, Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			return nil
		}}},
		withPublishErrorHandler[int](func(
			context.Context,
			asynxmd.Event[int],
			error,
		) {
			parkedWhenTold <- halted(d)
		}))

	enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return errors.New("store down") })
	if got := recv(t, parkedWhenTold, "the ack failure to be reported"); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("parked aggregates when the handler was told = %v, want [a]", got)
	}
}

// If the ack of v1 fails, v2 must not be delivered and acknowledged ahead of it.
func TestAckFailure_ParksTheAggregateSoALaterEventDoesNotOvertake(t *testing.T) {
	var rec deliveries
	var reported atomic.Int32
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		_ context.Context,
		e asynxmd.Event[int],
	) error {
		rec.add(e)
		return nil
	}}}, withPublishErrorHandler[int](func(
		context.Context,
		asynxmd.Event[int],
		error,
	) {
		reported.Add(1)
	}))

	ctx := context.Background()
	t1, _ := enqueueTicket(ctx, d, makeEvent("a", 1), func(context.Context) error { return errors.New("store down") })
	t2, _ := enqueueTicket(ctx, d, makeEvent("a", 2), func(context.Context) error { return nil })
	if err := final(t, t1, "v1"); err == nil {
		t.Fatal("v1 ended as acknowledged")
	}
	if err := final(t, t2, "v2"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v2 = %v, want ErrDeliveryParked", err)
	}
	if got := rec.list(); !slices.Equal(got, []string{"a/1"}) {
		t.Fatalf("delivered %v, want only v1", got)
	}
	if got := halted(d); !slices.Equal(got, []string{"a"}) || reported.Load() != 1 {
		t.Fatalf("halted=%v reported=%d", got, reported.Load())
	}
}
