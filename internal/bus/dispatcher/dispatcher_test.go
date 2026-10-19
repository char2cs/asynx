package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

func TestNew_WiresADispatcherThatDeliversThroughTheBus(t *testing.T) {
	bus := &recordingBus{}
	d := New(asynxmd.DispatcherConfig[int]{Bus: bus}, WithIdleTimeout[int](time.Minute))

	ticket, err := d.Enqueue(context.Background(), asynxmd.DispatchRequest[int]{Event: makeEvent("a", 1)})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := final(t, ticket, "final"); err != nil {
		t.Fatalf("final = %v", err)
	}
	if err := d.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := bus.snapshot(); len(got) != 1 {
		t.Fatalf("bus got %d events, want 1", len(got))
	}
	if d.(*dispatcher[int]).idleTimeout != time.Minute {
		t.Fatal("New did not apply its options")
	}
}

// WaitIdle returns once the job is handled, and the worker exits after its
// idle timeout. Dispatching again then starts a new worker.
func TestDispatch_IdleCleanup(t *testing.T) {
	bus := &recordingBus{}
	d := newTestDispatcher[int](bus, withIdleTimeout[int](10*time.Millisecond))
	ctx := context.Background()

	_ = dispatch(ctx, d, makeEvent("agg-1", 1), true)

	d.WaitIdle()
	d.workers.Wait()

	exists := d.registry.Wake("agg-1") != nil

	if exists {
		t.Error("expected queue to be cleaned up after idle timeout")
	}

	if err := dispatch(ctx, d, makeEvent("agg-1", 2), true); err != nil {
		t.Fatalf("dispatch after idle cleanup: %v", err)
	}

	calls := bus.snapshot()
	if len(calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(calls))
	}

	_ = d.Close(ctx)
}

func TestClose_DrainsAllEvents(t *testing.T) {
	bus := &recordingBus{}
	d := newTestDispatcher[int](bus)
	ctx := context.Background()

	for i := int64(1); i <= 20; i++ {
		_ = dispatch(ctx, d, makeEvent("agg-1", i), false)
	}

	if err := d.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	calls := bus.snapshot()
	if len(calls) != 20 {
		t.Fatalf("expected 20 calls, got %d", len(calls))
	}
	for i, e := range calls {
		checkCallVersion(t, i, e)
	}
}

// A second Close returns without panicking.
func TestClose_Idempotent(t *testing.T) {
	bus := &recordingBus{}
	d := newTestDispatcher[int](bus)
	ctx := context.Background()

	if err := d.Close(ctx); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := d.Close(ctx); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestDispatch_AfterClose(t *testing.T) {
	bus := &recordingBus{}
	d := newTestDispatcher[int](bus)
	ctx := context.Background()

	_ = d.Close(ctx)

	err := dispatch(ctx, d, makeEvent("agg-1", 1), false)
	if !errors.Is(err, asynxmd.ErrDispatcherClosed) {
		t.Errorf("expected ErrDispatcherClosed, got %v", err)
	}
}

// Close gives up when its context expires while the bus blocks forever in
// PublishSync.
func TestClose_ContextTimeout(t *testing.T) {
	started := make(chan struct{})
	bus := &callbackBus{fn: func(
		_ context.Context,
		_ asynxmd.Event[int],
	) error {
		close(started)
		select {}
	}}

	d := newTestDispatcher[int](bus)
	ctx := context.Background()

	_ = dispatch(ctx, d, makeEvent("agg-1", 1), false)

	<-started

	closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := d.Close(closeCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

// WaitIdle returns once the job is handled; workers.Wait waits for the
// goroutine to exit after its idle timeout.
func TestDispatch_WithIdleTimeoutOption(t *testing.T) {
	bus := &recordingBus{}
	d := newTestDispatcher[int](bus, withIdleTimeout[int](10*time.Millisecond))
	ctx := context.Background()

	_ = dispatch(ctx, d, makeEvent("agg-1", 1), true)

	d.WaitIdle()
	d.workers.Wait()

	if d.registry.Wake("agg-1") != nil {
		t.Errorf("expected 0 queues after idle timeout, got the queue of agg-1")
	}

	_ = d.Close(ctx)
}

// Enqueue never blocks on a slow handler, because callers enqueue while
// holding their per-aggregate command lock.
func TestEnqueue_NeverBlocksBehindASlowHandler(t *testing.T) {
	release := make(chan struct{})
	var mu sync.Mutex
	var seen []int64
	bus := &callbackBus{fn: func(
		_ context.Context,
		e asynxmd.Event[int],
	) error {
		<-release
		mu.Lock()
		seen = append(seen, e.Version)
		mu.Unlock()
		return nil
	}}
	d := newTestDispatcher[int](bus)
	defer d.Close(context.Background())

	const n = 200
	enqueued := make(chan struct{})
	go func() {
		for i := int64(1); i <= n; i++ {
			if _, err := enqueueEvent(context.Background(), d, makeEvent("a", i), nil); err != nil {
				t.Errorf("Enqueue: %v", err)
			}
		}
		close(enqueued)
	}()

	select {
	case <-enqueued:
	case <-time.After(2 * time.Second):
		t.Fatal("Enqueue blocked behind a slow handler")
	}

	close(release)
	d.WaitIdle()

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != n {
		t.Fatalf("handled %d events, want %d", len(seen), n)
	}
	for i, v := range seen {
		checkOrderAt(t, i, v)
	}
}

// Close drains the two jobs queued behind the one inside its handler. Once
// Interrupted closes, Close has closed the registry and is waiting for the
// workers.
func TestClose_DrainsJobsQueuedBehindASlowHandler(t *testing.T) {
	release := make(chan struct{})
	running := make(chan struct{}, 3)
	var handled atomic.Int32
	bus := &callbackBus{fn: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		running <- struct{}{}
		<-release
		handled.Add(1)
		return nil
	}}
	d := newTestDispatcher[int](bus)

	for i := int64(1); i <= 3; i++ {
		enqueueOrFail(t, d, i)
	}
	<-running

	closed := make(chan error, 1)
	go func() { closed <- d.Close(context.Background()) }()
	<-d.Interrupted()
	select {
	case err := <-closed:
		t.Fatalf("Close returned (%v) before the queued jobs drained", err)
	default:
	}
	close(release)

	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if handled.Load() != 3 {
		t.Fatalf("handled %d jobs, want 3 (Close must drain)", handled.Load())
	}
}

func TestTicketFor_FindsAnEventWhileItsDeliveryIsUnderWay(t *testing.T) {
	bus := &blockingBus{entered: make(chan struct{}, 4), release: make(chan struct{})}
	d := newTestDispatcher[int](bus)
	defer d.Close(context.Background())

	if _, found := d.TicketFor("a", 1); found {
		t.Fatal("TicketFor found an event that was never queued")
	}

	enqueued, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	<-bus.entered

	ticket, found := d.TicketFor("a", 1)
	if !found {
		t.Fatal("TicketFor lost an event whose handler is running")
	}
	if ticket.Settled != enqueued.Settled || ticket.Final != enqueued.Final {
		t.Fatal("TicketFor returned a different ticket than the one Enqueue returned")
	}
	if _, found := d.TicketFor("a", 2); found {
		t.Fatal("TicketFor matched another version")
	}
	if _, found := d.TicketFor("b", 1); found {
		t.Fatal("TicketFor matched another aggregate")
	}

	close(bus.release)
	<-ticket.Final
	waitFor(t, "TicketFor kept an event whose delivery is over", func() bool {
		_, found := d.TicketFor("a", 1)
		return !found
	})
}

func TestTicketFor_ForgetsEventsLeftBehindByAPark(t *testing.T) {
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		return nil
	}}})
	d.Park("a", errImposed)

	ticket, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	<-ticket.Final
	waitFor(t, "TicketFor kept an event that was never queued because its aggregate is parked", func() bool {
		_, found := d.TicketFor("a", 1)
		return !found
	})
}

func TestUnrun_RemembersEventsLeftUndeliveredByAPark(t *testing.T) {
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		return nil
	}}})
	d.Park("a", errImposed)

	ticket, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	<-ticket.Final

	if err := d.Unrun("a", 1); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("Unrun(a, 1) = %v, want ErrDeliveryParked", err)
	}
	if err := d.Unrun("a", 2); err != nil {
		t.Fatalf("Unrun of an event nobody enqueued = %v, want nil", err)
	}
	if err := d.Unrun("b", 1); err != nil {
		t.Fatalf("Unrun of another aggregate = %v, want nil", err)
	}

	resume(d, "a")
	if err := d.Unrun("a", 1); err != nil {
		t.Fatalf("Unrun after the park lifted = %v, want nil", err)
	}
}

func TestUnrun_RemembersQueuedEventsThatParkingTookOutOfMemory(t *testing.T) {
	bus := &blockingBus{entered: make(chan struct{}, 4), release: make(chan struct{})}
	d := newTestDispatcher[int](bus)
	defer d.Close(context.Background())
	ack := func(context.Context) error { return nil }
	if _, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), ack); err != nil {
		t.Fatal(err)
	}
	<-bus.entered
	queued, err := enqueueTicket(context.Background(), d, makeEvent("a", 2), ack)
	if err != nil {
		t.Fatal(err)
	}

	d.Park("a", errImposed)
	<-queued.Final
	close(bus.release)

	if err := d.Unrun("a", 2); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("Unrun(a, 2) = %v, want ErrDeliveryParked", err)
	}
	if err := d.Unrun("a", 1); err != nil {
		t.Fatalf("Unrun of the event whose handlers ran = %v, want nil", err)
	}
}

func TestParkUndelivered_RemembersTheNamedVersionsUntilTheParkLifts(t *testing.T) {
	d := newDurable(t, nil)

	d.Park("a", errImposed, 3, 5)

	if len(halted(d)) != 1 {
		t.Fatalf("halted = %v, want the aggregate parked", halted(d))
	}
	for _, v := range []int64{3, 5} {
		checkUnrunParked(t, d, v)
	}
	if err := d.Unrun("a", 4); err != nil {
		t.Errorf("Unrun(a, 4) = %v, want nil", err)
	}
	resume(d, "a")
	if err := d.Unrun("a", 3); err != nil {
		t.Errorf("Unrun after Resume = %v, want nil", err)
	}
}

// An event whose handlers ran and failed until the attempts ran out is not
// unrun, because a handler took it.
func TestUnrun_DoesNotCountAnEventWhoseHandlersRanAndFailed(t *testing.T) {
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		return errors.New("down")
	}}},
		withMaxAttempts[int](1))

	ticket, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	<-ticket.Final
	waitFor(t, "the aggregate to park", func() bool { return len(halted(d)) == 1 })

	if err := d.Unrun("a", 1); err != nil {
		t.Fatalf("Unrun = %v, want nil: the handler ran", err)
	}
}

func TestTicketFor_ForgetsQueuedEventsWhenTheyAreParked(t *testing.T) {
	bus := &blockingBus{entered: make(chan struct{}, 4), release: make(chan struct{})}
	d := newTestDispatcher[int](bus)
	defer d.Close(context.Background())
	ack := func(context.Context) error { return nil }

	if _, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), ack); err != nil {
		t.Fatal(err)
	}
	<-bus.entered
	queued, err := enqueueTicket(context.Background(), d, makeEvent("a", 2), ack)
	if err != nil {
		t.Fatal(err)
	}

	d.Park("a", errImposed)
	<-queued.Final
	waitFor(t, "TicketFor kept a queued event that parking took out of memory", func() bool {
		_, found := d.TicketFor("a", 2)
		return !found
	})
	close(bus.release)
}

// WaitIdle waits for what was queued when it was called and refuses nothing:
// commands keep enqueueing while it waits.
func TestWaitIdle_DoesNotRefuseEnqueuesWhileItWaits(t *testing.T) {
	bus := &blockingBus{entered: make(chan struct{}, 4), release: make(chan struct{})}
	d := newTestDispatcher[int](bus)
	defer d.Close(context.Background())

	first, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	<-bus.entered

	idle := make(chan struct{})
	go func() { d.WaitIdle(); close(idle) }()

	later, err := enqueueTicket(context.Background(), d, makeEvent("b", 1), nil)
	if err != nil {
		t.Fatalf("an enqueue during WaitIdle was refused: %v", err)
	}

	select {
	case <-idle:
		t.Fatal("WaitIdle returned while an event it should wait for was still being handled")
	default:
	}

	close(bus.release)
	<-idle
	<-first.Final
	<-later.Final
}

func TestWaitIdle_ReturnsAtOnceWhenNothingIsQueued(t *testing.T) {
	d := newTestDispatcher[int](&recordingBus{})
	defer d.Close(context.Background())
	d.WaitIdle()
}

// Interrupt leaves the dispatcher open: a non-durable event is still
// delivered normally afterwards.
func TestInterrupt_ReleasesWaitersWithoutClosingTheDispatcher(t *testing.T) {
	attempted := make(chan struct{}, 8)
	d := newTestDispatcher[int](&recordingBus{},
		withDurableSubs([]asynxmd.DurableSub[int]{{Match: always, Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			attempted <- struct{}{}
			return errors.New("poison")
		}}}),
		withRetryBackoff[int](time.Hour, time.Hour),
	)
	defer d.Close(context.Background())

	tk, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	recv(t, attempted, "first attempt")

	d.Interrupt()
	if got := final(t, tk, "final"); !errors.Is(got, asynxmd.ErrDispatcherClosed) {
		t.Fatalf("final = %v, want ErrDispatcherClosed", got)
	}
	done, err := enqueueEvent(context.Background(), d, makeEvent("b", 1), nil)
	if err != nil {
		t.Fatalf("Enqueue after Interrupt: %v", err)
	}
	recv(t, done, "live event after Interrupt")
}

func TestClose_CalledAgainStillWaitsForTheWorkers(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		close(started)
		<-release
		return nil
	}}})
	enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return nil })
	recv(t, started, "handler running")

	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := d.Close(short); err == nil {
		t.Fatal("first Close succeeded with a handler running")
	}
	short2, cancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel2()
	if err := d.Close(short2); err == nil {
		t.Fatal("second Close claimed success while the handler still runs")
	}
	close(release)
	if err := d.Close(context.Background()); err != nil {
		t.Fatalf("Close after the handler finished: %v", err)
	}
}

func TestTicket_UnrunSaysWhyNoHandlerRan(t *testing.T) {
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		return nil
	}}})
	ctx := context.Background()
	ack := func(context.Context) error { return nil }

	ran, _ := enqueueTicket(ctx, d, makeEvent("a", 1), ack)
	final(t, ran, "ran")
	if err := ran.Unrun(); err != nil {
		t.Fatalf("Unrun = %v for a delivered event", err)
	}

	d.Park("a", errImposed)
	parked, _ := enqueueTicket(ctx, d, makeEvent("a", 2), ack)
	recv(t, parked.Settled, "settled")
	if err := parked.Unrun(); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("Unrun = %v, want ErrDeliveryParked", err)
	}
}

func TestHandlerContext_MarksItsAggregateAndEndsWithTheDelivery(t *testing.T) {
	var got context.Context
	var d, other *dispatcher[int]
	d = newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		ctx context.Context,
		e asynxmd.Event[int],
	) error {
		got = ctx
		if other.InHandler(ctx, "a") {
			t.Error("another dispatcher's InHandler is true for this handler's context: same ID, different instance")
		}
		if !d.InHandler(ctx, "a") || d.InHandler(ctx, "b") {
			t.Errorf("InHandler(a)=%v InHandler(b)=%v", d.InHandler(ctx, "a"), d.InHandler(ctx, "b"))
		}
		return nil
	}}})
	other = newDurable(t, nil)
	tk, _ := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return nil })
	final(t, tk, "final")
	if got.Err() == nil {
		t.Fatal("handler context still live after the delivery ended")
	}
	if d.InHandler(context.Background(), "a") {
		t.Fatal("InHandler true for a plain context")
	}
}

func TestClose_GivingUpCancelsRunningHandlers(t *testing.T) {
	started := make(chan context.Context, 1)
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		ctx context.Context,
		_ asynxmd.Event[int],
	) error {
		started <- ctx
		<-ctx.Done()
		return ctx.Err()
	}}}, withMaxAttempts[int](1))
	enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return nil })
	hctx := recv(t, started, "handler running")

	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := d.Close(short); err == nil {
		t.Fatal("Close succeeded with a handler still running")
	}
	recv(t, hctx.Done(), "handler context cancelled")
}

func TestInterrupted_ClosesWhenTheDispatcherIsInterrupted(t *testing.T) {
	d := newTestDispatcher[int](&recordingBus{})
	defer d.Close(context.Background())

	select {
	case <-d.Interrupted():
		t.Fatal("Interrupted closed before Interrupt")
	default:
	}
	d.Interrupt()
	select {
	case <-d.Interrupted():
	default:
		t.Fatal("Interrupted did not close after Interrupt")
	}
}

func TestResume_LiftsAParkAndDeliversAgain(t *testing.T) {
	var healthy atomic.Bool
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		if healthy.Load() {
			return nil
		}
		return errors.New("down")
	}}}, withMaxAttempts[int](2))

	var acked sync.Map
	tk, _ := enqueueTicket(context.Background(), d, makeEvent("a", 1), ackInto(&acked, makeEvent("a", 1)))
	final(t, tk, "parked")

	healthy.Store(true)
	resume(d, "a")
	tk, err := enqueueRedelivery(context.Background(), d, makeEvent("a", 1), ackInto(&acked, makeEvent("a", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if err := final(t, tk, "redelivery"); err != nil {
		t.Fatalf("redelivery = %v", err)
	}
	if _, ok := acked.Load(int64(1)); !ok {
		t.Fatal("redelivered event was not acknowledged")
	}
	if len(halted(d)) != 0 {
		t.Fatalf("still halted: %v", halted(d))
	}
}

// The tombstone reaches a subscriber whose pattern does not match it.
func TestSupersede_LetsTheTombstoneThroughAFailingDelivery(t *testing.T) {
	var seen deliveries
	attempted := make(chan struct{}, 8)
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: func(name string) bool { return name == "test.event" },
		Handler: func(
			_ context.Context,
			e asynxmd.Event[int],
		) error {
			seen.add(e)
			if e.EventName == "test.event" {
				attempted <- struct{}{}
				return errors.New("poison")
			}
			return nil
		}}}, withRetryBackoff[int](time.Hour, time.Hour))

	var acked sync.Map
	ctx := context.Background()
	enqueueTicket(ctx, d, makeEvent("a", 1), ackInto(&acked, makeEvent("a", 1)))
	enqueueTicket(ctx, d, makeEvent("a", 2), ackInto(&acked, makeEvent("a", 2)))
	recv(t, attempted, "poison attempt")

	d.Supersede("a")
	tomb := makeEvent("a", 3)
	tomb.EventName = asynxmd.ForgetEventName
	tk, err := enqueueForget(ctx, d, tomb, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := final(t, tk, "tombstone"); err != nil {
		t.Fatalf("tombstone final = %v", err)
	}
	if len(halted(d)) != 0 {
		t.Fatalf("Supersede left the aggregate parked: %v", halted(d))
	}
	found := slices.Contains(seen.list(), "a/3")
	if !found {
		t.Fatalf("durable subscriber never saw the tombstone: %v", seen.list())
	}
}

func TestPark_OfAnAggregateWithoutAQueueStillBlocksItsDurableEvents(t *testing.T) {
	d := newDurable(t, nil)
	d.Park("ghost", errImposed)
	d.Park("ghost", errImposed)
	resume(d, "never-parked")

	var acked sync.Map
	tk, _ := enqueueTicket(context.Background(), d, makeEvent("ghost", 1), ackInto(&acked, makeEvent("ghost", 1)))
	if err := final(t, tk, "final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
	if _, ok := acked.Load(int64(1)); ok {
		t.Fatal("event of a parked aggregate was acknowledged")
	}

	resume(d, "ghost")
	tk, _ = enqueueTicket(context.Background(), d, makeEvent("ghost", 1), ackInto(&acked, makeEvent("ghost", 1)))
	if err := final(t, tk, "final after resume"); err != nil {
		t.Fatalf("final after resume = %v", err)
	}
}

func TestPark_KeepsLiveOnlyJobsQueuedAndDropsDurableOnes(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		_ context.Context,
		e asynxmd.Event[int],
	) error {
		if e.Version == 1 {
			started <- struct{}{}
			<-gate
		}
		return nil
	}}})
	ctx := context.Background()
	enqueueTicket(ctx, d, makeEvent("a", 1), func(context.Context) error { return nil })
	recv(t, started, "v1 running")
	durable, _ := enqueueTicket(ctx, d, makeEvent("a", 2), func(context.Context) error { return nil })
	live, _ := enqueueTicket(ctx, d, makeEvent("a", 3), nil)

	d.Park("a", errImposed)
	if err := final(t, durable, "durable ticket"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("durable final = %v, want ErrDeliveryParked", err)
	}
	close(gate)
	if err := final(t, live, "live ticket"); err != nil {
		t.Fatalf("live-only job final = %v, want delivered", err)
	}
}

func TestPark_QueuedDurableJobsReportThemselvesUnrun(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		_ context.Context,
		e asynxmd.Event[int],
	) error {
		if e.Version == 1 {
			started <- struct{}{}
			<-gate
		}
		return nil
	}}})
	ctx := context.Background()
	ack := func(context.Context) error { return nil }
	enqueueTicket(ctx, d, makeEvent("a", 1), ack)
	recv(t, started, "v1 running")
	queued, _ := enqueueTicket(ctx, d, makeEvent("a", 2), ack)

	d.Park("a", errImposed)
	recv(t, queued.Settled, "settled")
	if err := queued.Unrun(); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("Unrun = %v, want ErrDeliveryParked", err)
	}
	close(gate)
}

func TestResume_ReportsWhetherItLiftedAPark(t *testing.T) {
	d := newDurable(t, nil)
	if resume(d, "a") {
		t.Fatal("Resume of an aggregate that is not parked reported a lift")
	}
	d.Park("a", errImposed)
	if !resume(d, "a") || resume(d, "a") {
		t.Fatal("Resume must report the lift once")
	}
}

func TestResumeUndelivered_ReturnsWhatTheParkLeftUndeliveredAndForgetsIt(t *testing.T) {
	d := newDurable(t, nil)
	if lifted, undelivered := d.ResumeUndelivered("a", 1); lifted || undelivered != nil {
		t.Fatalf("not parked: got %v, %v", lifted, undelivered)
	}
	d.Park("a", errImposed, 7, 3, 5)
	lifted, undelivered := d.ResumeUndelivered("a", d.Parks()["a"])
	if !lifted || !slices.Equal(undelivered, []int64{3, 5, 7}) {
		t.Fatalf("ResumeUndelivered = %v, %v; want true, [3 5 7]", lifted, undelivered)
	}
	if err := d.Unrun("a", 3); err != nil {
		t.Fatalf("Unrun after the lift = %v, want nil", err)
	}
}

// What a caller read about a park is stale once the park has been lifted and
// imposed again: ResumeUndelivered leaves the new park, and what it left
// undelivered, alone.
func TestResumeUndelivered_LeavesAParkImposedAgainSinceItWasListed(t *testing.T) {
	d := newDurable(t, nil)
	d.Park("a", errImposed)
	listed := d.Parks()["a"]
	resume(d, "a")
	d.Park("a", errImposed, 4)

	if lifted, _ := d.ResumeUndelivered("a", listed); lifted {
		t.Fatal("lifted a park other than the one listed")
	}
	if got := halted(d); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("halted = %v, want a still parked", got)
	}
	if err := d.Unrun("a", 4); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("Unrun = %v, want what the new park left undelivered kept", err)
	}
	if again := d.Parks()["a"]; again == listed {
		t.Fatal("a new park reused the generation of the old one")
	}
}

type noopBus[T any] struct{}

func (b *noopBus[T]) Publish(_ context.Context, _ asynxmd.Event[T]) error     { return nil }
func (b *noopBus[T]) PublishSync(_ context.Context, _ asynxmd.Event[T]) error { return nil }
func (b *noopBus[T]) Subscribe(_ string, _ asynxmd.ProjectionHandler[T], _ ...asynxmd.SubscriptionOpt[T]) (string, error) {
	return "", nil
}
func (b *noopBus[T]) Unsubscribe(_ string) error    { return nil }
func (b *noopBus[T]) Close(_ context.Context) error { return nil }
func (b *noopBus[T]) WaitForHandlers()              {}

// BenchmarkDispatch_SingleAggregate benchmarks async dispatch where all events
// target a single aggregate "agg1".
func BenchmarkDispatch_SingleAggregate(b *testing.B) {
	d := newTestDispatcher[string](&noopBus[string]{})
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = dispatch(ctx, d, asynxmd.Event[string]{
			AggregateID: "agg1",
			EventName:   "BenchEvent",
			Version:     int64(i),
		}, false)
	}

	b.StopTimer()
	_ = d.Close(context.Background())
}

// BenchmarkDispatch_MultiAggregate benchmarks async dispatch spread across
// varying numbers of aggregates (10, 100, 1000).
func BenchmarkDispatch_MultiAggregate(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		n := n
		b.Run(fmt.Sprintf("aggs=%d", n), func(b *testing.B) {
			d := newTestDispatcher[string](&noopBus[string]{})
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				aggID := fmt.Sprintf("agg%d", i%n)
				_ = dispatch(ctx, d, asynxmd.Event[string]{
					AggregateID: aggID,
					EventName:   "BenchEvent",
					Version:     int64(i),
				}, false)
			}

			b.StopTimer()
			_ = d.Close(context.Background())
		})
	}
}

// BenchmarkDispatch_Parallel benchmarks concurrent async dispatch to "agg1"
// using RunParallel.
func BenchmarkDispatch_Parallel(b *testing.B) {
	d := newTestDispatcher[string](&noopBus[string]{})
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			_ = dispatch(ctx, d, asynxmd.Event[string]{
				AggregateID: "agg1",
				EventName:   "BenchEvent",
				Version:     i,
			}, false)
			i++
		}
	})

	b.StopTimer()
	_ = d.Close(context.Background())
}

// BenchmarkDispatch_Sync benchmarks synchronous dispatch (waitHandlers=true)
// where all events target "agg1".
func BenchmarkDispatch_Sync(b *testing.B) {
	d := newTestDispatcher[string](&noopBus[string]{})
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = dispatch(ctx, d, asynxmd.Event[string]{
			AggregateID: "agg1",
			EventName:   "BenchEvent",
			Version:     int64(i),
		}, true)
	}

	b.StopTimer()
	_ = d.Close(context.Background())
}

var (
	errImposed = errors.New("test: parked by the test")
	errDown    = errors.New("test: down")
	errAck     = errors.New("test: ack failed")
)

func failingSub(
	attempts *atomic.Int32,
) []asynxmd.DurableSub[int] {
	return []asynxmd.DurableSub[int]{{Name: "flaky", Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		attempts.Add(1)
		return errDown
	}}}
}

func TestInspect_GiveUpCauseNamesTheLastHandlerError(t *testing.T) {
	var attempts atomic.Int32
	d := newDurable(t, failingSub(&attempts), withMaxAttempts[int](3))

	tk, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(
		context.Context,
	) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	finalErr := final(t, tk, "the delivery to give up")

	got := d.Inspect("a")
	if !got.Parked || got.Generation != d.Parks()["a"] {
		t.Fatalf("Inspect = %+v, want parked with generation %d", got, d.Parks()["a"])
	}
	if !errors.Is(got.Cause, asynxmd.ErrDeliveryParked) || !errors.Is(got.Cause, errDown) {
		t.Fatalf("Cause = %v, want ErrDeliveryParked wrapping %v", got.Cause, errDown)
	}
	if !strings.Contains(got.Cause.Error(), "3 attempts") {
		t.Fatalf("Cause = %q, want the attempt count (3)", got.Cause)
	}
	if got.Cause != finalErr {
		t.Fatalf("Cause = %v, want the error the ticket's Final returned, %v", got.Cause, finalErr)
	}
}

func TestInspect_AckFailureCauseIsTheAckError(t *testing.T) {
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		return nil
	}}})

	tk, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(
		context.Context,
	) error {
		return errAck
	})
	if err != nil {
		t.Fatal(err)
	}
	final(t, tk, "the failed ack")

	got := d.Inspect("a")
	if !got.Parked || !errors.Is(got.Cause, errAck) {
		t.Fatalf("Inspect = %+v, want parked with %v", got, errAck)
	}
}

func TestInspect_InterruptCauseIsDispatcherClosed(t *testing.T) {
	var attempts atomic.Int32
	d := newDurable(t, failingSub(&attempts), withRetryBackoff[int](time.Hour, time.Hour))

	tk, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(
		context.Context,
	) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the first attempt", func() bool { return attempts.Load() == 1 })
	d.Interrupt()
	if err := final(t, tk, "the interrupted delivery"); !errors.Is(err, asynxmd.ErrDispatcherClosed) {
		t.Fatalf("final = %v, want ErrDispatcherClosed", err)
	}

	got := d.Inspect("a")
	if !got.Parked || !errors.Is(got.Cause, asynxmd.ErrDispatcherClosed) {
		t.Fatalf("Inspect = %+v, want parked with ErrDispatcherClosed", got)
	}
}

func TestInspect_ReportsTheCauseAndUndeliveredGivenToPark(t *testing.T) {
	d := newDurable(t, nil)
	if got := d.Inspect("a"); !reflect.DeepEqual(got, asynxmd.DeliveryStatus{}) {
		t.Fatalf("Inspect of an unknown aggregate = %+v, want the zero status", got)
	}
	d.Park("a", errImposed, 5, 3)
	got := d.Inspect("a")
	wrongInspect := !got.Parked || !errors.Is(got.Cause, errImposed) || !reflect.DeepEqual(got.Undelivered, []int64{3, 5})
	if wrongInspect {
		t.Fatalf("Inspect = %+v, want parked with %v and [3 5]", got, errImposed)
	}
	resume(d, "a")
	if got := d.Inspect("a"); !reflect.DeepEqual(got, asynxmd.DeliveryStatus{}) {
		t.Fatalf("Inspect after the lift = %+v, want the zero status", got)
	}
}

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

type blockingBus struct {
	recordingBus
	entered chan struct{}
	release chan struct{}
}

func (b *blockingBus) PublishSync(
	ctx context.Context,
	e asynxmd.Event[int],
) error {
	b.entered <- struct{}{}
	<-b.release
	return b.recordingBus.PublishSync(ctx, e)
}

type callbackBus struct {
	fn func(
		context.Context,
		asynxmd.Event[int],
	) error
}

func (b *callbackBus) PublishSync(
	ctx context.Context,
	e asynxmd.Event[int],
) error {
	return b.fn(ctx, e)
}

func (b *callbackBus) Publish(
	context.Context,
	asynxmd.Event[int],
) error {
	return nil
}

func (b *callbackBus) Subscribe(
	string,
	asynxmd.ProjectionHandler[int],
	...asynxmd.SubscriptionOpt[int],
) (string, error) {
	return "", nil
}

func (b *callbackBus) Unsubscribe(string) error { return nil }

func (b *callbackBus) Close(context.Context) error { return nil }

func (b *callbackBus) WaitForHandlers() {}

type recordingBus struct {
	mu    sync.Mutex
	calls []asynxmd.Event[int]
	delay time.Duration
}

func (b *recordingBus) PublishSync(
	_ context.Context,
	e asynxmd.Event[int],
) error {
	if b.delay > 0 {
		time.Sleep(b.delay)
	}
	b.mu.Lock()
	b.calls = append(b.calls, e)
	b.mu.Unlock()
	return nil
}

func (b *recordingBus) Publish(
	context.Context,
	asynxmd.Event[int],
) error {
	return nil
}

func (b *recordingBus) Subscribe(
	string,
	asynxmd.ProjectionHandler[int],
	...asynxmd.SubscriptionOpt[int],
) (string, error) {
	return "", nil
}

func (b *recordingBus) Unsubscribe(string) error { return nil }

func (b *recordingBus) Close(context.Context) error { return nil }

func (b *recordingBus) WaitForHandlers() {}

func (b *recordingBus) snapshot() []asynxmd.Event[int] {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]asynxmd.Event[int], len(b.calls))
	copy(out, b.calls)
	return out
}

type deliveries struct {
	mu   sync.Mutex
	seen []string
}

func (r *deliveries) add(e asynxmd.Event[int]) {
	r.mu.Lock()
	r.seen = append(r.seen, e.AggregateID+"/"+string(rune('0'+e.Version)))
	r.mu.Unlock()
}

func (r *deliveries) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.seen...)
}

type testConfig[T any] struct {
	cfg  asynxmd.DispatcherConfig[T]
	opts []Opt[T]
}

type testOpt[T any] func(
	c *testConfig[T],
)
