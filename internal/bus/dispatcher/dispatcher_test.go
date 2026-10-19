package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"runtime"
	"slices"
	"strconv"
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
	d.Park("a", errParkCause)

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
	d.Park("a", errParkCause)

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

	d.Park("a", errParkCause)
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

	d.Park("a", errParkCause, 3, 5)

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

	d.Park("a", errParkCause)
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

	d.Park("a", errParkCause)
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
	d.Park("ghost", errParkCause)
	d.Park("ghost", errParkCause)
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

	d.Park("a", errParkCause)
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

	d.Park("a", errParkCause)
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
	d.Park("a", errParkCause)
	if !resume(d, "a") || resume(d, "a") {
		t.Fatal("Resume must report the lift once")
	}
}

func TestResumeUndelivered_ReturnsWhatTheParkLeftUndeliveredAndForgetsIt(t *testing.T) {
	d := newDurable(t, nil)
	if lifted, undelivered := d.ResumeUndelivered("a", 1); lifted || undelivered != nil {
		t.Fatalf("not parked: got %v, %v", lifted, undelivered)
	}
	d.Park("a", errParkCause, 7, 3, 5)
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
	d.Park("a", errParkCause)
	listed := d.Parks()["a"]
	resume(d, "a")
	d.Park("a", errParkCause, 4)

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
	errParkCause   = errors.New("test: parked by the test")
	errHandlerDown = errors.New("test: down")
	errAckFailure  = errors.New("test: ack failed")
)

func failingSub(
	attempts *atomic.Int32,
) []asynxmd.DurableSub[int] {
	return []asynxmd.DurableSub[int]{{Name: "flaky", Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		attempts.Add(1)
		return errHandlerDown
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
	if !errors.Is(got.Cause, asynxmd.ErrDeliveryParked) || !errors.Is(got.Cause, errHandlerDown) {
		t.Fatalf("Cause = %v, want ErrDeliveryParked wrapping %v", got.Cause, errHandlerDown)
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
		return errAckFailure
	})
	if err != nil {
		t.Fatal(err)
	}
	final(t, tk, "the failed ack")

	got := d.Inspect("a")
	if !got.Parked || !errors.Is(got.Cause, errAckFailure) {
		t.Fatalf("Inspect = %+v, want parked with %v", got, errAckFailure)
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
	d.Park("a", errParkCause, 5, 3)
	got := d.Inspect("a")
	wrongInspect := !got.Parked || !errors.Is(got.Cause, errParkCause) || !reflect.DeepEqual(got.Undelivered, []int64{3, 5})
	if wrongInspect {
		t.Fatalf("Inspect = %+v, want parked with %v and [3 5]", got, errParkCause)
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

const patience = 5 * time.Second

// Run executes the full conformance suite against dispatchers built by
// factory, one subtest per scenario. Each scenario builds its own dispatchers
// from the factory with the DispatcherConfig it needs (a recording bus, its
// durable subscribers, its handlers, a fast retry backoff, an attempt cap) and
// closes them when it ends. The factory must return a fresh, independent
// dispatcher on every call; a factory error fails the scenario. Every wait is
// bounded, so a dispatcher that loses an event fails instead of hanging.
func Run(
	t *testing.T,
	factory asynxmd.DispatcherFactory[Aggregate],
) {
	t.Helper()
	h := harness{factory: factory, patience: patience}
	all := scenarios()
	for _, name := range slices.Sorted(maps.Keys(all)) {
		t.Run(name, h.subtest(all[name]))
	}
}

func scenarios() map[string]scenario {
	all := map[string]scenario{}
	maps.Copy(all, orderingScenarios())
	maps.Copy(all, durableScenarios())
	maps.Copy(all, parkingScenarios())
	maps.Copy(all, ticketScenarios())
	maps.Copy(all, lifecycleScenarios())
	maps.Copy(all, inspectScenarios())
	return all
}

type scenario func(
	t testing.TB,
	h harness,
)

// Aggregate is the aggregate state the suite's events carry: the dispatchers
// under test are models.Dispatcher[Aggregate], built by the factory given to
// Run. The suite never inspects it; it exists so that an implementation is
// exercised through a concrete instantiation of its generic type.
type Aggregate struct {
	Name string
}

type harness struct {
	factory  asynxmd.DispatcherFactory[Aggregate]
	patience time.Duration
}

func (h harness) subtest(
	check scenario,
) func(
	*testing.T,
) {
	return func(
		t *testing.T,
	) {
		check(t, h)
	}
}

func (h harness) build(
	t testing.TB,
	cfg asynxmd.DispatcherConfig[Aggregate],
) asynxmd.Dispatcher[Aggregate] {
	t.Helper()
	d, err := h.factory(cfg)
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	t.Cleanup(func() { h.shut(t, d) })
	return d
}

func (h harness) shut(
	t testing.TB,
	d asynxmd.Dispatcher[Aggregate],
) {
	if err := h.close(d); err != nil {
		t.Errorf("Close at cleanup: %v", err)
	}
}

func (h harness) close(
	d asynxmd.Dispatcher[Aggregate],
) error {
	ctx, cancel := context.WithTimeout(context.Background(), h.patience)
	defer cancel()
	return d.Close(ctx)
}

func (h harness) wait(
	t testing.TB,
	ch <-chan struct{},
	what string,
) {
	t.Helper()
	receive(t, h.patience, ch, what)
}

func (h harness) final(
	t testing.TB,
	tk asynxmd.Ticket,
	what string,
) error {
	t.Helper()
	h.wait(t, tk.Final, what)
	return tk.Err()
}

func (h harness) idle(
	t testing.TB,
	d asynxmd.Dispatcher[Aggregate],
) {
	t.Helper()
	done := make(chan struct{})
	go waitIdle(d, done)
	h.wait(t, done, "WaitIdle to return")
}

func (h harness) waitFor(
	t testing.TB,
	what string,
	cond func() bool,
) {
	t.Helper()
	deadline := time.Now().Add(h.patience)
	for !cond() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !cond() {
		t.Fatalf("timed out waiting for %s", what)
	}
}

func (h harness) awaitForgotten(
	t testing.TB,
	d asynxmd.Dispatcher[Aggregate],
	aggregateID string,
	version int64,
	what string,
) {
	t.Helper()
	h.waitFor(t, what, func() bool {
		_, found := d.TicketFor(aggregateID, version)
		return !found
	})
}

func (h harness) enqueueBounded(
	t testing.TB,
	d asynxmd.Dispatcher[Aggregate],
	r asynxmd.DispatchRequest[Aggregate],
) asynxmd.Ticket {
	t.Helper()
	tickets := make(chan asynxmd.Ticket, 1)
	go func() {
		tk, _ := d.Enqueue(context.Background(), r)
		tickets <- tk
	}()
	return receive(t, h.patience, tickets, "Enqueue to return")
}

type bus struct {
	fn func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error
	mu    sync.Mutex
	calls []asynxmd.Event[Aggregate]
}

func (b *bus) PublishSync(
	ctx context.Context,
	e asynxmd.Event[Aggregate],
) error {
	err := b.call(ctx, e)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls = append(b.calls, e)
	return err
}

func (b *bus) call(
	ctx context.Context,
	e asynxmd.Event[Aggregate],
) error {
	if b.fn == nil {
		return nil
	}
	return b.fn(ctx, e)
}

func (b *bus) published() []asynxmd.Event[Aggregate] {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]asynxmd.Event[Aggregate](nil), b.calls...)
}

func (b *bus) Publish(
	context.Context,
	asynxmd.Event[Aggregate],
) error {
	return nil
}

func (b *bus) Subscribe(
	string,
	asynxmd.ProjectionHandler[Aggregate],
	...asynxmd.SubscriptionOpt[Aggregate],
) (string, error) {
	return "", nil
}

func (b *bus) Unsubscribe(
	string,
) error {
	return nil
}

func (b *bus) Close(
	context.Context,
) error {
	return nil
}

func (b *bus) WaitForHandlers() {}

const eventName = "test.event"

func event(
	aggregateID string,
	version int64,
) asynxmd.Event[Aggregate] {
	return asynxmd.Event[Aggregate]{
		ID:          fmt.Sprintf("%s-v%d", aggregateID, version),
		AggregateID: aggregateID,
		EventName:   eventName,
		Version:     version,
		OccurredAt:  time.Now(),
	}
}

func marker(
	aggregateID string,
	version int64,
) asynxmd.Event[Aggregate] {
	e := event(aggregateID, version)
	e.EventName = asynxmd.ForgetEventName
	return e
}

func live(
	e asynxmd.Event[Aggregate],
) asynxmd.DispatchRequest[Aggregate] {
	return asynxmd.DispatchRequest[Aggregate]{Event: e}
}

func durable(
	e asynxmd.Event[Aggregate],
	ack func(
		context.Context,
	) error,
) asynxmd.DispatchRequest[Aggregate] {
	return asynxmd.DispatchRequest[Aggregate]{Event: e, Ack: ack}
}

func redelivery(
	e asynxmd.Event[Aggregate],
	ack func(
		context.Context,
	) error,
) asynxmd.DispatchRequest[Aggregate] {
	return asynxmd.DispatchRequest[Aggregate]{Event: e, Ack: ack, Kind: asynxmd.DispatchRedelivery}
}

func tombstone(
	e asynxmd.Event[Aggregate],
	ack func(
		context.Context,
	) error,
) asynxmd.DispatchRequest[Aggregate] {
	return asynxmd.DispatchRequest[Aggregate]{Event: e, Ack: ack, Kind: asynxmd.DispatchTombstone}
}

func mustEnqueue(
	t testing.TB,
	d asynxmd.Dispatcher[Aggregate],
	r asynxmd.DispatchRequest[Aggregate],
) asynxmd.Ticket {
	t.Helper()
	tk, err := d.Enqueue(context.Background(), r)
	if err != nil {
		t.Fatalf("Enqueue %s/%d: %v", r.Event.AggregateID, r.Event.Version, err)
	}
	return tk
}

func acknowledge(
	context.Context,
) error {
	return nil
}

func ackVersionInto(
	acked *sync.Map,
	version int64,
) func(
	context.Context,
) error {
	return func(
		context.Context,
	) error {
		acked.Store(version, true)
		return nil
	}
}

func failAck(
	err error,
) func(
	context.Context,
) error {
	return func(
		context.Context,
	) error {
		return err
	}
}

func config(
	b asynxmd.Bus[Aggregate],
	subs ...asynxmd.DurableSub[Aggregate],
) asynxmd.DispatcherConfig[Aggregate] {
	return asynxmd.DispatcherConfig[Aggregate]{
		Bus:          b,
		DurableSubs:  subs,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
		MaxAttempts:  20,
	}
}

func waitIdle(
	d asynxmd.Dispatcher[Aggregate],
	done chan<- struct{},
) {
	d.WaitIdle()
	close(done)
}

func waitIdleAfter(
	d asynxmd.Dispatcher[Aggregate],
	started chan<- struct{},
	done chan<- struct{},
) {
	close(started)
	waitIdle(d, done)
}

func enqueueAll(
	d asynxmd.Dispatcher[Aggregate],
	aggregateID string,
	n int64,
	refused *journal,
	done chan<- struct{},
) {
	for v := int64(1); v <= n; v++ {
		_, err := d.Enqueue(context.Background(), live(event(aggregateID, v)))
		noteError(refused, err)
	}
	close(done)
}

func enqueueOne(
	wg *sync.WaitGroup,
	d asynxmd.Dispatcher[Aggregate],
	r asynxmd.DispatchRequest[Aggregate],
	refused *journal,
) {
	defer wg.Done()
	_, err := d.Enqueue(context.Background(), r)
	noteError(refused, err)
}

func enqueueRound(
	t testing.TB,
	d asynxmd.Dispatcher[Aggregate],
	version int64,
	aggregateIDs ...string,
) {
	t.Helper()
	for _, id := range aggregateIDs {
		mustEnqueue(t, d, live(event(id, version)))
	}
}

func noteError(
	j *journal,
	err error,
) {
	if err == nil {
		return
	}
	j.note(err.Error())
}

var (
	errPoison = errors.New("dispatchertest: poison")
	errDown   = errors.New("dispatchertest: down")
)

func never(
	string,
) bool {
	return false
}

func onlyTestEvents(
	name string,
) bool {
	return name == eventName
}

func onlyForgetEvents(
	name string,
) bool {
	return name == asynxmd.ForgetEventName
}

func subscriber(
	handler asynxmd.DurableHandler[Aggregate],
) asynxmd.DurableSub[Aggregate] {
	return asynxmd.DurableSub[Aggregate]{Name: "sub", Match: always, Handler: handler}
}

func succeed(
	context.Context,
	asynxmd.Event[Aggregate],
) error {
	return nil
}

func fail(
	context.Context,
	asynxmd.Event[Aggregate],
) error {
	return errDown
}

func errorIf(
	cond bool,
	err error,
) error {
	if !cond {
		return nil
	}
	return err
}

func panicIf(
	cond bool,
	value any,
) {
	if !cond {
		return
	}
	panic(value)
}

func holdIf(
	cond bool,
	ch <-chan struct{},
) {
	if !cond {
		return
	}
	<-ch
}

func holdUpTo(
	ch <-chan struct{},
	patience time.Duration,
) {
	timer := time.NewTimer(patience)
	defer timer.Stop()
	select {
	case <-ch:
	case <-timer.C:
	}
}

func flagIf(
	cond bool,
	flag *atomic.Bool,
) {
	if !cond {
		return
	}
	flag.Store(true)
}

func countIf(
	cond bool,
	counter *atomic.Int32,
) {
	if !cond {
		return
	}
	counter.Add(1)
}

func sendIf[V any](
	cond bool,
	ch chan<- V,
	value V,
) {
	if !cond {
		return
	}
	select {
	case ch <- value:
	default:
	}
}

func firstTime(
	once *sync.Once,
) bool {
	first := false
	once.Do(func() { first = true })
	return first
}

func holdFirst(
	once *sync.Once,
	entered chan<- struct{},
	release <-chan struct{},
) {
	once.Do(func() {
		close(entered)
		<-release
	})
}

type journal struct {
	mu      sync.Mutex
	entries []string
}

func (j *journal) add(
	e asynxmd.Event[Aggregate],
) {
	j.note(fmt.Sprintf("%s/%d", e.AggregateID, e.Version))
}

func (j *journal) note(
	entry string,
) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.entries = append(j.entries, entry)
}

func (j *journal) list() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]string(nil), j.entries...)
}

func receive[V any](
	t testing.TB,
	patience time.Duration,
	ch <-chan V,
	what string,
) V {
	t.Helper()
	timer := time.NewTimer(patience)
	defer timer.Stop()
	select {
	case v := <-ch:
		return v
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", what)
	}
	var zero V
	return zero
}

func isClosed(
	ch <-chan struct{},
) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func parked(
	d asynxmd.Dispatcher[Aggregate],
) []string {
	return slices.Sorted(maps.Keys(d.Parks()))
}

func resumeIfParked(
	d asynxmd.Dispatcher[Aggregate],
	aggregateID string,
) bool {
	generation, isParked := d.Parks()[aggregateID]
	if !isParked {
		return false
	}
	lifted, _ := d.ResumeUndelivered(aggregateID, generation)
	return lifted
}

func versionsOf(
	events []asynxmd.Event[Aggregate],
) []int64 {
	versions := make([]int64, 0, len(events))
	for _, e := range events {
		versions = append(versions, e.Version)
	}
	return versions
}

func sequence(
	n int64,
) []int64 {
	out := make([]int64, 0, n)
	for v := int64(1); v <= n; v++ {
		out = append(out, v)
	}
	return out
}

func byAggregate(
	events []asynxmd.Event[Aggregate],
) map[string][]int64 {
	out := map[string][]int64{}
	for _, e := range events {
		out[e.AggregateID] = append(out[e.AggregateID], e.Version)
	}
	return out
}

func distinct(
	versions []int64,
) int {
	seen := map[int64]struct{}{}
	for _, v := range versions {
		seen[v] = struct{}{}
	}
	return len(seen)
}

func without(
	entries []string,
	prefix string,
) []string {
	return slices.DeleteFunc(slices.Clone(entries), func(
		entry string,
	) bool {
		return len(entry) >= len(prefix) && entry[:len(prefix)] == prefix
	})
}

func unjoin(
	err error,
) []error {
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		return []error{err}
	}
	return joined.Unwrap()
}

func subscriberNames(
	err error,
) []string {
	var names []string
	for _, part := range unjoin(err) {
		names = appendSubscriber(names, part)
	}
	return names
}

func appendSubscriber(
	names []string,
	err error,
) []string {
	var sub *asynxmd.SubscriberError
	if !errors.As(err, &sub) {
		return names
	}
	return append(names, sub.Subscriber)
}

func orderingScenarios() map[string]scenario {
	return map[string]scenario{
		"TestDispatch_OrderingGuarantee":                             dispatchOrderingGuarantee,
		"TestDispatch_CrossAggregateIndependence":                    dispatchCrossAggregateIndependence,
		"TestDispatch_SyncBlocking":                                  dispatchSyncBlocking,
		"TestDispatch_AsyncNonBlocking":                              dispatchAsyncNonBlocking,
		"TestDispatch_MultipleAggregatesOrdered":                     dispatchMultipleAggregatesOrdered,
		"TestDispatch_ConcurrentSameAggregate":                       dispatchConcurrentSameAggregate,
		"TestEnqueue_NeverBlocksBehindASlowHandler":                  enqueueNeverBlocksBehindASlowHandler,
		"TestDispatch_NilBusNoPanic":                                 dispatchNilBusNoPanic,
		"TestDispatch_PublishErrorHandler":                           dispatchPublishErrorHandler,
		"TestDurable_TimeoutIsReportedButTheHandlerIsStillWaitedFor": durableTimeoutIsStillWaitedFor,
	}
}

func dispatchOrderingGuarantee(
	t testing.TB,
	h harness,
) {
	b := &bus{}
	d := h.build(t, config(b))
	for v := int64(1); v <= 10; v++ {
		mustEnqueue(t, d, live(event("agg-1", v)))
	}
	if err := h.close(d); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := versionsOf(b.published()); !slices.Equal(got, sequence(10)) {
		t.Fatalf("published versions = %v, want 1..10 in order", got)
	}
}

func dispatchCrossAggregateIndependence(
	t testing.TB,
	h harness,
) {
	release := make(chan struct{})
	b := &bus{fn: func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		holdIf(e.AggregateID == "agg-slow", release)
		return nil
	}}
	d := h.build(t, config(b))
	t.Cleanup(func() { closeOnce(release) })

	mustEnqueue(t, d, live(event("agg-slow", 1)))
	fast := mustEnqueue(t, d, live(event("agg-fast", 1)))
	h.wait(t, fast.Final, "agg-fast to be delivered while agg-slow is held")
}

func dispatchSyncBlocking(
	t testing.TB,
	h harness,
) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	b := &bus{fn: func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		entered <- struct{}{}
		<-release
		return nil
	}}
	d := h.build(t, config(b))
	t.Cleanup(func() { closeOnce(release) })

	tk := mustEnqueue(t, d, live(event("agg-1", 1)))
	h.wait(t, entered, "the handler to start")
	if isClosed(tk.Settled) {
		t.Fatal("Settled closed while the handler was still running")
	}
	closeOnce(release)
	h.wait(t, tk.Settled, "Settled after the handler returned")
}

func dispatchAsyncNonBlocking(
	t testing.TB,
	h harness,
) {
	release := make(chan struct{})
	b := &bus{fn: func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		<-release
		return nil
	}}
	d := h.build(t, config(b))
	t.Cleanup(func() { closeOnce(release) })

	h.enqueueBounded(t, d, live(event("agg-1", 1)))
	h.enqueueBounded(t, d, live(event("agg-1", 2)))
}

func dispatchMultipleAggregatesOrdered(
	t testing.TB,
	h harness,
) {
	b := &bus{}
	d := h.build(t, config(b))
	for v := int64(1); v <= 5; v++ {
		enqueueRound(t, d, v, "a", "b", "c")
	}
	if err := h.close(d); err != nil {
		t.Fatalf("Close: %v", err)
	}
	want := map[string][]int64{"a": sequence(5), "b": sequence(5), "c": sequence(5)}
	if got := byAggregate(b.published()); !maps.EqualFunc(got, want, slices.Equal) {
		t.Fatalf("published per aggregate = %v, want 1..5 each in order", got)
	}
}

func dispatchConcurrentSameAggregate(
	t testing.TB,
	h harness,
) {
	const n = 100
	b := &bus{}
	d := h.build(t, config(b))
	var refused journal
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range int64(n) {
		go enqueueOne(&wg, d, live(event("agg-1", i+1)), &refused)
	}
	wg.Wait()
	if err := h.close(d); err != nil {
		t.Fatalf("Close: %v", err)
	}
	versions := versionsOf(b.published())
	wrongCount := len(versions) != n || distinct(versions) != n
	if wrongCount || len(refused.list()) != 0 {
		t.Fatalf("published %d events (%d distinct), refused %v; want %d once each", len(versions), distinct(versions), refused.list(), n)
	}
}

func enqueueNeverBlocksBehindASlowHandler(
	t testing.TB,
	h harness,
) {
	const n = 200
	release := make(chan struct{})
	t.Cleanup(func() { closeOnce(release) })
	b := &bus{fn: func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		<-release
		return nil
	}}
	d := h.build(t, config(b))
	var refused journal
	enqueued := make(chan struct{})
	go enqueueAll(d, "a", n, &refused, enqueued)

	h.wait(t, enqueued, "Enqueue to return while a handler is blocked")
	closeOnce(release)
	h.idle(t, d)
	if got := versionsOf(b.published()); !slices.Equal(got, sequence(n)) || len(refused.list()) != 0 {
		t.Fatalf("published %v (refused %v), want 1..%d in order", got, refused.list(), n)
	}
}

func dispatchNilBusNoPanic(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(nil))
	tk := mustEnqueue(t, d, live(event("agg-1", 1)))
	if err := h.final(t, tk, "the live event"); err != nil {
		t.Fatalf("final = %v", err)
	}
}

func dispatchPublishErrorHandler(
	t testing.TB,
	h harness,
) {
	publishErr := errors.New("publish failed")
	b := &bus{fn: func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		return publishErr
	}}
	reported := make(chan error, 4)
	events := make(chan asynxmd.Event[Aggregate], 4)
	cfg := config(b)
	cfg.OnPublishError = func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
		err error,
	) {
		sendIf(true, reported, err)
		sendIf(true, events, e)
	}
	d := h.build(t, cfg)

	mustEnqueue(t, d, live(event("agg-1", 1)))
	got := receive(t, h.patience, reported, "the publish error report")
	e := receive(t, h.patience, events, "the reported event")
	if !errors.Is(got, publishErr) || e.AggregateID != "agg-1" {
		t.Fatalf("reported %v for %q, want the publish error for agg-1", got, e.AggregateID)
	}
}

func durableTimeoutIsStillWaitedFor(
	t testing.TB,
	h harness,
) {
	var running, overlapped atomic.Bool
	var timeouts, acks atomic.Int32
	timedOut := make(chan struct{}, 8)
	sub := subscriber(func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		flagIf(!running.CompareAndSwap(false, true), &overlapped)
		<-timedOut
		running.Store(false)
		return nil
	})
	sub.Timeout = 10 * time.Millisecond
	cfg := config(&bus{}, sub)
	cfg.OnDeliveryError = func(
		_ context.Context,
		_ asynxmd.Event[Aggregate],
		err error,
		_ int,
	) {
		countIf(errors.Is(err, asynxmd.ErrHandlerTimeout), &timeouts)
		sendIf(errors.Is(err, asynxmd.ErrHandlerTimeout), timedOut, struct{}{})
	}
	d := h.build(t, cfg)
	ack := func(
		context.Context,
	) error {
		acks.Add(1)
		return nil
	}

	mustEnqueue(t, d, durable(event("a", 1), ack))
	mustEnqueue(t, d, durable(event("a", 2), ack))
	h.waitFor(t, "both deliveries to be acked", func() bool { return acks.Load() == 2 })
	if overlapped.Load() || timeouts.Load() != 2 {
		t.Fatalf("overlapped = %v, timeouts = %d; want no overlap and 2 timeouts", overlapped.Load(), timeouts.Load())
	}
}

func durableScenarios() map[string]scenario {
	return map[string]scenario{
		"TestEnqueue_AckRunsAfterHandlersFinish":                                 enqueueAckRunsAfterHandlersFinish,
		"TestEnqueue_AckErrorIsReported":                                         enqueueAckErrorIsReported,
		"TestEnqueue_DurableDeliveryStillAcksWhenBusRejectsEvent":                durableStillAcksWhenBusRejects,
		"TestDurable_FailedHandlerRetriesAndHoldsBackLaterEventsOfTheAggregate":  durableFailedHandlerRetriesAndHoldsBack,
		"TestDurable_DoneClosesAfterTheFirstAttemptEvenWhenItFails":              durableSettledAfterTheFirstAttempt,
		"TestDurable_PanicIsAFailureThatRetriesAndReachesThePanicHandler":        durablePanicIsAFailure,
		"TestDurable_APanickingPanicHandlerCannotKillTheWorker":                  durablePanickingPanicHandler,
		"TestDurable_OnlyFailedHandlersAreRetried":                               durableOnlyFailedHandlersAreRetried,
		"TestDurable_NonDurableJobsSkipDurableSubscribers":                       durableNonDurableJobsSkipDurableSubscribers,
		"TestDurable_DeliveryWithNoMatchingSubscriberIsStillAcked":               durableNoMatchingSubscriberIsStillAcked,
		"TestDurable_HandlerFinishingBeforeItsTimeoutReportsNothing":             durableHandlerWithinTimeoutReportsNothing,
		"TestDurable_ATimedOutHandlerThatThenFailsIsRetried":                     durableTimedOutThenFailedIsRetried,
		"TestDurable_FailuresNameTheSubscriberAndDeadLetterGetsThemAll":          durableFailuresNameTheSubscriber,
		"TestDurable_ATimeoutNamesItsSubscriber":                                 durableTimeoutNamesItsSubscriber,
		"TestBusErrorDoesNotSkipTheDurableDelivery":                              busErrorDoesNotSkipTheDurableDelivery,
		"TestBusPanicDoesNotSkipTheDurableDelivery":                              busPanicDoesNotSkipTheDurableDelivery,
		"TestDispatch_WorkerSurvivesPanic":                                       dispatchWorkerSurvivesPanic,
		"TestHandle_DispatcherPanicIsReportedNotSwallowed":                       handleBusPanicIsReported,
		"TestDeliveryErrorHandlerPanicDoesNotReleaseTheHold":                     deliveryErrorHandlerPanicKeepsTheHold,
		"TestPublishErrorHandlerPanicDoesNotCrashTheWorker":                      publishErrorHandlerPanicKeepsTheWorker,
		"TestEnqueueRedelivery_GoesToTheDurableSubscribersOnly":                  redeliveryGoesToTheDurableSubscribersOnly,
		"TestTombstone_ReachesEveryDurableSubscriber":                            tombstoneReachesEveryDurableSubscriber,
		"TestTombstone_WithANilAckRunsTheHandlersAndCallsNoAck":                  tombstoneWithANilAck,
		"TestRedelivery_OfAForgetMarkerWithANilAckRunsTheHandlers":               redeliveredMarkerWithANilAck,
		"TestDeadLetter_TheDeadLetteredEventIsAckedAndItsFinalClosesNil":         deadLetteredEventIsAcked,
		"TestEnqueue_AnEventNamedLikeTheTombstoneReachesOnlyMatchingSubscribers": eventNamedLikeATombstone,
		"TestRedelivery_OfAnOrdinaryEventWithANilAckRunsNoDurableHandler":        redeliveryWithANilAck,
	}
}

func enqueueAckRunsAfterHandlersFinish(
	t testing.TB,
	h harness,
) {
	var handled, acked, ackedEarly atomic.Bool
	gate := make(chan struct{})
	t.Cleanup(func() { closeOnce(gate) })
	b := &bus{fn: func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		<-gate
		handled.Store(true)
		return nil
	}}
	d := h.build(t, config(b))
	ack := func(
		context.Context,
	) error {
		ackedEarly.Store(!handled.Load())
		acked.Store(true)
		return nil
	}

	tk := mustEnqueue(t, d, durable(event("a", 1), ack))
	ranWhileBlocked := acked.Load()
	closeOnce(gate)
	h.wait(t, tk.Settled, "the delivery to settle")
	if ranWhileBlocked || ackedEarly.Load() {
		t.Fatal("ack ran before the handlers finished")
	}
	if err := h.final(t, tk, "the delivery"); err != nil || !acked.Load() {
		t.Fatalf("final = %v, acked = %v; want nil and acked", err, acked.Load())
	}
}

func enqueueAckErrorIsReported(
	t testing.TB,
	h harness,
) {
	ackErr := errors.New("ack failed")
	reported := make(chan error, 4)
	cfg := config(&bus{})
	cfg.OnPublishError = func(
		_ context.Context,
		_ asynxmd.Event[Aggregate],
		err error,
	) {
		sendIf(true, reported, err)
	}
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), failAck(ackErr)))
	if got := receive(t, h.patience, reported, "the ack failure report"); !errors.Is(got, ackErr) {
		t.Fatalf("reported %v, want the ack error", got)
	}
	if err := h.final(t, tk, "the delivery"); !errors.Is(err, ackErr) {
		t.Fatalf("final = %v, want the ack error", err)
	}
}

func durableStillAcksWhenBusRejects(
	t testing.TB,
	h harness,
) {
	var acked atomic.Bool
	var reported atomic.Int32
	b := &bus{fn: func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		return asynxmd.ErrBusClosed
	}}
	cfg := config(b)
	cfg.OnPublishError = func(
		context.Context,
		asynxmd.Event[Aggregate],
		error,
	) {
		reported.Add(1)
	}
	d := h.build(t, cfg)
	ack := func(
		context.Context,
	) error {
		acked.Store(true)
		return nil
	}

	tk := mustEnqueue(t, d, durable(event("a", 1), ack))
	if err := h.final(t, tk, "the delivery"); err != nil {
		t.Fatalf("final = %v, want nil", err)
	}
	if !acked.Load() || reported.Load() != 1 {
		t.Fatalf("acked = %v, reported = %d; want acked and one report", acked.Load(), reported.Load())
	}
}

func durableFailedHandlerRetriesAndHoldsBack(
	t testing.TB,
	h harness,
) {
	var order, attempts journal
	var failuresLeft, acks atomic.Int32
	failuresLeft.Store(3)
	var once sync.Once
	gate := make(chan struct{})
	t.Cleanup(func() { closeOnce(gate) })
	sub := subscriber(func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		holdIf(e.AggregateID == "a" && e.Version == 1 && firstTime(&once), gate)
		failing := e.AggregateID == "a" && e.Version == 1 && failuresLeft.Add(-1) >= 0
		order.note(label(e, failing))
		return errorIf(failing, errDown)
	})
	cfg := config(&bus{}, sub)
	cfg.OnDeliveryError = func(
		_ context.Context,
		_ asynxmd.Event[Aggregate],
		_ error,
		attempt int,
	) {
		attempts.note(strconv.Itoa(attempt))
	}
	d := h.build(t, cfg)
	ack := func(
		context.Context,
	) error {
		acks.Add(1)
		return nil
	}

	mustEnqueue(t, d, durable(event("a", 1), ack))
	mustEnqueue(t, d, durable(event("a", 2), ack))
	b1 := mustEnqueue(t, d, durable(event("b", 1), ack))
	closeOnce(gate)
	h.wait(t, b1.Final, "aggregate b, which a's failure must not hold back")
	h.waitFor(t, "all three deliveries to be acked", func() bool { return acks.Load() == 3 })

	want := []string{"a/1-fail", "a/1-fail", "a/1-fail", "a/1", "a/2"}
	if got := without(order.list(), "b/"); !slices.Equal(got, want) {
		t.Fatalf("deliveries of a = %v, want %v", got, want)
	}
	if got := attempts.list(); !slices.Equal(got, []string{"1", "2", "3"}) {
		t.Fatalf("delivery error attempts = %v, want [1 2 3]", got)
	}
}

func label(
	e asynxmd.Event[Aggregate],
	failing bool,
) string {
	name := e.AggregateID + "/" + strconv.FormatInt(e.Version, 10)
	if !failing {
		return name
	}
	return name + "-fail"
}

func durableSettledAfterTheFirstAttempt(
	t testing.TB,
	h harness,
) {
	release := make(chan struct{})
	t.Cleanup(func() { closeOnce(release) })
	var acked atomic.Bool
	d := h.build(t, config(&bus{}, subscriber(func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		return errorIf(!isClosed(release), errDown)
	})))
	ack := func(
		context.Context,
	) error {
		acked.Store(true)
		return nil
	}

	tk := mustEnqueue(t, d, durable(event("a", 1), ack))
	h.wait(t, tk.Settled, "Settled after the first, failed, attempt")
	if acked.Load() {
		closeOnce(release)
		t.Fatal("acked a delivery whose handler failed")
	}
	closeOnce(release)
	h.waitFor(t, "the ack after the retry succeeded", acked.Load)
}

func durablePanicIsAFailure(
	t testing.TB,
	h harness,
) {
	var attempts, panics atomic.Int32
	var acked atomic.Bool
	reported := make(chan error, 8)
	cfg := config(&bus{}, subscriber(func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		panicIf(attempts.Add(1) == 1, "boom")
		return nil
	}))
	cfg.OnPanic = func(
		context.Context,
		asynxmd.Event[Aggregate],
		any,
	) {
		panics.Add(1)
	}
	cfg.OnDeliveryError = func(
		_ context.Context,
		_ asynxmd.Event[Aggregate],
		err error,
		_ int,
	) {
		sendIf(true, reported, err)
	}
	d := h.build(t, cfg)
	ack := func(
		context.Context,
	) error {
		acked.Store(true)
		return nil
	}

	mustEnqueue(t, d, durable(event("a", 1), ack))
	h.waitFor(t, "the ack after a panic then a success", acked.Load)
	got := receive(t, h.patience, reported, "the panic report")
	if panics.Load() != 1 || !errors.Is(got, asynxmd.ErrHandlerPanic) {
		t.Fatalf("panic handler calls = %d, reported %v; want 1 and ErrHandlerPanic", panics.Load(), got)
	}
}

func durablePanickingPanicHandler(
	t testing.TB,
	h harness,
) {
	var attempts atomic.Int32
	var acked atomic.Bool
	cfg := config(&bus{}, subscriber(func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		panicIf(attempts.Add(1) == 1, "boom")
		return nil
	}))
	cfg.OnPanic = func(
		context.Context,
		asynxmd.Event[Aggregate],
		any,
	) {
		panic("handler of handlers")
	}
	d := h.build(t, cfg)
	ack := func(
		context.Context,
	) error {
		acked.Store(true)
		return nil
	}

	mustEnqueue(t, d, durable(event("a", 1), ack))
	h.waitFor(t, "recovery from a panicking panic handler", acked.Load)
}

func durableOnlyFailedHandlersAreRetried(
	t testing.TB,
	h harness,
) {
	var goodRuns, flakyRuns, strayRuns atomic.Int32
	var acked atomic.Bool
	good := subscriber(func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		goodRuns.Add(1)
		return nil
	})
	flaky := subscriber(func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		return errorIf(flakyRuns.Add(1) < 3, errDown)
	})
	stray := subscriber(func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		strayRuns.Add(1)
		return nil
	})
	stray.Match = never
	d := h.build(t, config(&bus{}, good, flaky, stray))
	ack := func(
		context.Context,
	) error {
		acked.Store(true)
		return nil
	}

	mustEnqueue(t, d, durable(event("a", 1), ack))
	h.waitFor(t, "the ack", acked.Load)
	wrongRetries := goodRuns.Load() != 1 || flakyRuns.Load() != 3
	if wrongRetries || strayRuns.Load() != 0 {
		t.Fatalf("good ran %d, flaky %d, non-matching %d; want 1, 3, 0", goodRuns.Load(), flakyRuns.Load(), strayRuns.Load())
	}
}

func durableNonDurableJobsSkipDurableSubscribers(
	t testing.TB,
	h harness,
) {
	var runs atomic.Int32
	d := h.build(t, config(&bus{}, subscriber(func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		runs.Add(1)
		return nil
	})))

	tk := mustEnqueue(t, d, live(event("a", 1)))
	if err := h.final(t, tk, "the live event"); err != nil || runs.Load() != 0 {
		t.Fatalf("final = %v, durable runs = %d; want nil and 0", err, runs.Load())
	}
}

func durableNoMatchingSubscriberIsStillAcked(
	t testing.TB,
	h harness,
) {
	var acked atomic.Bool
	d := h.build(t, config(&bus{}))
	ack := func(
		context.Context,
	) error {
		acked.Store(true)
		return nil
	}

	tk := mustEnqueue(t, d, durable(event("a", 1), ack))
	if err := h.final(t, tk, "the delivery"); err != nil || !acked.Load() {
		t.Fatalf("final = %v, acked = %v; want nil and acked", err, acked.Load())
	}
}

func durableHandlerWithinTimeoutReportsNothing(
	t testing.TB,
	h harness,
) {
	var reports atomic.Int32
	sub := subscriber(succeed)
	sub.Timeout = time.Minute
	cfg := config(&bus{}, sub)
	cfg.OnDeliveryError = func(
		context.Context,
		asynxmd.Event[Aggregate],
		error,
		int,
	) {
		reports.Add(1)
	}
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	if err := h.final(t, tk, "the delivery"); err != nil || reports.Load() != 0 {
		t.Fatalf("final = %v, reports = %d; want nil and 0", err, reports.Load())
	}
}

func durableTimedOutThenFailedIsRetried(
	t testing.TB,
	h harness,
) {
	var attempts atomic.Int32
	timedOut := make(chan struct{}, 4)
	sub := subscriber(func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		first := attempts.Add(1) == 1
		holdIf(first, timedOut)
		return errorIf(first, errDown)
	})
	sub.Timeout = 5 * time.Millisecond
	cfg := config(&bus{}, sub)
	cfg.OnDeliveryError = func(
		_ context.Context,
		_ asynxmd.Event[Aggregate],
		err error,
		_ int,
	) {
		sendIf(errors.Is(err, asynxmd.ErrHandlerTimeout), timedOut, struct{}{})
	}
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	if err := h.final(t, tk, "the delivery"); err != nil || attempts.Load() != 2 {
		t.Fatalf("final = %v, attempts = %d; want nil and 2", err, attempts.Load())
	}
}

func durableFailuresNameTheSubscriber(
	t testing.TB,
	h harness,
) {
	billing := errors.New("billing down")
	search := errors.New("search down")
	reported := make(chan error, 16)
	deadLettered := make(chan error, 1)
	first := asynxmd.DurableSub[Aggregate]{Name: "OrderUpdated#1", Match: always, Handler: func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		return billing
	}}
	second := asynxmd.DurableSub[Aggregate]{Name: "OrderUpdated#2", Match: always, Handler: func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		return search
	}}
	cfg := config(&bus{}, first, second)
	cfg.MaxAttempts = 2
	cfg.OnDeliveryError = func(
		_ context.Context,
		_ asynxmd.Event[Aggregate],
		err error,
		_ int,
	) {
		sendIf(true, reported, err)
	}
	cfg.DeadLetter = func(
		_ context.Context,
		_ asynxmd.Event[Aggregate],
		err error,
	) error {
		sendIf(true, deadLettered, err)
		return nil
	}
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	if err := h.final(t, tk, "the dead-lettered delivery"); err != nil {
		t.Fatalf("final = %v, want nil", err)
	}
	got := receive(t, h.patience, deadLettered, "the dead-letter handler")
	if !errors.Is(got, billing) || !errors.Is(got, search) {
		t.Fatalf("dead-letter handler learned of %q, want both failures", got)
	}
	if names := subscriberNames(got); !slices.Equal(names, []string{"OrderUpdated#1", "OrderUpdated#2"}) {
		t.Fatalf("subscribers named in the dead-letter error = %v", names)
	}
	firstReport := receive(t, h.patience, reported, "the first failure report")
	if names := subscriberNames(firstReport); !slices.Equal(names, []string{"OrderUpdated#1"}) {
		t.Fatalf("first delivery error = %v, want it to name OrderUpdated#1", firstReport)
	}
}

func durableTimeoutNamesItsSubscriber(
	t testing.TB,
	h harness,
) {
	release := make(chan struct{})
	t.Cleanup(func() { closeOnce(release) })
	reported := make(chan error, 4)
	sub := asynxmd.DurableSub[Aggregate]{Name: "slow#1", Match: always, Timeout: time.Millisecond, Handler: func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		<-release
		return nil
	}}
	cfg := config(&bus{}, sub)
	cfg.OnDeliveryError = func(
		_ context.Context,
		_ asynxmd.Event[Aggregate],
		err error,
		_ int,
	) {
		sendIf(true, reported, err)
	}
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	got := receive(t, h.patience, reported, "the timeout report")
	closeOnce(release)
	h.wait(t, tk.Final, "the delivery")
	if names := subscriberNames(got); !slices.Equal(names, []string{"slow#1"}) || !errors.Is(got, asynxmd.ErrHandlerTimeout) {
		t.Fatalf("timeout reported as %q, want ErrHandlerTimeout from slow#1", got)
	}
}

func busErrorDoesNotSkipTheDurableDelivery(
	t testing.TB,
	h harness,
) {
	var rec journal
	var acked sync.Map
	b := &bus{fn: func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		return errDown
	}}
	d := h.build(t, config(b, subscriber(func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		rec.add(e)
		return nil
	})))

	mustEnqueue(t, d, durable(event("a", 1), ackVersionInto(&acked, 1)))
	mustEnqueue(t, d, durable(event("a", 2), ackVersionInto(&acked, 2)))
	last := mustEnqueue(t, d, durable(event("a", 3), ackVersionInto(&acked, 3)))
	if err := h.final(t, last, "the last delivery"); err != nil {
		t.Fatalf("final = %v", err)
	}
	if got := rec.list(); !slices.Equal(got, []string{"a/1", "a/2", "a/3"}) {
		t.Fatalf("durable deliveries = %v, want a/1 a/2 a/3 in order", got)
	}
}

func busPanicDoesNotSkipTheDurableDelivery(
	t testing.TB,
	h harness,
) {
	var acked atomic.Bool
	var reported atomic.Int32
	b := &bus{fn: func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		panic("bus exploded")
	}}
	cfg := config(b, subscriber(succeed))
	cfg.OnPublishError = func(
		context.Context,
		asynxmd.Event[Aggregate],
		error,
	) {
		reported.Add(1)
	}
	d := h.build(t, cfg)
	ack := func(
		context.Context,
	) error {
		acked.Store(true)
		return nil
	}

	tk := mustEnqueue(t, d, durable(event("a", 1), ack))
	if err := h.final(t, tk, "the delivery"); err != nil {
		t.Fatalf("final = %v", err)
	}
	if !acked.Load() || reported.Load() != 1 {
		t.Fatalf("acked = %v, reported = %d; want the panic reported and the delivery completed", acked.Load(), reported.Load())
	}
}

func dispatchWorkerSurvivesPanic(
	t testing.TB,
	h harness,
) {
	var calls atomic.Int32
	b := &bus{fn: func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		panicIf(calls.Add(1) == 1, "boom")
		return nil
	}}
	d := h.build(t, config(b))

	first := mustEnqueue(t, d, live(event("agg-1", 1)))
	h.wait(t, first.Settled, "the panicking event")
	second := mustEnqueue(t, d, live(event("agg-1", 2)))
	h.wait(t, second.Final, "the event after the panic")
	if calls.Load() != 2 {
		t.Fatalf("PublishSync calls = %d, want 2", calls.Load())
	}
}

func handleBusPanicIsReported(
	t testing.TB,
	h harness,
) {
	reported := make(chan error, 4)
	b := &bus{fn: func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		panic("bus exploded")
	}}
	cfg := config(b)
	cfg.OnPublishError = func(
		_ context.Context,
		_ asynxmd.Event[Aggregate],
		err error,
	) {
		sendIf(true, reported, err)
	}
	d := h.build(t, cfg)

	mustEnqueue(t, d, live(event("a", 1)))
	if got := receive(t, h.patience, reported, "the bus panic report"); got == nil {
		t.Fatal("a panic in the bus was reported as a nil error")
	}
	next := mustEnqueue(t, d, live(event("a", 2)))
	h.wait(t, next.Final, "the worker to keep delivering")
}

func deliveryErrorHandlerPanicKeepsTheHold(
	t testing.TB,
	h harness,
) {
	var rec journal
	var failures atomic.Int32
	failures.Store(2)
	var once sync.Once
	gate := make(chan struct{})
	t.Cleanup(func() { closeOnce(gate) })
	cfg := config(&bus{}, subscriber(func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		rec.add(e)
		holdIf(e.Version == 1 && firstTime(&once), gate)
		return errorIf(e.Version == 1 && failures.Add(-1) >= 0, errDown)
	}))
	cfg.OnDeliveryError = func(
		context.Context,
		asynxmd.Event[Aggregate],
		error,
		int,
	) {
		panic("boom")
	}
	d := h.build(t, cfg)

	mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	last := mustEnqueue(t, d, durable(event("a", 2), acknowledge))
	closeOnce(gate)
	if err := h.final(t, last, "v2"); err != nil {
		t.Fatalf("v2 = %v", err)
	}
	if got := rec.list(); !slices.Equal(got, []string{"a/1", "a/1", "a/1", "a/2"}) {
		t.Fatalf("delivered %v: a panicking error handler released the hold", got)
	}
}

func publishErrorHandlerPanicKeepsTheWorker(
	t testing.TB,
	h harness,
) {
	b := &bus{fn: func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		return errDown
	}}
	cfg := config(b)
	cfg.OnPublishError = func(
		context.Context,
		asynxmd.Event[Aggregate],
		error,
	) {
		panic("boom")
	}
	d := h.build(t, cfg)

	first := mustEnqueue(t, d, live(event("a", 1)))
	h.wait(t, first.Settled, "the first event")
	second := mustEnqueue(t, d, live(event("a", 2)))
	h.wait(t, second.Settled, "the worker to survive")
}

func redeliveryGoesToTheDurableSubscribersOnly(
	t testing.TB,
	h harness,
) {
	b := &bus{}
	var seen journal
	var acked atomic.Int32
	sub := subscriber(func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		seen.add(e)
		return nil
	})
	sub.Match = onlyTestEvents
	d := h.build(t, config(b, sub))
	ack := func(
		context.Context,
	) error {
		acked.Add(1)
		return nil
	}

	h.final(t, mustEnqueue(t, d, redelivery(event("a", 1), ack)), "the redelivery")
	h.final(t, mustEnqueue(t, d, redelivery(marker("a", 2), ack)), "the redelivered marker")
	if got := len(b.published()); got != 0 {
		t.Fatalf("live subscribers got %d redelivered events", got)
	}
	if got := seen.list(); !slices.Equal(got, []string{"a/1", "a/2"}) || acked.Load() != 2 {
		t.Fatalf("durable subscriber saw %v, acked %d; want the event and the marker, both acked", got, acked.Load())
	}
}

func tombstoneReachesEveryDurableSubscriber(
	t testing.TB,
	h harness,
) {
	b := &bus{}
	var matching, other journal
	first := subscriber(func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		matching.add(e)
		return nil
	})
	first.Match = onlyTestEvents
	second := subscriber(func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		other.add(e)
		return nil
	})
	second.Match = never
	d := h.build(t, config(b, first, second))

	if err := h.final(t, mustEnqueue(t, d, tombstone(marker("a", 1), acknowledge)), "the tombstone"); err != nil {
		t.Fatalf("tombstone final = %v", err)
	}
	if !slices.Equal(matching.list(), []string{"a/1"}) || !slices.Equal(other.list(), []string{"a/1"}) {
		t.Fatalf("subscribers saw %v and %v, want the tombstone in both", matching.list(), other.list())
	}
	if got := len(b.published()); got != 1 {
		t.Fatalf("live subscribers got %d events, want the tombstone", got)
	}
}

func tombstoneWithANilAck(
	t testing.TB,
	h harness,
) {
	var seen journal
	sub := subscriber(func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		seen.add(e)
		return nil
	})
	sub.Match = never
	d := h.build(t, config(&bus{}, sub))

	if err := h.final(t, mustEnqueue(t, d, tombstone(marker("a", 1), nil)), "the tombstone"); err != nil {
		t.Fatalf("tombstone final = %v", err)
	}
	if got := seen.list(); !slices.Equal(got, []string{"a/1"}) {
		t.Fatalf("durable subscriber saw %v, want the tombstone", got)
	}
}

func redeliveredMarkerWithANilAck(
	t testing.TB,
	h harness,
) {
	b := &bus{}
	var seen journal
	sub := subscriber(func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		seen.add(e)
		return nil
	})
	sub.Match = never
	d := h.build(t, config(b, sub))

	if err := h.final(t, mustEnqueue(t, d, redelivery(marker("a", 1), nil)), "the redelivered marker"); err != nil {
		t.Fatalf("marker final = %v", err)
	}
	if got := seen.list(); !slices.Equal(got, []string{"a/1"}) || len(b.published()) != 0 {
		t.Fatalf("durable subscriber saw %v, live %d; want the marker durably only", got, len(b.published()))
	}
}

func deadLetteredEventIsAcked(
	t testing.TB,
	h harness,
) {
	var acked sync.Map
	var dead atomic.Int32
	cfg := config(&bus{}, subscriber(fail))
	cfg.MaxAttempts = 2
	cfg.DeadLetter = func(
		context.Context,
		asynxmd.Event[Aggregate],
		error,
	) error {
		dead.Add(1)
		return nil
	}
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), ackVersionInto(&acked, 1)))
	if err := h.final(t, tk, "the dead-lettered delivery"); err != nil {
		t.Fatalf("final = %v, want nil", err)
	}
	_, ok := acked.Load(int64(1))
	notSettled := !ok || dead.Load() != 1
	if notSettled || len(parked(d)) != 0 {
		t.Fatalf("acked = %v, dead letters = %d, parked = %v; want acked once, unparked", ok, dead.Load(), parked(d))
	}
}

func eventNamedLikeATombstone(
	t testing.TB,
	h harness,
) {
	var ordinary, forgets journal
	first := subscriber(func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		ordinary.add(e)
		return nil
	})
	first.Match = onlyTestEvents
	second := subscriber(func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		forgets.add(e)
		return nil
	})
	second.Match = onlyForgetEvents
	d := h.build(t, config(&bus{}, first, second))

	tk := mustEnqueue(t, d, durable(marker("a", 1), acknowledge))
	if err := h.final(t, tk, "the event"); err != nil {
		t.Fatalf("final = %v", err)
	}
	if len(ordinary.list()) != 0 || !slices.Equal(forgets.list(), []string{"a/1"}) {
		t.Fatalf("non-matching saw %v, matching saw %v; want a DispatchEvent to reach only the matching subscriber", ordinary.list(), forgets.list())
	}
}

func redeliveryWithANilAck(
	t testing.TB,
	h harness,
) {
	b := &bus{}
	var runs atomic.Int32
	d := h.build(t, config(b, subscriber(func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		runs.Add(1)
		return nil
	})))

	tk := mustEnqueue(t, d, redelivery(event("a", 1), nil))
	if err := h.final(t, tk, "the redelivery"); err != nil {
		t.Fatalf("final = %v", err)
	}
	if runs.Load() != 0 || len(b.published()) != 0 {
		t.Fatalf("durable runs %d, live %d; want neither for an ordinary redelivery without an ack", runs.Load(), len(b.published()))
	}
}

func parkingScenarios() map[string]scenario {
	return map[string]scenario{
		"TestExhaustedAttempts_ParkTheAggregateAndLeaveOthersAlone":             exhaustedAttemptsPark,
		"TestResume_LiftsAParkAndDeliversAgain":                                 resumeLiftsAPark,
		"TestResume_ReportsWhetherItLiftedAPark":                                resumeReportsTheLift,
		"TestDeadLetter_AcceptingTheEventSkipsItAndLetsLaterEventsFlow":         deadLetterAcceptance,
		"TestDeadLetter_RefusalParksTheAggregate":                               deadLetterRefusal,
		"TestDeadLetter_AReturnedErrorParksTheAggregate":                        deadLetterReturnedError,
		"TestDeadLetter_TheAggregateIsParkedBeforeTheRefusalIsReported":         deadLetterRefusalParksBeforeReporting,
		"TestSupersede_LetsATombstoneWithAnAckThroughARetryingDelivery":         supersedeLetsATombstoneWithAnAckThrough,
		"TestSupersede_LetsTheTombstoneThroughAFailingDelivery":                 supersedeLetsTheTombstoneThrough,
		"TestSupersede_AfterThePopStillSkipsTheSupersededJob":                   supersedeAfterThePop,
		"TestSupersede_LeavesTheAggregateUnparkedWithNoRecord":                  supersedeLeavesNoRecord,
		"TestTombstone_FinalReportsADurableSubscriberThatKeepsFailing":          tombstoneFinalReportsAFailure,
		"TestEnqueueForget_IsNeverDeadLettered":                                 forgetIsNeverDeadLettered,
		"TestGiveUp_AfterASupersedeDoesNotParkTheAggregateAgain":                giveUpAfterASupersede,
		"TestInterrupt_AfterASupersedeDoesNotParkTheAggregate":                  interruptAfterASupersede,
		"TestPark_OfAnAggregateWithoutAQueueStillBlocksItsDurableEvents":        parkWithoutAQueue,
		"TestPark_KeepsLiveOnlyJobsQueuedAndDropsDurableOnes":                   parkKeepsLiveOnlyJobs,
		"TestPark_QueuedDurableJobsReportThemselvesUnrun":                       parkQueuedJobsReportUnrun,
		"TestPark_OnAParkedAggregateKeepsItsGenerationAndMergesTheVersions":     parkMergesIntoAPark,
		"TestPark_AfterALiftImposesANewGeneration":                              parkAfterALift,
		"TestParkUndelivered_RemembersTheNamedVersionsUntilTheParkLifts":        parkRemembersTheNamedVersions,
		"TestGiveUp_TheAggregateIsParkedBeforeTheErrorHandlerIsTold":            giveUpParksBeforeReporting,
		"TestAckFailure_TheAggregateIsParkedBeforeThePublishErrorHandlerIsTold": ackFailureParksBeforeReporting,
		"TestAckFailure_ParksTheAggregateSoALaterEventDoesNotOvertake":          ackFailureParks,
		"TestResumeUndelivered_ReturnsWhatTheParkLeftUndeliveredAndForgetsIt":   resumeUndeliveredReturnsTheRecord,
		"TestResumeUndelivered_LeavesAParkImposedAgainSinceItWasListed":         resumeUndeliveredLeavesANewerPark,
	}
}

func exhaustedAttemptsPark(
	t testing.TB,
	h harness,
) {
	var attempts, parkedReports atomic.Int32
	var rec journal
	var acked sync.Map
	var once sync.Once
	gate := make(chan struct{})
	cfg := config(&bus{}, subscriber(func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		rec.add(e)
		poison := e.AggregateID == "a" && e.Version == 1
		holdIf(poison && firstTime(&once), gate)
		countIf(poison, &attempts)
		return errorIf(poison, errPoison)
	}))
	cfg.MaxAttempts = 4
	cfg.OnDeliveryError = func(
		_ context.Context,
		_ asynxmd.Event[Aggregate],
		err error,
		_ int,
	) {
		countIf(errors.Is(err, asynxmd.ErrDeliveryParked), &parkedReports)
	}
	d := h.build(t, cfg)
	t.Cleanup(func() { closeOnce(gate) })

	t1 := mustEnqueue(t, d, durable(event("a", 1), ackVersionInto(&acked, 1)))
	t2 := mustEnqueue(t, d, durable(event("a", 2), ackVersionInto(&acked, 2)))
	closeOnce(gate)
	if err := h.final(t, t1, "v1"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v1 final = %v, want ErrDeliveryParked", err)
	}
	if err := h.final(t, t2, "v2"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v2 final = %v, want ErrDeliveryParked", err)
	}
	if attempts.Load() != 4 || !slices.Equal(parked(d), []string{"a"}) {
		t.Fatalf("attempts = %d, parked = %v; want exactly the cap (4) and [a]", attempts.Load(), parked(d))
	}
	if slices.Contains(rec.list(), "a/2") {
		t.Fatal("v2 delivered although v1 was parked")
	}

	t3 := mustEnqueue(t, d, durable(event("a", 3), ackVersionInto(&acked, 3)))
	if err := h.final(t, t3, "v3"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v3 final = %v, want ErrDeliveryParked", err)
	}
	tb := mustEnqueue(t, d, durable(event("b", 1), ackVersionInto(&acked, 1)))
	if err := h.final(t, tb, "b"); err != nil {
		t.Fatalf("aggregate b held back: %v", err)
	}
	if parkedReports.Load() != 1 {
		t.Fatalf("parked reports = %d, want 1", parkedReports.Load())
	}
}

func resumeLiftsAPark(
	t testing.TB,
	h harness,
) {
	var healthy atomic.Bool
	var acked sync.Map
	cfg := config(&bus{}, subscriber(func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		return errorIf(!healthy.Load(), errDown)
	}))
	cfg.MaxAttempts = 2
	d := h.build(t, cfg)

	h.final(t, mustEnqueue(t, d, durable(event("a", 1), acknowledge)), "the parked delivery")
	healthy.Store(true)
	if !resumeIfParked(d, "a") {
		t.Fatal("the park was not lifted")
	}
	tk := mustEnqueue(t, d, redelivery(event("a", 1), ackVersionInto(&acked, 1)))
	if err := h.final(t, tk, "the redelivery"); err != nil {
		t.Fatalf("redelivery = %v", err)
	}
	if _, ok := acked.Load(int64(1)); !ok || len(parked(d)) != 0 {
		t.Fatalf("acked = %v, parked = %v; want the redelivery acked and nothing parked", ok, parked(d))
	}
}

func resumeReportsTheLift(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	if resumeIfParked(d, "a") {
		t.Fatal("lifted a park on an aggregate that is not parked")
	}
	d.Park("a", errImposed)
	if !resumeIfParked(d, "a") || resumeIfParked(d, "a") {
		t.Fatal("ResumeUndelivered must report the lift once")
	}
}

func deadLetterAcceptance(
	t testing.TB,
	h harness,
) {
	var rec journal
	var dead atomic.Int64
	var acked sync.Map
	var once sync.Once
	gate := make(chan struct{})
	cfg := config(&bus{}, subscriber(func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		rec.add(e)
		holdIf(e.Version == 1 && firstTime(&once), gate)
		return errorIf(e.Version == 1, errPoison)
	}))
	cfg.MaxAttempts = 3
	cfg.DeadLetter = func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
		_ error,
	) error {
		dead.Store(e.Version)
		return nil
	}
	d := h.build(t, cfg)
	t.Cleanup(func() { closeOnce(gate) })

	t1 := mustEnqueue(t, d, durable(event("a", 1), ackVersionInto(&acked, 1)))
	t2 := mustEnqueue(t, d, durable(event("a", 2), ackVersionInto(&acked, 2)))
	closeOnce(gate)
	if err := h.final(t, t1, "v1"); err != nil {
		t.Fatalf("dead-lettered event final = %v, want nil", err)
	}
	if err := h.final(t, t2, "v2"); err != nil {
		t.Fatalf("v2 final = %v", err)
	}
	_, ok := acked.Load(int64(1))
	notSettled := dead.Load() != 1 || !ok
	if notSettled || len(parked(d)) != 0 {
		t.Fatalf("dead letter saw v%d, v1 acked = %v, parked = %v; want v1, acked, nothing parked", dead.Load(), ok, parked(d))
	}
}

func deadLetterRefusal(
	t testing.TB,
	h harness,
) {
	cfg := config(&bus{}, subscriber(fail))
	cfg.MaxAttempts = 2
	cfg.DeadLetter = func(
		context.Context,
		asynxmd.Event[Aggregate],
		error,
	) error {
		panic("dead-letter store down")
	}
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	if err := h.final(t, tk, "the delivery"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
	if !slices.Equal(parked(d), []string{"a"}) {
		t.Fatalf("parked = %v, want [a]", parked(d))
	}
}

func deadLetterReturnedError(
	t testing.TB,
	h harness,
) {
	var rec journal
	var acked sync.Map
	var dead atomic.Int32
	var once sync.Once
	gate := make(chan struct{})
	cfg := config(&bus{}, subscriber(func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		rec.add(e)
		holdIf(e.Version == 1 && firstTime(&once), gate)
		return errorIf(e.Version == 1, errPoison)
	}))
	cfg.MaxAttempts = 2
	cfg.DeadLetter = func(
		context.Context,
		asynxmd.Event[Aggregate],
		error,
	) error {
		dead.Add(1)
		return errDown
	}
	d := h.build(t, cfg)
	t.Cleanup(func() { closeOnce(gate) })

	t1 := mustEnqueue(t, d, durable(event("a", 1), ackVersionInto(&acked, 1)))
	t2 := mustEnqueue(t, d, durable(event("a", 2), ackVersionInto(&acked, 2)))
	closeOnce(gate)
	if err := h.final(t, t1, "v1"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v1 final = %v, want ErrDeliveryParked", err)
	}
	if err := h.final(t, t2, "the queued v2"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v2 final = %v, want ErrDeliveryParked", err)
	}
	_, v1Acked := acked.Load(int64(1))
	notHeld := v1Acked || dead.Load() != 1
	notParked := !slices.Equal(parked(d), []string{"a"}) || slices.Contains(rec.list(), "a/2")
	if notHeld || notParked {
		t.Fatalf("v1 acked %v, dead letters %d, parked %v, delivered %v; want unacked, 1, [a], no v2", v1Acked, dead.Load(), parked(d), rec.list())
	}
}

func deadLetterRefusalParksBeforeReporting(
	t testing.TB,
	h harness,
) {
	var d asynxmd.Dispatcher[Aggregate]
	parkedWhenTold := make(chan []string, 4)
	cfg := config(&bus{}, subscriber(fail))
	cfg.MaxAttempts = 1
	cfg.DeadLetter = func(
		context.Context,
		asynxmd.Event[Aggregate],
		error,
	) error {
		return errDown
	}
	cfg.OnDeliveryError = func(
		_ context.Context,
		_ asynxmd.Event[Aggregate],
		err error,
		_ int,
	) {
		sendIf(errors.Is(err, asynxmd.ErrDeliveryParked), parkedWhenTold, parked(d))
	}
	d = h.build(t, cfg)

	mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	if got := receive(t, h.patience, parkedWhenTold, "the park to be reported"); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("parked when the handler was told = %v, want [a]", got)
	}
}

func supersedeLetsATombstoneWithAnAckThrough(
	t testing.TB,
	h harness,
) {
	var acked sync.Map
	attempted := make(chan struct{}, 8)
	cfg := config(&bus{}, subscriber(func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		poison := e.EventName == eventName
		sendIf(poison, attempted, struct{}{})
		return errorIf(poison, errPoison)
	}))
	cfg.RetryInitial, cfg.RetryMax = time.Hour, time.Hour
	d := h.build(t, cfg)

	retrying := mustEnqueue(t, d, durable(event("a", 1), ackVersionInto(&acked, 1)))
	h.wait(t, attempted, "the first attempt")
	d.Supersede("a")
	tk := mustEnqueue(t, d, tombstone(marker("a", 2), ackVersionInto(&acked, 2)))
	if err := h.final(t, tk, "the tombstone"); err != nil {
		t.Fatalf("tombstone final = %v, want nil", err)
	}
	h.wait(t, retrying.Final, "the superseded delivery")
	_, v1 := acked.Load(int64(1))
	_, tombstoneAcked := acked.Load(int64(2))
	wrongAcks := v1 || !tombstoneAcked
	if wrongAcks || len(parked(d)) != 0 {
		t.Fatalf("v1 acked %v, tombstone acked %v, parked %v; want only the tombstone acked, unparked", v1, tombstoneAcked, parked(d))
	}
}

func supersedeLetsTheTombstoneThrough(
	t testing.TB,
	h harness,
) {
	var seen journal
	var acked sync.Map
	attempted := make(chan struct{}, 8)
	sub := subscriber(func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		seen.add(e)
		poison := e.EventName == eventName
		sendIf(poison, attempted, struct{}{})
		return errorIf(poison, errPoison)
	})
	sub.Match = onlyTestEvents
	cfg := config(&bus{}, sub)
	cfg.RetryInitial, cfg.RetryMax = time.Hour, time.Hour
	d := h.build(t, cfg)

	mustEnqueue(t, d, durable(event("a", 1), ackVersionInto(&acked, 1)))
	mustEnqueue(t, d, durable(event("a", 2), ackVersionInto(&acked, 2)))
	h.wait(t, attempted, "the poison attempt")
	d.Supersede("a")
	tk := mustEnqueue(t, d, tombstone(marker("a", 3), nil))
	if err := h.final(t, tk, "the tombstone"); err != nil {
		t.Fatalf("tombstone final = %v", err)
	}
	if len(parked(d)) != 0 || !slices.Contains(seen.list(), "a/3") {
		t.Fatalf("parked = %v, seen = %v; want unparked and the tombstone seen", parked(d), seen.list())
	}
}

func supersedeAfterThePop(
	t testing.TB,
	h harness,
) {
	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { closeOnce(release) })
	var once sync.Once
	var oldRuns atomic.Int32
	b := &bus{fn: func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		holdFirst(&once, entered, release)
		return nil
	}}
	cfg := config(b, subscriber(func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		old := e.EventName != asynxmd.ForgetEventName
		countIf(old, &oldRuns)
		return errorIf(old, errDown)
	}))
	cfg.RetryInitial, cfg.RetryMax = time.Hour, time.Hour
	cfg.MaxAttempts = 5
	d := h.build(t, cfg)

	mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, entered, "v1 popped and publishing")
	d.Supersede("a")
	tk := mustEnqueue(t, d, tombstone(marker("a", 2), acknowledge))
	closeOnce(release)
	if err := h.final(t, tk, "the marker"); err != nil {
		t.Fatalf("marker = %v", err)
	}
	if oldRuns.Load() != 0 || len(parked(d)) != 0 {
		t.Fatalf("the superseded job ran %d attempts, parked = %v; want 0 and none", oldRuns.Load(), parked(d))
	}
}

func supersedeLeavesNoRecord(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	d.Park("a", errImposed, 2)
	d.Supersede("a")
	if len(parked(d)) != 0 || d.Unrun("a", 2) != nil {
		t.Fatalf("parked = %v, Unrun = %v; want unparked and no record", parked(d), d.Unrun("a", 2))
	}
	tk := mustEnqueue(t, d, durable(event("a", 3), acknowledge))
	if err := h.final(t, tk, "a durable event after the supersede"); err != nil {
		t.Fatalf("final = %v, want delivered", err)
	}
}

func tombstoneFinalReportsAFailure(
	t testing.TB,
	h harness,
) {
	sub := subscriber(fail)
	sub.Match = never
	cfg := config(&bus{}, sub)
	cfg.MaxAttempts = 2
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, tombstone(marker("a", 2), nil))
	if err := h.final(t, tk, "the tombstone"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
	if got := parked(d); len(got) != 0 {
		t.Fatalf("parked = %v, want none: a tombstone without an ack leaves parking to Forget", got)
	}
}

func forgetIsNeverDeadLettered(
	t testing.TB,
	h harness,
) {
	var dead atomic.Int32
	cfg := config(&bus{}, subscriber(fail))
	cfg.MaxAttempts = 2
	cfg.DeadLetter = func(
		context.Context,
		asynxmd.Event[Aggregate],
		error,
	) error {
		dead.Add(1)
		return nil
	}
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, tombstone(marker("a", 2), acknowledge))
	if err := h.final(t, tk, "the marker"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked: a dead letter must not let Forget erase", err)
	}
	if dead.Load() != 0 || !slices.Equal(parked(d), []string{"a"}) {
		t.Fatalf("dead letters = %d, parked = %v; want none and [a]", dead.Load(), parked(d))
	}
}

func giveUpAfterASupersede(
	t testing.TB,
	h harness,
) {
	inHandler := make(chan struct{}, 4)
	release := make(chan struct{})
	var dead atomic.Int32
	cfg := config(&bus{}, subscriber(func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		inHandler <- struct{}{}
		<-release
		return errDown
	}))
	cfg.MaxAttempts = 1
	cfg.DeadLetter = func(
		context.Context,
		asynxmd.Event[Aggregate],
		error,
	) error {
		dead.Add(1)
		return nil
	}
	d := h.build(t, cfg)
	t.Cleanup(func() { closeOnce(release) })

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, inHandler, "the last attempt to run")
	d.Supersede("a")
	closeOnce(release)
	if err := h.final(t, tk, "the superseded delivery"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
	if len(parked(d)) != 0 || dead.Load() != 0 {
		t.Fatalf("parked = %v, dead letters = %d: the exhausted attempt acted on a superseded delivery", parked(d), dead.Load())
	}
}

func interruptAfterASupersede(
	t testing.TB,
	h harness,
) {
	attempted := make(chan struct{}, 4)
	cfg := config(&bus{}, subscriber(func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		sendIf(true, attempted, struct{}{})
		return errDown
	}))
	cfg.RetryInitial, cfg.RetryMax = time.Hour, time.Hour
	cfg.MaxAttempts = 100
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, attempted, "the first attempt")
	d.Supersede("a")
	h.final(t, tk, "the superseded delivery")
	d.Interrupt()
	if got := parked(d); len(got) != 0 {
		t.Fatalf("parked = %v, want none", got)
	}
}

func parkWithoutAQueue(
	t testing.TB,
	h harness,
) {
	var acked sync.Map
	d := h.build(t, config(&bus{}))
	d.Park("ghost", errImposed)
	d.Park("ghost", errImposed)
	resumeIfParked(d, "never-parked")

	tk := mustEnqueue(t, d, durable(event("ghost", 1), ackVersionInto(&acked, 1)))
	if err := h.final(t, tk, "the parked event"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
	if _, ok := acked.Load(int64(1)); ok {
		t.Fatal("an event of a parked aggregate was acknowledged")
	}
	resumeIfParked(d, "ghost")
	tk = mustEnqueue(t, d, durable(event("ghost", 1), ackVersionInto(&acked, 1)))
	if err := h.final(t, tk, "the event after the lift"); err != nil {
		t.Fatalf("final after the lift = %v", err)
	}
}

func parkKeepsLiveOnlyJobs(
	t testing.TB,
	h harness,
) {
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	d := h.build(t, config(&bus{}, subscriber(func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		sendIf(e.Version == 1, started, struct{}{})
		holdIf(e.Version == 1, gate)
		return nil
	})))
	t.Cleanup(func() { closeOnce(gate) })

	mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, started, "v1 running")
	queued := mustEnqueue(t, d, durable(event("a", 2), acknowledge))
	liveOnly := mustEnqueue(t, d, live(event("a", 3)))
	d.Park("a", errImposed)
	if err := h.final(t, queued, "the queued durable event"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("durable final = %v, want ErrDeliveryParked", err)
	}
	closeOnce(gate)
	if err := h.final(t, liveOnly, "the live-only event"); err != nil {
		t.Fatalf("live-only final = %v, want delivered", err)
	}
}

func closeOnce(
	ch chan struct{},
) {
	if isClosed(ch) {
		return
	}
	close(ch)
}

func parkQueuedJobsReportUnrun(
	t testing.TB,
	h harness,
) {
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	d := h.build(t, config(&bus{}, subscriber(func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		sendIf(e.Version == 1, started, struct{}{})
		holdIf(e.Version == 1, gate)
		return nil
	})))
	t.Cleanup(func() { closeOnce(gate) })

	mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, started, "v1 running")
	queued := mustEnqueue(t, d, durable(event("a", 2), acknowledge))
	d.Park("a", errImposed)
	h.wait(t, queued.Settled, "the queued event to settle")
	if err := queued.Unrun(); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("Unrun = %v, want ErrDeliveryParked", err)
	}
}

func parkMergesIntoAPark(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	d.Park("a", errImposed, 3)
	generation := d.Parks()["a"]
	d.Park("a", errImposed, 5)
	if again := d.Parks()["a"]; again != generation {
		t.Fatalf("generation = %d after parking again, want the first park's %d", again, generation)
	}
	lifted, undelivered := d.ResumeUndelivered("a", generation)
	if !lifted || !slices.Equal(undelivered, []int64{3, 5}) {
		t.Fatalf("ResumeUndelivered = %v, %v; want true, [3 5]", lifted, undelivered)
	}
}

func parkAfterALift(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	d.Park("a", errImposed)
	first := d.Parks()["a"]
	resumeIfParked(d, "a")
	d.Park("a", errImposed)
	if again, isParked := d.Parks()["a"]; !isParked || again == first {
		t.Fatalf("generation after a lift and a new park = %d (parked %v), want parked with a generation other than %d", again, isParked, first)
	}
}

func parkRemembersTheNamedVersions(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	d.Park("a", errImposed, 3, 5)
	if !slices.Equal(parked(d), []string{"a"}) {
		t.Fatalf("parked = %v, want [a]", parked(d))
	}
	if !errors.Is(d.Unrun("a", 3), asynxmd.ErrDeliveryParked) || !errors.Is(d.Unrun("a", 5), asynxmd.ErrDeliveryParked) {
		t.Fatalf("Unrun(a, 3) = %v, Unrun(a, 5) = %v; want ErrDeliveryParked", d.Unrun("a", 3), d.Unrun("a", 5))
	}
	if err := d.Unrun("a", 4); err != nil {
		t.Fatalf("Unrun(a, 4) = %v, want nil", err)
	}
	resumeIfParked(d, "a")
	if err := d.Unrun("a", 3); err != nil {
		t.Fatalf("Unrun after the lift = %v, want nil", err)
	}
}

func giveUpParksBeforeReporting(
	t testing.TB,
	h harness,
) {
	var d asynxmd.Dispatcher[Aggregate]
	parkedWhenTold := make(chan []string, 4)
	cfg := config(&bus{}, subscriber(fail))
	cfg.MaxAttempts = 1
	cfg.OnDeliveryError = func(
		_ context.Context,
		_ asynxmd.Event[Aggregate],
		err error,
		_ int,
	) {
		sendIf(errors.Is(err, asynxmd.ErrDeliveryParked), parkedWhenTold, parked(d))
	}
	d = h.build(t, cfg)

	mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	if got := receive(t, h.patience, parkedWhenTold, "the park to be reported"); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("parked when the handler was told = %v, want [a]", got)
	}
}

func ackFailureParksBeforeReporting(
	t testing.TB,
	h harness,
) {
	var d asynxmd.Dispatcher[Aggregate]
	parkedWhenTold := make(chan []string, 4)
	cfg := config(&bus{}, subscriber(succeed))
	cfg.OnPublishError = func(
		context.Context,
		asynxmd.Event[Aggregate],
		error,
	) {
		sendIf(true, parkedWhenTold, parked(d))
	}
	d = h.build(t, cfg)

	mustEnqueue(t, d, durable(event("a", 1), failAck(errDown)))
	if got := receive(t, h.patience, parkedWhenTold, "the ack failure to be reported"); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("parked when the handler was told = %v, want [a]", got)
	}
}

func ackFailureParks(
	t testing.TB,
	h harness,
) {
	var rec journal
	var reported atomic.Int32
	cfg := config(&bus{}, subscriber(func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		rec.add(e)
		return nil
	}))
	cfg.OnPublishError = func(
		context.Context,
		asynxmd.Event[Aggregate],
		error,
	) {
		reported.Add(1)
	}
	d := h.build(t, cfg)
	gate := make(chan struct{})
	t.Cleanup(func() { closeOnce(gate) })
	blockedFailingAck := func(
		context.Context,
	) error {
		holdUpTo(gate, h.patience)
		return errDown
	}

	t1 := mustEnqueue(t, d, durable(event("a", 1), blockedFailingAck))
	t2 := mustEnqueue(t, d, durable(event("a", 2), acknowledge))
	closeOnce(gate)
	if err := h.final(t, t1, "v1"); err == nil {
		t.Fatal("v1 ended as acknowledged")
	}
	if err := h.final(t, t2, "v2"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v2 = %v, want ErrDeliveryParked", err)
	}
	wrongState := !slices.Equal(rec.list(), []string{"a/1"}) || !slices.Equal(parked(d), []string{"a"})
	if wrongState || reported.Load() != 1 {
		t.Fatalf("delivered %v, parked %v, reported %d; want only v1, [a], 1", rec.list(), parked(d), reported.Load())
	}
}

func resumeUndeliveredReturnsTheRecord(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
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

func resumeUndeliveredLeavesANewerPark(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	d.Park("a", errImposed)
	listed := d.Parks()["a"]
	resumeIfParked(d, "a")
	d.Park("a", errImposed, 4)

	if lifted, _ := d.ResumeUndelivered("a", listed); lifted {
		t.Fatal("lifted a park other than the one listed")
	}
	if !slices.Equal(parked(d), []string{"a"}) {
		t.Fatalf("parked = %v, want a still parked", parked(d))
	}
	if err := d.Unrun("a", 4); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("Unrun = %v, want what the new park left undelivered kept", err)
	}
	if again := d.Parks()["a"]; again == listed {
		t.Fatal("a new park reused the generation of the old one")
	}
}

func ticketScenarios() map[string]scenario {
	return map[string]scenario{
		"TestTicketFor_FindsAnEventWhileItsDeliveryIsUnderWay":           ticketForFindsAnEventUnderWay,
		"TestTicketFor_FindsAnEventQueuedBehindARunningOne":              ticketForFindsAQueuedEvent,
		"TestTicketFor_FindsAnEventWhileItRetries":                       ticketForFindsAnEventWhileItRetries,
		"TestTicketFor_ForgetsEventsLeftBehindByAPark":                   ticketForForgetsParkedEvents,
		"TestTicketFor_ForgetsQueuedEventsWhenTheyAreParked":             ticketForForgetsQueuedParkedEvents,
		"TestUnrun_RemembersEventsLeftUndeliveredByAPark":                unrunRemembersParkedEvents,
		"TestUnrun_RemembersQueuedEventsThatParkingTookOutOfMemory":      unrunRemembersQueuedParkedEvents,
		"TestUnrun_DoesNotCountAnEventWhoseHandlersRanAndFailed":         unrunIgnoresEventsWhoseHandlersRan,
		"TestTicket_UnrunSaysWhyNoHandlerRan":                            ticketUnrunSaysWhy,
		"TestTicket_AParkedDurableEventSettlesAndEndsWithDeliveryParked": ticketOfAParkedEvent,
	}
}

func gatedBus(
	entered chan struct{},
	release <-chan struct{},
) *bus {
	return &bus{fn: func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		entered <- struct{}{}
		<-release
		return nil
	}}
}

func ticketForFindsAnEventUnderWay(
	t testing.TB,
	h harness,
) {
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	d := h.build(t, config(gatedBus(entered, release)))
	t.Cleanup(func() { closeOnce(release) })
	if _, found := d.TicketFor("a", 1); found {
		t.Fatal("TicketFor found an event that was never queued")
	}

	enqueued := mustEnqueue(t, d, live(event("a", 1)))
	h.wait(t, entered, "the handler to start")
	ticket, found := d.TicketFor("a", 1)
	sameTicket := ticket.Settled == enqueued.Settled && ticket.Final == enqueued.Final
	if !found || !sameTicket {
		t.Fatalf("TicketFor = found %v; want the ticket Enqueue returned while its handler runs", found)
	}
	_, otherVersion := d.TicketFor("a", 2)
	_, otherAggregate := d.TicketFor("b", 1)
	if otherVersion || otherAggregate {
		t.Fatal("TicketFor matched another version or aggregate")
	}
	closeOnce(release)
	h.wait(t, ticket.Final, "the delivery")
	h.awaitForgotten(t, d, "a", 1, "TicketFor to forget an event whose delivery is over")
}

func ticketForFindsAQueuedEvent(
	t testing.TB,
	h harness,
) {
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	d := h.build(t, config(gatedBus(entered, release)))
	t.Cleanup(func() { closeOnce(release) })

	mustEnqueue(t, d, live(event("a", 1)))
	h.wait(t, entered, "v1 to start")
	queued := mustEnqueue(t, d, durable(event("a", 2), acknowledge))
	ticket, found := d.TicketFor("a", 2)
	sameTicket := ticket.Settled == queued.Settled && ticket.Final == queued.Final
	if !found || !sameTicket {
		t.Fatalf("TicketFor(a, 2) = found %v; want the ticket of the event queued behind a running one", found)
	}
	closeOnce(release)
	h.wait(t, queued.Final, "the queued event")
}

func ticketForFindsAnEventWhileItRetries(
	t testing.TB,
	h harness,
) {
	cfg := config(&bus{}, subscriber(fail))
	cfg.RetryInitial, cfg.RetryMax = time.Hour, time.Hour
	d := h.build(t, cfg)

	enqueued := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, enqueued.Settled, "the first attempt")
	if _, found := d.TicketFor("a", 1); !found {
		t.Fatal("TicketFor lost an event that is retrying")
	}
	d.Interrupt()
	h.wait(t, enqueued.Final, "the interrupted delivery")
	h.awaitForgotten(t, d, "a", 1, "TicketFor to forget an event whose delivery gave up")
}

func ticketForForgetsParkedEvents(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}, subscriber(succeed)))
	d.Park("a", errImposed)
	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, tk.Final, "the parked event")
	h.awaitForgotten(t, d, "a", 1, "TicketFor to forget an event that was never queued because its aggregate is parked")
}

func ticketForForgetsQueuedParkedEvents(
	t testing.TB,
	h harness,
) {
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	d := h.build(t, config(gatedBus(entered, release)))
	t.Cleanup(func() { closeOnce(release) })

	mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, entered, "v1 to start")
	queued := mustEnqueue(t, d, durable(event("a", 2), acknowledge))
	d.Park("a", errImposed)
	h.wait(t, queued.Final, "the queued event to leave memory")
	h.awaitForgotten(t, d, "a", 2, "TicketFor to forget a queued event that parking took out of memory")
}

func unrunRemembersParkedEvents(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}, subscriber(succeed)))
	d.Park("a", errImposed)
	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, tk.Final, "the parked event")

	if err := d.Unrun("a", 1); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("Unrun(a, 1) = %v, want ErrDeliveryParked", err)
	}
	if d.Unrun("a", 2) != nil || d.Unrun("b", 1) != nil {
		t.Fatalf("Unrun(a, 2) = %v, Unrun(b, 1) = %v; want nil for events nobody enqueued", d.Unrun("a", 2), d.Unrun("b", 1))
	}
	resumeIfParked(d, "a")
	if err := d.Unrun("a", 1); err != nil {
		t.Fatalf("Unrun after the park lifted = %v, want nil", err)
	}
}

func unrunRemembersQueuedParkedEvents(
	t testing.TB,
	h harness,
) {
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	d := h.build(t, config(gatedBus(entered, release)))
	t.Cleanup(func() { closeOnce(release) })

	first := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, entered, "v1 to start")
	queued := mustEnqueue(t, d, durable(event("a", 2), acknowledge))
	d.Park("a", errImposed)
	h.wait(t, queued.Final, "the queued event to leave memory")
	closeOnce(release)
	h.wait(t, first.Final, "v1")

	if err := d.Unrun("a", 2); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("Unrun(a, 2) = %v, want ErrDeliveryParked", err)
	}
	if err := d.Unrun("a", 1); err != nil {
		t.Fatalf("Unrun of the event whose handlers ran = %v, want nil", err)
	}
}

func unrunIgnoresEventsWhoseHandlersRan(
	t testing.TB,
	h harness,
) {
	cfg := config(&bus{}, subscriber(fail))
	cfg.MaxAttempts = 1
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, tk.Final, "the exhausted delivery")
	h.waitFor(t, "the aggregate to park", func() bool { return len(parked(d)) == 1 })
	if err := d.Unrun("a", 1); err != nil {
		t.Fatalf("Unrun = %v, want nil: the handler ran", err)
	}
	if err := tk.Unrun(); err != nil {
		t.Fatalf("ticket Unrun = %v, want nil: the handler ran", err)
	}
}

func ticketUnrunSaysWhy(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}, subscriber(succeed)))
	ran := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, ran.Final, "the delivered event")
	if err := ran.Unrun(); err != nil {
		t.Fatalf("Unrun = %v for a delivered event", err)
	}
	d.Park("a", errImposed)
	parkedTicket := mustEnqueue(t, d, durable(event("a", 2), acknowledge))
	h.wait(t, parkedTicket.Settled, "the parked event to settle")
	if err := parkedTicket.Unrun(); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("Unrun = %v, want ErrDeliveryParked", err)
	}
}

func ticketOfAParkedEvent(
	t testing.TB,
	h harness,
) {
	var runs journal
	b := &bus{}
	d := h.build(t, config(b, subscriber(func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		runs.add(e)
		return nil
	})))
	d.Park("a", errImposed)

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	if err := h.final(t, tk, "the parked event"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
	if !errors.Is(tk.Unrun(), asynxmd.ErrDeliveryParked) || len(runs.list()) != 0 {
		t.Fatalf("Unrun = %v, durable runs = %v; want ErrDeliveryParked and none", tk.Unrun(), runs.list())
	}
	liveTicket := mustEnqueue(t, d, live(event("a", 2)))
	if err := h.final(t, liveTicket, "a live event of the parked aggregate"); err != nil || len(b.published()) != 1 {
		t.Fatalf("live final = %v, published %d; want live events to flow while parked", err, len(b.published()))
	}
}

const shortWait = 20 * time.Millisecond

func lifecycleScenarios() map[string]scenario {
	return map[string]scenario{
		"TestClose_DrainsAllEvents":                                         closeDrainsAllEvents,
		"TestClose_Idempotent":                                              closeIdempotent,
		"TestDispatch_AfterClose":                                           enqueueAfterClose,
		"TestClose_ContextTimeout":                                          closeContextTimeout,
		"TestClose_DrainsJobsQueuedBehindASlowHandler":                      closeDrainsQueuedJobs,
		"TestClose_CalledAgainStillWaitsForTheWorkers":                      closeCalledAgainStillWaits,
		"TestClose_GivingUpCancelsRunningHandlers":                          closeGivingUpCancelsHandlers,
		"TestDurable_CloseInterruptsRetriesAndLeavesTheDeliveryUnacked":     closeInterruptsRetries,
		"TestShutdownDuringRetry_NeverDeliversALaterEventPastTheFailingOne": shutdownDuringRetry,
		"TestInterrupt_ReleasesWaitersWithoutClosingTheDispatcher":          interruptReleasesWaiters,
		"TestInterrupt_ParksTheAggregateOfTheRetryingDelivery":              interruptParks,
		"TestInterrupted_ClosesWhenTheDispatcherIsInterrupted":              interruptedOnInterrupt,
		"TestInterrupted_ClosesWhenTheDispatcherIsClosed":                   interruptedOnClose,
		"TestWaitIdle_DoesNotRefuseEnqueuesWhileItWaits":                    waitIdleRefusesNothing,
		"TestWaitIdle_ReturnsAtOnceWhenNothingIsQueued":                     waitIdleWhenNothingIsQueued,
		"TestHandlerContext_MarksItsAggregateAndEndsWithTheDelivery":        handlerContextMarksItsAggregate,
	}
}

func closeDrainsAllEvents(
	t testing.TB,
	h harness,
) {
	b := &bus{}
	d := h.build(t, config(b))
	for v := int64(1); v <= 20; v++ {
		mustEnqueue(t, d, live(event("agg-1", v)))
	}
	if err := h.close(d); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := versionsOf(b.published()); !slices.Equal(got, sequence(20)) {
		t.Fatalf("published after Close = %v, want 1..20 in order", got)
	}
}

func closeIdempotent(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	if err := h.close(d); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := h.close(d); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func enqueueAfterClose(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	if err := h.close(d); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, liveErr := d.Enqueue(context.Background(), live(event("agg-1", 1)))
	_, durableErr := d.Enqueue(context.Background(), durable(event("agg-1", 2), acknowledge))
	if !errors.Is(liveErr, asynxmd.ErrDispatcherClosed) || !errors.Is(durableErr, asynxmd.ErrDispatcherClosed) {
		t.Fatalf("Enqueue after Close = %v, %v; want ErrDispatcherClosed", liveErr, durableErr)
	}
}

func closeContextTimeout(
	t testing.TB,
	h harness,
) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	d := h.build(t, config(gatedBus(entered, release)))
	t.Cleanup(func() { closeOnce(release) })

	mustEnqueue(t, d, live(event("agg-1", 1)))
	h.wait(t, entered, "the handler to start")
	ctx, cancel := context.WithTimeout(context.Background(), shortWait)
	defer cancel()
	if err := d.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close = %v, want DeadlineExceeded", err)
	}
}

func closeDrainsQueuedJobs(
	t testing.TB,
	h harness,
) {
	release := make(chan struct{})
	running := make(chan struct{}, 3)
	var handled atomic.Int32
	b := &bus{fn: func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		sendIf(e.AggregateID == "a", running, struct{}{})
		holdIf(e.AggregateID == "a", release)
		countIf(e.AggregateID == "a", &handled)
		return nil
	}}
	d := h.build(t, config(b))
	t.Cleanup(func() { closeOnce(release) })
	for v := int64(1); v <= 3; v++ {
		mustEnqueue(t, d, live(event("a", v)))
	}
	h.wait(t, running, "the first job to be inside its handler")

	var handledAtClose atomic.Int32
	closed := make(chan error, 1)
	go func() {
		err := h.close(d)
		handledAtClose.Store(handled.Load())
		closed <- err
	}()
	h.waitFor(t, "Close to stop accepting requests", func() bool { return refusesProbe(d) })
	closeOnce(release)
	if err := receive(t, h.patience, closed, "Close to return"); err != nil || handledAtClose.Load() != 3 {
		t.Fatalf("Close = %v with %d jobs handled when it returned; want nil and all 3 drained first", err, handledAtClose.Load())
	}
}

func refusesProbe(
	d asynxmd.Dispatcher[Aggregate],
) bool {
	_, err := d.Enqueue(context.Background(), live(event("probe", 1)))
	return errors.Is(err, asynxmd.ErrDispatcherClosed)
}

func closeCalledAgainStillWaits(
	t testing.TB,
	h harness,
) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	d := h.build(t, config(&bus{}, subscriber(func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		sendIf(true, started, struct{}{})
		<-release
		return nil
	})))
	t.Cleanup(func() { closeOnce(release) })

	mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, started, "the handler to run")
	if err := closeWithin(d, shortWait); err == nil {
		t.Fatal("the first Close succeeded with a handler running")
	}
	if err := closeWithin(d, shortWait); err == nil {
		t.Fatal("the second Close claimed success while the handler still runs")
	}
	closeOnce(release)
	if err := h.close(d); err != nil {
		t.Fatalf("Close after the handler finished: %v", err)
	}
}

func closeWithin(
	d asynxmd.Dispatcher[Aggregate],
	wait time.Duration,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	return d.Close(ctx)
}

func closeGivingUpCancelsHandlers(
	t testing.TB,
	h harness,
) {
	started := make(chan context.Context, 1)
	cfg := config(&bus{}, subscriber(func(
		ctx context.Context,
		_ asynxmd.Event[Aggregate],
	) error {
		sendIf(true, started, ctx)
		<-ctx.Done()
		return ctx.Err()
	}))
	cfg.MaxAttempts = 1
	d := h.build(t, cfg)

	mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	handlerCtx := receive(t, h.patience, started, "the handler to run")
	if err := closeWithin(d, shortWait); err == nil {
		t.Fatal("Close succeeded with a handler still running")
	}
	h.wait(t, handlerCtx.Done(), "the handler context to be cancelled")
}

func closeInterruptsRetries(
	t testing.TB,
	h harness,
) {
	var acked atomic.Bool
	cfg := config(&bus{}, subscriber(fail))
	cfg.RetryInitial, cfg.RetryMax = time.Hour, time.Hour
	d := h.build(t, cfg)
	ack := func(
		context.Context,
	) error {
		acked.Store(true)
		return nil
	}

	tk := mustEnqueue(t, d, durable(event("a", 1), ack))
	h.wait(t, tk.Settled, "the first attempt")
	if err := h.close(d); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if acked.Load() {
		t.Fatal("a failing delivery was acknowledged on Close")
	}
}

func shutdownDuringRetry(
	t testing.TB,
	h harness,
) {
	var rec journal
	var acked sync.Map
	attempted := make(chan struct{}, 64)
	cfg := config(&bus{}, subscriber(func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		rec.add(e)
		sendIf(e.Version == 1, attempted, struct{}{})
		return errorIf(e.Version == 1, errPoison)
	}))
	cfg.RetryInitial, cfg.RetryMax = time.Hour, time.Hour
	d := h.build(t, cfg)

	t1 := mustEnqueue(t, d, durable(event("a", 1), ackVersionInto(&acked, 1)))
	t2 := mustEnqueue(t, d, durable(event("a", 2), ackVersionInto(&acked, 2)))
	h.wait(t, attempted, "the first attempt of v1")
	if err := h.close(d); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if h.final(t, t1, "v1") == nil || h.final(t, t2, "v2") == nil {
		t.Fatal("a delivery behind a failing one ended as delivered")
	}
	_, v1 := acked.Load(int64(1))
	_, v2 := acked.Load(int64(2))
	anyAcked := v1 || v2
	if anyAcked || slices.Contains(rec.list(), "a/2") {
		t.Fatalf("acked v1 %v, v2 %v, delivered %v; want nothing past the failing v1", v1, v2, rec.list())
	}
}

func interruptReleasesWaiters(
	t testing.TB,
	h harness,
) {
	attempted := make(chan struct{}, 8)
	cfg := config(&bus{}, subscriber(func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		sendIf(true, attempted, struct{}{})
		return errPoison
	}))
	cfg.RetryInitial, cfg.RetryMax = time.Hour, time.Hour
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, attempted, "the first attempt")
	d.Interrupt()
	if got := h.final(t, tk, "the interrupted delivery"); !errors.Is(got, asynxmd.ErrDispatcherClosed) {
		t.Fatalf("final = %v, want ErrDispatcherClosed", got)
	}
	next, err := d.Enqueue(context.Background(), live(event("b", 1)))
	if err != nil {
		t.Fatalf("Enqueue after Interrupt: %v", err)
	}
	h.wait(t, next.Final, "a live event after Interrupt")
}

func interruptParks(
	t testing.TB,
	h harness,
) {
	attempted := make(chan struct{}, 8)
	cfg := config(&bus{}, subscriber(func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		sendIf(true, attempted, struct{}{})
		return errPoison
	}))
	cfg.RetryInitial, cfg.RetryMax = time.Hour, time.Hour
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, attempted, "the first attempt")
	d.Interrupt()
	h.wait(t, tk.Final, "the interrupted delivery")
	if !slices.Equal(parked(d), []string{"a"}) {
		t.Fatalf("parked = %v, want the interrupted aggregate parked", parked(d))
	}
	later := mustEnqueue(t, d, durable(event("a", 2), acknowledge))
	if err := h.final(t, later, "a durable event after Interrupt"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
}

func interruptedOnInterrupt(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	if isClosed(d.Interrupted()) {
		t.Fatal("Interrupted closed before Interrupt")
	}
	d.Interrupt()
	if !isClosed(d.Interrupted()) {
		t.Fatal("Interrupted did not close after Interrupt")
	}
}

func interruptedOnClose(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	if err := h.close(d); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !isClosed(d.Interrupted()) {
		t.Fatal("Interrupted did not close after Close")
	}
}

func waitIdleRefusesNothing(
	t testing.TB,
	h harness,
) {
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	b := &bus{fn: func(
		_ context.Context,
		e asynxmd.Event[Aggregate],
	) error {
		sendIf(e.AggregateID == "a", entered, struct{}{})
		holdIf(e.AggregateID == "a", release)
		return nil
	}}
	d := h.build(t, config(b))
	t.Cleanup(func() { closeOnce(release) })

	first := mustEnqueue(t, d, live(event("a", 1)))
	h.wait(t, entered, "the handler to start")
	started := make(chan struct{})
	idle := make(chan struct{})
	go waitIdleAfter(d, started, idle)
	h.wait(t, started, "WaitIdle to be called")
	var refused journal
	enqueued := make(chan struct{})
	enqueueAll(d, "b", 50, &refused, enqueued)
	if got := refused.list(); len(got) != 0 || isClosed(idle) {
		t.Fatalf("refused %v, WaitIdle returned %v; want nothing refused while WaitIdle waits on a running event", got, isClosed(idle))
	}
	closeOnce(release)
	h.wait(t, idle, "WaitIdle to return")
	h.wait(t, first.Final, "the first event")
}

func waitIdleWhenNothingIsQueued(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	h.idle(t, d)
}

func handlerContextMarksItsAggregate(
	t testing.TB,
	h harness,
) {
	var d, other asynxmd.Dispatcher[Aggregate]
	var inOwn, inOther, inOtherAggregate atomic.Bool
	contexts := make(chan context.Context, 1)
	d = h.build(t, config(&bus{}, subscriber(func(
		ctx context.Context,
		_ asynxmd.Event[Aggregate],
	) error {
		inOwn.Store(d.InHandler(ctx, "a"))
		inOtherAggregate.Store(d.InHandler(ctx, "b"))
		inOther.Store(other.InHandler(ctx, "a"))
		sendIf(true, contexts, ctx)
		return nil
	})))
	other = h.build(t, config(&bus{}))

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, tk.Final, "the delivery")
	handlerCtx := receive(t, h.patience, contexts, "the handler context")
	wrongOwner := !inOwn.Load() || inOtherAggregate.Load()
	if wrongOwner || inOther.Load() {
		t.Fatalf("InHandler own=%v otherAggregate=%v otherInstance=%v; want true, false, false", inOwn.Load(), inOtherAggregate.Load(), inOther.Load())
	}
	if handlerCtx.Err() == nil || d.InHandler(context.Background(), "a") {
		t.Fatal("the handler context outlived the delivery, or a plain context counts as a handler's")
	}
}

var (
	errImposed = errors.New("dispatchertest: parked by the test")
	errLater   = errors.New("dispatchertest: a later cause")
	errAck     = errors.New("dispatchertest: ack failed")
)

func inspectScenarios() map[string]scenario {
	return map[string]scenario{
		"TestInspect_UnknownAggregateIsTheZeroStatus":                  inspectUnknownAggregate,
		"TestInspect_ReportsParkGenerationCauseAndUndelivered":         inspectReportsThePark,
		"TestInspect_ParkedAgainKeepsTheGenerationAndTheFirstCause":    inspectKeepsTheFirstCause,
		"TestInspect_ALaterCauseReplacesANilOne":                       inspectReplacesANilCause,
		"TestInspect_GenerationChangesWhenAParkIsImposedAgain":         inspectNewGeneration,
		"TestInspect_IsClearAfterTheParkLifts":                         inspectClearAfterTheLift,
		"TestInspect_GiveUpCauseNamesTheLastHandlerError":              inspectGiveUpCause,
		"TestInspect_AckFailureCauseIsTheAckError":                     inspectAckFailureCause,
		"TestInspect_InterruptCauseIsDispatcherClosed":                 inspectInterruptCause,
		"TestInspect_ADurableEventRefusedByTheParkIsListedUndelivered": inspectListsARefusedEvent,
	}
}

func inspectUnknownAggregate(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}, subscriber(succeed)))
	h.final(t, mustEnqueue(t, d, durable(event("busy", 1), acknowledge)), "a delivered event")
	if got := d.Inspect("ghost"); !reflect.DeepEqual(got, asynxmd.DeliveryStatus{}) {
		t.Fatalf("Inspect(ghost) = %+v, want the zero status", got)
	}
	if got := d.Inspect("busy"); !reflect.DeepEqual(got, asynxmd.DeliveryStatus{}) {
		t.Fatalf("Inspect(busy) = %+v, want the zero status for an aggregate that is not parked", got)
	}
}

func inspectReportsThePark(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	d.Park("a", errImposed, 5, 3)
	got := d.Inspect("a")
	unparked := !got.Parked || got.Generation == 0
	if unparked || got.Generation != d.Parks()["a"] {
		t.Fatalf("Inspect = %+v, want parked with the generation Parks lists (%d)", got, d.Parks()["a"])
	}
	if !errors.Is(got.Cause, errImposed) {
		t.Fatalf("Cause = %v, want %v", got.Cause, errImposed)
	}
	if !slices.Equal(got.Undelivered, []int64{3, 5}) {
		t.Fatalf("Undelivered = %v, want [3 5] ascending", got.Undelivered)
	}
}

func inspectKeepsTheFirstCause(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	d.Park("a", errImposed, 3)
	first := d.Inspect("a").Generation
	d.Park("a", errLater, 1)
	d.Park("a", nil)
	got := d.Inspect("a")
	if !got.Parked || got.Generation != first {
		t.Fatalf("Inspect = %+v, want still parked with generation %d", got, first)
	}
	if !errors.Is(got.Cause, errImposed) || errors.Is(got.Cause, errLater) {
		t.Fatalf("Cause = %v, want the first one, %v", got.Cause, errImposed)
	}
	if !slices.Equal(got.Undelivered, []int64{1, 3}) {
		t.Fatalf("Undelivered = %v, want the merged [1 3]", got.Undelivered)
	}
}

func inspectReplacesANilCause(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	d.Park("a", nil)
	if got := d.Inspect("a"); !got.Parked || got.Cause != nil {
		t.Fatalf("Inspect = %+v, want parked with no cause", got)
	}
	d.Park("a", errLater)
	if got := d.Inspect("a"); !errors.Is(got.Cause, errLater) {
		t.Fatalf("Cause = %v, want %v replacing the nil one", got.Cause, errLater)
	}
}

func inspectNewGeneration(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	d.Park("a", errImposed)
	first := d.Inspect("a").Generation
	resumeIfParked(d, "a")
	d.Park("a", errLater)
	got := d.Inspect("a")
	if !got.Parked || got.Generation == first {
		t.Fatalf("Inspect = %+v, want parked with a generation other than %d", got, first)
	}
	if got.Generation != d.Parks()["a"] {
		t.Fatalf("Generation = %d, want the one Parks lists, %d", got.Generation, d.Parks()["a"])
	}
	if !errors.Is(got.Cause, errLater) {
		t.Fatalf("Cause = %v, want the new park's %v", got.Cause, errLater)
	}
}

func inspectClearAfterTheLift(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	d.Park("a", errImposed, 3)
	resumeIfParked(d, "a")
	if got := d.Inspect("a"); !reflect.DeepEqual(got, asynxmd.DeliveryStatus{}) {
		t.Fatalf("Inspect after the lift = %+v, want the zero status", got)
	}
	d.Park("a", nil)
	got := d.Inspect("a")
	if !got.Parked || got.Cause != nil {
		t.Fatalf("Inspect = %+v, want a fresh park with no cause", got)
	}
	if len(got.Undelivered) != 0 {
		t.Fatalf("Undelivered = %v, want nothing after the lift", got.Undelivered)
	}
}

func inspectGiveUpCause(
	t testing.TB,
	h harness,
) {
	cfg := config(&bus{}, subscriber(func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		return errPoison
	}))
	cfg.MaxAttempts = 3
	d := h.build(t, cfg)

	h.final(t, mustEnqueue(t, d, durable(event("a", 1), acknowledge)), "the delivery to give up")
	got := d.Inspect("a")
	if !got.Parked || got.Generation != d.Parks()["a"] {
		t.Fatalf("Inspect = %+v, want parked with the generation Parks lists", got)
	}
	if !errors.Is(got.Cause, asynxmd.ErrDeliveryParked) || !errors.Is(got.Cause, errPoison) {
		t.Fatalf("Cause = %v, want ErrDeliveryParked wrapping %v", got.Cause, errPoison)
	}
}

func inspectAckFailureCause(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}, subscriber(succeed)))
	h.final(t, mustEnqueue(t, d, durable(event("a", 1), failAck(errAck))), "the failed ack")
	got := d.Inspect("a")
	if !got.Parked || !errors.Is(got.Cause, errAck) {
		t.Fatalf("Inspect = %+v, want parked with %v", got, errAck)
	}
}

func inspectInterruptCause(
	t testing.TB,
	h harness,
) {
	attempted := make(chan struct{}, 8)
	cfg := config(&bus{}, subscriber(func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		sendIf(true, attempted, struct{}{})
		return errPoison
	}))
	cfg.RetryInitial, cfg.RetryMax = time.Hour, time.Hour
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, attempted, "the first attempt")
	d.Interrupt()
	h.wait(t, tk.Final, "the interrupted delivery")
	got := d.Inspect("a")
	if !got.Parked || !errors.Is(got.Cause, asynxmd.ErrDispatcherClosed) {
		t.Fatalf("Inspect = %+v, want parked with ErrDispatcherClosed", got)
	}
}

func inspectListsARefusedEvent(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}, subscriber(succeed)))
	d.Park("a", errImposed)
	h.final(t, mustEnqueue(t, d, durable(event("a", 4), acknowledge)), "the refused event")
	if got := d.Inspect("a"); !got.Parked || !slices.Equal(got.Undelivered, []int64{4}) {
		t.Fatalf("Inspect = %+v, want parked with [4] undelivered", got)
	}
}

func acceptingDeadLetters(
	cfg asynxmd.DispatcherConfig[Aggregate],
) (asynxmd.Dispatcher[Aggregate], error) {
	cfg.DeadLetter = swallowRefusals(cfg.DeadLetter)
	return defaultFactory(cfg)
}

func swallowRefusals(
	deadLetter asynxmd.DeadLetterHandler[Aggregate],
) asynxmd.DeadLetterHandler[Aggregate] {
	if deadLetter == nil {
		return nil
	}
	return func(
		ctx context.Context,
		e asynxmd.Event[Aggregate],
		err error,
	) error {
		_ = deadLetter(ctx, e, err)
		return nil
	}
}

type brokenCase struct {
	factory asynxmd.DispatcherFactory[Aggregate]
	flagged []string
}

func (c brokenCase) check(
	t *testing.T,
) {
	t.Parallel()
	failed := failures(harness{factory: c.factory, patience: brokenPatience})
	t.Logf("failed scenarios: %v", failedNames(failed))
	missed := slices.DeleteFunc(slices.Clone(c.flagged), func(
		scenario string,
	) bool {
		return failed[scenario]
	})
	if len(missed) != 0 {
		t.Fatalf("the suite let it pass %v (failed: %v)", missed, failedNames(failed))
	}
}

func TestBus_RecordsEveryPublishAndReturnsTheCallbackError(t *testing.T) {
	b := &bus{fn: func(
		context.Context,
		asynxmd.Event[Aggregate],
	) error {
		return errDown
	}}
	if err := b.PublishSync(context.Background(), event("a", 1)); !errors.Is(err, errDown) {
		t.Fatalf("PublishSync = %v, want the callback error", err)
	}
	if got := versionsOf(b.published()); len(got) != 1 || got[0] != 1 {
		t.Fatalf("published = %v, want [1]", got)
	}
}

func TestBus_TheRestOfTheBusIsANoOp(t *testing.T) {
	b := &bus{}
	id, subscribeErr := b.Subscribe("*", nil)
	errs := errors.Join(
		b.Publish(context.Background(), event("a", 1)),
		subscribeErr,
		b.Unsubscribe(id),
		b.Close(context.Background()),
	)
	b.WaitForHandlers()
	rejected := errs != nil || id != ""
	if rejected || len(b.published()) != 0 {
		t.Fatalf("errors %v, id %q, published %d; want a silent no-op", errs, id, len(b.published()))
	}
}

const brokenPatience = 2 * time.Second

func defaultFactory(
	cfg asynxmd.DispatcherConfig[Aggregate],
) (asynxmd.Dispatcher[Aggregate], error) {
	return New(cfg), nil
}

func brokenBy(
	wrap func(
		asynxmd.Dispatcher[Aggregate],
	) asynxmd.Dispatcher[Aggregate],
) asynxmd.DispatcherFactory[Aggregate] {
	return func(
		cfg asynxmd.DispatcherConfig[Aggregate],
	) (asynxmd.Dispatcher[Aggregate], error) {
		inner, err := defaultFactory(cfg)
		return wrap(inner), err
	}
}

func failures(
	h harness,
) map[string]bool {
	t := &tally{failed: map[string]bool{}}
	for name, check := range scenarios() {
		t.wg.Add(1)
		go t.run(name, check, h)
	}
	t.wg.Wait()
	return t.failed
}

func failedNames(
	failed map[string]bool,
) []string {
	names := slices.Collect(maps.Keys(failed))
	names = slices.DeleteFunc(names, func(
		name string,
	) bool {
		return !failed[name]
	})
	slices.Sort(names)
	return names
}

func runGroup(
	t *testing.T,
	group map[string]scenario,
) {
	h := harness{factory: defaultFactory, patience: patience}
	for _, name := range slices.Sorted(maps.Keys(group)) {
		t.Run(name, h.subtest(group[name]))
	}
}

func TestDispatcherDefault(t *testing.T) {
	Run(t, defaultFactory)
}

func TestFailures_TheDefaultFailsNoScenarioUnderTheRecorder(t *testing.T) {
	failed := failures(harness{factory: defaultFactory, patience: patience})
	if names := failedNames(failed); len(names) != 0 {
		t.Fatalf("the default failed %v under the recorder", names)
	}
	if len(failed) != len(scenarios()) {
		t.Fatalf("ran %d scenarios, want %d", len(failed), len(scenarios()))
	}
}

func TestSuite_FlagsADispatcherThatDeliversNothing(t *testing.T) {
	failed := failures(harness{factory: hollow, patience: brokenPatience})
	if got, total := len(failedNames(failed)), len(failed); got < total/2 {
		t.Fatalf("only %d of %d scenarios failed a dispatcher that delivers nothing: %v", got, total, failedNames(failed))
	}
}

func TestSuite_FlagsADispatcherThatParksEverything(t *testing.T) {
	failed := failures(harness{factory: parkingEverything, patience: brokenPatience})
	if got, total := len(failedNames(failed)), len(failed); got < total/2 {
		t.Fatalf("only %d of %d scenarios failed a dispatcher that parks everything: %v", got, total, failedNames(failed))
	}
}

func TestSuite_FlagsADispatcherThatRefusesEverything(t *testing.T) {
	failed := failures(harness{factory: refusing, patience: brokenPatience})
	if got, total := len(failedNames(failed)), len(failed); got < total*3/4 {
		t.Fatalf("only %d of %d scenarios failed a dispatcher that refuses every request: %v", got, total, failedNames(failed))
	}
}

func TestSuite_FlagsEveryBrokenDispatcher(t *testing.T) {
	cases := map[string]brokenCase{
		"reorders an aggregate's events": {brokenBy(reorder), []string{
			"TestDispatch_OrderingGuarantee",
			"TestDispatch_MultipleAggregatesOrdered",
			"TestClose_DrainsAllEvents",
		}},
		"acks before the handlers succeed": {brokenBy(ackEarly), []string{
			"TestEnqueue_AckRunsAfterHandlersFinish",
			"TestDurable_DoneClosesAfterTheFirstAttemptEvenWhenItFails",
			"TestDurable_CloseInterruptsRetriesAndLeavesTheDeliveryUnacked",
			"TestShutdownDuringRetry_NeverDeliversALaterEventPastTheFailingOne",
		}},
		"ignores the generation in ResumeUndelivered": {brokenBy(ignoreGeneration), []string{
			"TestResumeUndelivered_LeavesAParkImposedAgainSinceItWasListed",
		}},
		"queues durable events while parked": {brokenBy(queueWhileParked), []string{
			"TestPark_OfAnAggregateWithoutAQueueStillBlocksItsDurableEvents",
			"TestTicket_AParkedDurableEventSettlesAndEndsWithDeliveryParked",
			"TestTicket_UnrunSaysWhyNoHandlerRan",
			"TestUnrun_RemembersEventsLeftUndeliveredByAPark",
		}},
		"never reports ErrDeliveryParked": {brokenBy(hideParks), []string{
			"TestExhaustedAttempts_ParkTheAggregateAndLeaveOthersAlone",
			"TestUnrun_RemembersEventsLeftUndeliveredByAPark",
			"TestTicket_UnrunSaysWhyNoHandlerRan",
			"TestParkUndelivered_RemembersTheNamedVersionsUntilTheParkLifts",
		}},
		"drops Close's drain": {brokenBy(skipDrain), []string{
			"TestClose_DrainsJobsQueuedBehindASlowHandler",
			"TestClose_CalledAgainStillWaitsForTheWorkers",
		}},
		"dead-letters tombstones": {brokenBy(deadLetterTombstones), []string{
			"TestEnqueueForget_IsNeverDeadLettered",
		}},
		"accepts the event when the dead-letter handler returns an error": {acceptingDeadLetters, []string{
			"TestDeadLetter_AReturnedErrorParksTheAggregate",
		}},
		"delivers an event named like the tombstone as a tombstone": {brokenBy(treatForgetNamesAsTombstones), []string{
			"TestEnqueue_AnEventNamedLikeTheTombstoneReachesOnlyMatchingSubscribers",
		}},
	}
	for name, c := range cases {
		t.Run(name, c.check)
	}
}

type drainSkippingDispatcher struct {
	asynxmd.Dispatcher[Aggregate]
}

func skipDrain(
	inner asynxmd.Dispatcher[Aggregate],
) asynxmd.Dispatcher[Aggregate] {
	return drainSkippingDispatcher{Dispatcher: inner}
}

func (d drainSkippingDispatcher) Close(
	context.Context,
) error {
	go d.Dispatcher.Close(context.Background())
	return nil
}

func TestDurableScenarios_PassOnTheDefault(t *testing.T) {
	runGroup(t, durableScenarios())
}

type earlyAckDispatcher struct {
	asynxmd.Dispatcher[Aggregate]
}

func ackEarly(
	inner asynxmd.Dispatcher[Aggregate],
) asynxmd.Dispatcher[Aggregate] {
	return earlyAckDispatcher{Dispatcher: inner}
}

func (d earlyAckDispatcher) Enqueue(
	ctx context.Context,
	r asynxmd.DispatchRequest[Aggregate],
) (asynxmd.Ticket, error) {
	if r.Ack == nil {
		return d.Dispatcher.Enqueue(ctx, r)
	}
	_ = r.Ack(ctx)
	r.Ack = acknowledge
	return d.Dispatcher.Enqueue(ctx, r)
}

func TestEvents_RequestsCarryTheirKindAndAck(t *testing.T) {
	requests := []asynxmd.DispatchRequest[Aggregate]{
		live(event("a", 1)),
		durable(event("a", 2), acknowledge),
		redelivery(event("a", 3), nil),
		tombstone(marker("a", 4), acknowledge),
	}
	kinds := []asynxmd.DispatchKind{}
	for _, r := range requests {
		kinds = append(kinds, r.Kind)
	}
	want := []asynxmd.DispatchKind{asynxmd.DispatchEvent, asynxmd.DispatchEvent, asynxmd.DispatchRedelivery, asynxmd.DispatchTombstone}
	if !slices.Equal(kinds, want) {
		t.Fatalf("kinds = %v, want %v", kinds, want)
	}
	if requests[0].Ack != nil || requests[3].Event.EventName != asynxmd.ForgetEventName {
		t.Fatalf("want a live request without ack and a tombstone named %q", asynxmd.ForgetEventName)
	}
}

func TestEvents_MustEnqueueFailsTheScenarioOnARefusal(t *testing.T) {
	d, _ := refusing(config(nil))
	if !fails(func(
		t testing.TB,
	) {
		mustEnqueue(t, d, live(event("a", 1)))
	}) {
		t.Fatal("a refused Enqueue did not fail the scenario")
	}
}

func TestEvents_AcksRecordOrFail(t *testing.T) {
	var acked sync.Map
	errs := errors.Join(acknowledge(context.Background()), ackVersionInto(&acked, 7)(context.Background()))
	_, stored := acked.Load(int64(7))
	failsAsTold := errors.Is(failAck(errDown)(context.Background()), errDown)
	acksBroken := errs != nil || !stored
	if acksBroken || !failsAsTold {
		t.Fatalf("errors %v, stored %v; want acks that succeed, record, or fail as told", errs, stored)
	}
}

func TestEvents_EnqueueHelpersNoteRefusals(t *testing.T) {
	d, _ := refusing(config(nil))
	var refused journal
	done := make(chan struct{})
	enqueueAll(d, "a", 2, &refused, done)
	var wg sync.WaitGroup
	wg.Add(1)
	enqueueOne(&wg, d, live(event("b", 1)), &refused)
	wg.Wait()
	if got := len(refused.list()); got != 3 || !isClosed(done) {
		t.Fatalf("noted %d refusals, done closed %v; want 3 and closed", got, isClosed(done))
	}
}

type generationBlindDispatcher struct {
	asynxmd.Dispatcher[Aggregate]
}

func ignoreGeneration(
	inner asynxmd.Dispatcher[Aggregate],
) asynxmd.Dispatcher[Aggregate] {
	return generationBlindDispatcher{Dispatcher: inner}
}

func (d generationBlindDispatcher) ResumeUndelivered(
	aggregateID string,
	_ uint64,
) (bool, []int64) {
	return d.Dispatcher.ResumeUndelivered(aggregateID, d.Parks()[aggregateID])
}

func TestHandlers_MatchersAndFixedOutcomes(t *testing.T) {
	ctx := context.Background()
	matches := always("x") && !never("x") && onlyTestEvents(eventName) && !onlyTestEvents("other")
	outcomesFixed := succeed(ctx, event("a", 1)) == nil && errors.Is(fail(ctx, event("a", 1)), errDown)
	if !matches || !outcomesFixed {
		t.Fatal("a matcher or a fixed handler does not do what its name says")
	}
	if sub := subscriber(succeed); sub.Name == "" || !sub.Match("anything") {
		t.Fatalf("subscriber = %+v, want a named subscriber matching everything", sub)
	}
}

func TestHandlers_ConditionalHelpersActOnlyWhenTheConditionHolds(t *testing.T) {
	var flag atomic.Bool
	var counter atomic.Int32
	ch := make(chan int, 1)
	flagIf(false, &flag)
	countIf(false, &counter)
	sendIf(false, ch, 1)
	holdIf(false, nil)
	panicIf(false, "never")
	actedOnFalse := errorIf(false, errDown) != nil || flag.Load()
	leftover := counter.Load() != 0 || len(ch) != 0
	if actedOnFalse || leftover {
		t.Fatal("a helper acted on a false condition")
	}
	flagIf(true, &flag)
	countIf(true, &counter)
	sendIf(true, ch, 1)
	sendIf(true, ch, 2)
	actedOnTrue := errors.Is(errorIf(true, errDown), errDown) && flag.Load() && counter.Load() == 1
	if !actedOnTrue || <-ch != 1 {
		t.Fatal("a helper did not act on a true condition, or a full channel blocked")
	}
}

func recovered(
	fn func(),
) (value any) {
	defer func() { value = recover() }()
	fn()
	return nil
}

func TestHandlers_PanicIfPanicsWithTheValue(t *testing.T) {
	if got := recovered(func() { panicIf(true, "boom") }); got != "boom" {
		t.Fatalf("recovered %v, want boom", got)
	}
}

func TestHandlers_HoldFirstHoldsOnlyTheFirstCall(t *testing.T) {
	var once sync.Once
	entered := make(chan struct{})
	release := make(chan struct{})
	close(release)
	holdFirst(&once, entered, release)
	holdFirst(&once, entered, release)
	if !isClosed(entered) {
		t.Fatal("the first call did not signal that it entered")
	}
}

func failingFactory(
	asynxmd.DispatcherConfig[Aggregate],
) (asynxmd.Dispatcher[Aggregate], error) {
	return nil, errRefused
}

func TestHarness_BuildFailsTheScenarioWhenTheFactoryFails(t *testing.T) {
	h := harness{factory: failingFactory, patience: time.Second}
	if !fails(func(
		t testing.TB,
	) {
		h.build(t, config(&bus{}))
	}) {
		t.Fatal("a factory error did not fail the scenario")
	}
}

func TestHarness_ACloseErrorAtCleanupFailsTheScenario(t *testing.T) {
	h := harness{factory: refusing, patience: time.Second}
	if !fails(func(
		t testing.TB,
	) {
		h.build(t, config(&bus{}))
	}) {
		t.Fatal("a Close error at cleanup did not fail the scenario")
	}
}

func neverTrue() bool {
	return false
}

func TestHarness_BoundedWaitsFailInsteadOfHanging(t *testing.T) {
	h := harness{factory: defaultFactory, patience: time.Millisecond}
	never := make(chan struct{})
	waitFails := fails(func(
		t testing.TB,
	) {
		h.wait(t, never, "nothing")
	})
	waitForFails := fails(func(
		t testing.TB,
	) {
		h.waitFor(t, "nothing", neverTrue)
	})
	finalFails := fails(func(
		t testing.TB,
	) {
		h.final(t, asynxmd.Ticket{Final: never}, "nothing")
	})
	waitsFail := waitFails && waitForFails
	if !waitsFail || !finalFails {
		t.Fatalf("wait %v, waitFor %v, final %v: each must fail on what never happens", waitFails, waitForFails, finalFails)
	}
}

func TestHarness_IdleAndEnqueueBoundedReturnOnTheDefault(t *testing.T) {
	h := harness{factory: defaultFactory, patience: patience}
	d := h.build(t, config(&bus{}))
	tk := h.enqueueBounded(t, d, live(event("a", 1)))
	h.idle(t, d)
	if err := h.final(t, tk, "the live event"); err != nil {
		t.Fatalf("final = %v", err)
	}
}

type heldRequest struct {
	ctx     context.Context
	request asynxmd.DispatchRequest[Aggregate]
	tickets chan asynxmd.Ticket
}

func (h heldRequest) ticket() asynxmd.Ticket {
	settled := make(chan struct{})
	final := make(chan struct{})
	var inner atomic.Pointer[asynxmd.Ticket]
	go relay(h.tickets, &inner, settled, final)
	return asynxmd.Ticket{
		Settled: settled,
		Final:   final,
		Err:     func() error { return inner.Load().Err() },
		Unrun:   func() error { return inner.Load().Unrun() },
	}
}

func (h heldRequest) release(
	d asynxmd.Dispatcher[Aggregate],
) {
	tk, err := d.Enqueue(h.ctx, h.request)
	if err != nil {
		return
	}
	h.tickets <- tk
}

func relay(
	tickets <-chan asynxmd.Ticket,
	inner *atomic.Pointer[asynxmd.Ticket],
	settled chan<- struct{},
	final chan<- struct{},
) {
	tk := <-tickets
	inner.Store(&tk)
	<-tk.Settled
	close(settled)
	<-tk.Final
	close(final)
}

type hollowDispatcher struct{}

func hollow(
	asynxmd.DispatcherConfig[Aggregate],
) (asynxmd.Dispatcher[Aggregate], error) {
	return hollowDispatcher{}, nil
}

func (hollowDispatcher) Enqueue(
	context.Context,
	asynxmd.DispatchRequest[Aggregate],
) (asynxmd.Ticket, error) {
	done := make(chan struct{})
	close(done)
	return asynxmd.Ticket{
		Settled: done,
		Final:   done,
		Err:     func() error { return nil },
		Unrun:   func() error { return nil },
	}, nil
}

func (hollowDispatcher) TicketFor(
	string,
	int64,
) (asynxmd.Ticket, bool) {
	return asynxmd.Ticket{}, false
}

func (hollowDispatcher) Unrun(
	string,
	int64,
) error {
	return nil
}

func (hollowDispatcher) Park(
	string,
	error,
	...int64,
) {
}

func (hollowDispatcher) Inspect(
	string,
) asynxmd.DeliveryStatus {
	return asynxmd.DeliveryStatus{}
}

func (hollowDispatcher) Supersede(
	string,
) {
}

func (hollowDispatcher) Parks() map[string]uint64 {
	return map[string]uint64{}
}

func (hollowDispatcher) ResumeUndelivered(
	string,
	uint64,
) (bool, []int64) {
	return false, nil
}

func (hollowDispatcher) InHandler(
	context.Context,
	string,
) bool {
	return false
}

func (hollowDispatcher) Interrupt() {}

func (hollowDispatcher) Interrupted() <-chan struct{} {
	return nil
}

func (hollowDispatcher) Close(
	context.Context,
) error {
	return nil
}

func (hollowDispatcher) WaitIdle() {}

type inspectHidingDispatcher struct {
	asynxmd.Dispatcher[Aggregate]
}

func hideParksFromInspect(
	inner asynxmd.Dispatcher[Aggregate],
) asynxmd.Dispatcher[Aggregate] {
	return inspectHidingDispatcher{Dispatcher: inner}
}

func (d inspectHidingDispatcher) Inspect(
	aggregateID string,
) asynxmd.DeliveryStatus {
	status := d.Dispatcher.Inspect(aggregateID)
	status.Parked = false
	return status
}

func TestSuite_FlagsADispatcherWhoseInspectHidesItsParks(t *testing.T) {
	brokenCase{brokenBy(hideParksFromInspect), []string{
		"TestInspect_ReportsParkGenerationCauseAndUndelivered",
		"TestInspect_ParkedAgainKeepsTheGenerationAndTheFirstCause",
		"TestInspect_ALaterCauseReplacesANilOne",
		"TestInspect_GenerationChangesWhenAParkIsImposedAgain",
		"TestInspect_IsClearAfterTheParkLifts",
		"TestInspect_GiveUpCauseNamesTheLastHandlerError",
		"TestInspect_AckFailureCauseIsTheAckError",
		"TestInspect_InterruptCauseIsDispatcherClosed",
		"TestInspect_ADurableEventRefusedByTheParkIsListedUndelivered",
	}}.check(t)
}

func TestInspectScenarios_PassOnTheDefault(t *testing.T) {
	runGroup(t, inspectScenarios())
}

func TestJournal_ListsEntriesInTheOrderTheyWereAdded(t *testing.T) {
	var j journal
	j.add(event("a", 1))
	j.note("free text")
	got := j.list()
	got[0] = "changed"
	if want := []string{"a/1", "free text"}; !slices.Equal(j.list(), want) {
		t.Fatalf("list = %v, want %v and a copy", j.list(), want)
	}
}

func TestLifecycleScenarios_PassOnTheDefault(t *testing.T) {
	runGroup(t, lifecycleScenarios())
}

func TestOrderingScenarios_PassOnTheDefault(t *testing.T) {
	runGroup(t, orderingScenarios())
}

type parkHidingDispatcher struct {
	asynxmd.Dispatcher[Aggregate]
}

func hideParks(
	inner asynxmd.Dispatcher[Aggregate],
) asynxmd.Dispatcher[Aggregate] {
	return parkHidingDispatcher{Dispatcher: inner}
}

func (d parkHidingDispatcher) Enqueue(
	ctx context.Context,
	r asynxmd.DispatchRequest[Aggregate],
) (asynxmd.Ticket, error) {
	tk, err := d.Dispatcher.Enqueue(ctx, r)
	return hideParked(tk), err
}

func (d parkHidingDispatcher) TicketFor(
	aggregateID string,
	version int64,
) (asynxmd.Ticket, bool) {
	tk, found := d.Dispatcher.TicketFor(aggregateID, version)
	return hideParked(tk), found
}

func (d parkHidingDispatcher) Unrun(
	aggregateID string,
	version int64,
) error {
	return unparked(d.Dispatcher.Unrun(aggregateID, version))
}

func hideParked(
	tk asynxmd.Ticket,
) asynxmd.Ticket {
	errOf, unrunOf := tk.Err, tk.Unrun
	tk.Err = func() error { return unparked(errOf()) }
	tk.Unrun = func() error { return unparked(unrunOf()) }
	return tk
}

func unparked(
	err error,
) error {
	if errors.Is(err, asynxmd.ErrDeliveryParked) {
		return nil
	}
	return err
}

type parkIgnoringDispatcher struct {
	asynxmd.Dispatcher[Aggregate]
}

func queueWhileParked(
	inner asynxmd.Dispatcher[Aggregate],
) asynxmd.Dispatcher[Aggregate] {
	return parkIgnoringDispatcher{Dispatcher: inner}
}

func (d parkIgnoringDispatcher) Enqueue(
	ctx context.Context,
	r asynxmd.DispatchRequest[Aggregate],
) (asynxmd.Ticket, error) {
	_, isParked := d.Parks()[r.Event.AggregateID]
	isLiveWithAck := r.Kind == asynxmd.DispatchEvent && r.Ack != nil
	if !isParked || !isLiveWithAck {
		return d.Dispatcher.Enqueue(ctx, r)
	}
	r.Ack = nil
	return d.Dispatcher.Enqueue(ctx, r)
}

type parkingEverythingDispatcher struct {
	hollowDispatcher
}

func parkingEverything(
	asynxmd.DispatcherConfig[Aggregate],
) (asynxmd.Dispatcher[Aggregate], error) {
	return parkingEverythingDispatcher{}, nil
}

func (parkingEverythingDispatcher) Enqueue(
	context.Context,
	asynxmd.DispatchRequest[Aggregate],
) (asynxmd.Ticket, error) {
	done := make(chan struct{})
	close(done)
	return asynxmd.Ticket{
		Settled: done,
		Final:   done,
		Err:     func() error { return asynxmd.ErrDeliveryParked },
		Unrun:   func() error { return asynxmd.ErrDeliveryParked },
	}, nil
}

func (parkingEverythingDispatcher) TicketFor(
	string,
	int64,
) (asynxmd.Ticket, bool) {
	return asynxmd.Ticket{}, true
}

func (parkingEverythingDispatcher) Unrun(
	string,
	int64,
) error {
	return asynxmd.ErrDeliveryParked
}

func (parkingEverythingDispatcher) Parks() map[string]uint64 {
	return map[string]uint64{"a": 1, "ghost": 1}
}

func (parkingEverythingDispatcher) ResumeUndelivered(
	string,
	uint64,
) (bool, []int64) {
	return true, []int64{1}
}

func (parkingEverythingDispatcher) InHandler(
	context.Context,
	string,
) bool {
	return true
}

func (parkingEverythingDispatcher) Interrupted() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

func TestParkingScenarios_PassOnTheDefault(t *testing.T) {
	runGroup(t, parkingScenarios())
}

type recorder struct {
	testing.TB
	failed   atomic.Bool
	mu       sync.Mutex
	cleanups []func()
}

func (r *recorder) Helper() {}

func (r *recorder) Errorf(
	string,
	...any,
) {
	r.failed.Store(true)
}

func (r *recorder) Error(
	...any,
) {
	r.failed.Store(true)
}

func (r *recorder) Fatalf(
	string,
	...any,
) {
	r.failed.Store(true)
	runtime.Goexit()
}

func (r *recorder) Fatal(
	...any,
) {
	r.failed.Store(true)
	runtime.Goexit()
}

func (r *recorder) Cleanup(
	fn func(),
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanups = append(r.cleanups, fn)
}

func (r *recorder) cleanup() {
	r.mu.Lock()
	cleanups := slices.Clone(r.cleanups)
	r.mu.Unlock()
	slices.Reverse(cleanups)
	for _, fn := range cleanups {
		fn()
	}
}

func fails(
	check func(
		testing.TB,
	),
) bool {
	return (&recorder{}).run(func(
		t testing.TB,
		_ harness,
	) {
		check(t)
	}, harness{})
}

func (r *recorder) run(
	check scenario,
	h harness,
) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer r.cleanup()
		check(r, h)
	}()
	<-done
	return r.failed.Load()
}

var errRefused = errors.New("dispatchertest: refused")

type refusingDispatcher struct {
	hollowDispatcher
}

func refusing(
	asynxmd.DispatcherConfig[Aggregate],
) (asynxmd.Dispatcher[Aggregate], error) {
	return refusingDispatcher{}, nil
}

func (refusingDispatcher) Enqueue(
	context.Context,
	asynxmd.DispatchRequest[Aggregate],
) (asynxmd.Ticket, error) {
	return asynxmd.Ticket{}, errRefused
}

func (refusingDispatcher) Close(
	context.Context,
) error {
	return errRefused
}

type reorderingDispatcher struct {
	asynxmd.Dispatcher[Aggregate]
	mu   sync.Mutex
	held map[string]heldRequest
}

func reorder(
	inner asynxmd.Dispatcher[Aggregate],
) asynxmd.Dispatcher[Aggregate] {
	return &reorderingDispatcher{Dispatcher: inner, held: map[string]heldRequest{}}
}

func (d *reorderingDispatcher) Enqueue(
	ctx context.Context,
	r asynxmd.DispatchRequest[Aggregate],
) (asynxmd.Ticket, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	id := r.Event.AggregateID
	held, isHeld := d.held[id]
	if !isHeld {
		d.held[id] = heldRequest{ctx: ctx, request: r, tickets: make(chan asynxmd.Ticket, 1)}
		return d.held[id].ticket(), nil
	}
	delete(d.held, id)
	tk, err := d.Dispatcher.Enqueue(ctx, r)
	held.release(d.Dispatcher)
	return tk, err
}

func (d *reorderingDispatcher) flush() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, held := range d.held {
		held.release(d.Dispatcher)
	}
	clear(d.held)
}

func (d *reorderingDispatcher) WaitIdle() {
	d.flush()
	d.Dispatcher.WaitIdle()
}

func (d *reorderingDispatcher) Close(
	ctx context.Context,
) error {
	d.flush()
	return d.Dispatcher.Close(ctx)
}

type tally struct {
	mu     sync.Mutex
	wg     sync.WaitGroup
	failed map[string]bool
}

func (t *tally) run(
	name string,
	check scenario,
	h harness,
) {
	defer t.wg.Done()
	didFail := (&recorder{}).run(check, h)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failed[name] = didFail
}

type ticketHoardingDispatcher struct {
	asynxmd.Dispatcher[Aggregate]
	mu      *sync.Mutex
	tickets map[string]map[int64]asynxmd.Ticket
}

func hoardTickets(
	inner asynxmd.Dispatcher[Aggregate],
) asynxmd.Dispatcher[Aggregate] {
	return ticketHoardingDispatcher{
		Dispatcher: inner,
		mu:         &sync.Mutex{},
		tickets:    map[string]map[int64]asynxmd.Ticket{},
	}
}

func (d ticketHoardingDispatcher) Enqueue(
	ctx context.Context,
	r asynxmd.DispatchRequest[Aggregate],
) (asynxmd.Ticket, error) {
	tk, err := d.Dispatcher.Enqueue(ctx, r)
	if err != nil {
		return tk, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	id := r.Event.AggregateID
	if d.tickets[id] == nil {
		d.tickets[id] = map[int64]asynxmd.Ticket{}
	}
	d.tickets[id][r.Event.Version] = tk
	return tk, nil
}

func (d ticketHoardingDispatcher) TicketFor(
	aggregateID string,
	version int64,
) (asynxmd.Ticket, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	tk, found := d.tickets[aggregateID][version]
	return tk, found
}

func TestSuite_FlagsADispatcherWhoseTicketForNeverForgets(t *testing.T) {
	brokenCase{brokenBy(hoardTickets), []string{
		"TestTicketFor_FindsAnEventWhileItsDeliveryIsUnderWay",
		"TestTicketFor_FindsAnEventWhileItRetries",
		"TestTicketFor_ForgetsEventsLeftBehindByAPark",
		"TestTicketFor_ForgetsQueuedEventsWhenTheyAreParked",
	}}.check(t)
}

func TestTicketScenarios_PassOnTheDefault(t *testing.T) {
	runGroup(t, ticketScenarios())
}

type tombstoneDeadLetteringDispatcher struct {
	asynxmd.Dispatcher[Aggregate]
}

func deadLetterTombstones(
	inner asynxmd.Dispatcher[Aggregate],
) asynxmd.Dispatcher[Aggregate] {
	return tombstoneDeadLetteringDispatcher{Dispatcher: inner}
}

func (d tombstoneDeadLetteringDispatcher) Enqueue(
	ctx context.Context,
	r asynxmd.DispatchRequest[Aggregate],
) (asynxmd.Ticket, error) {
	if r.Kind != asynxmd.DispatchTombstone || r.Ack == nil {
		return d.Dispatcher.Enqueue(ctx, r)
	}
	r.Kind = asynxmd.DispatchEvent
	return d.Dispatcher.Enqueue(ctx, r)
}

type tombstoneRekindingDispatcher struct {
	asynxmd.Dispatcher[Aggregate]
}

func treatForgetNamesAsTombstones(
	inner asynxmd.Dispatcher[Aggregate],
) asynxmd.Dispatcher[Aggregate] {
	return tombstoneRekindingDispatcher{Dispatcher: inner}
}

func (d tombstoneRekindingDispatcher) Enqueue(
	ctx context.Context,
	r asynxmd.DispatchRequest[Aggregate],
) (asynxmd.Ticket, error) {
	if r.Kind != asynxmd.DispatchEvent || r.Event.EventName != asynxmd.ForgetEventName {
		return d.Dispatcher.Enqueue(ctx, r)
	}
	r.Kind = asynxmd.DispatchTombstone
	return d.Dispatcher.Enqueue(ctx, r)
}

func TestViews_ReceiveFailsTheScenarioWhenNothingArrives(t *testing.T) {
	if !fails(func(
		t testing.TB,
	) {
		receive(t, time.Millisecond, make(chan int), "nothing")
	}) {
		t.Fatal("receive did not fail on a silent channel")
	}
}

func TestViews_ShapeEventsForAssertions(t *testing.T) {
	events := []asynxmd.Event[Aggregate]{event("a", 1), event("b", 1), event("a", 2), event("a", 2)}
	if got := versionsOf(events); !slices.Equal(got, []int64{1, 1, 2, 2}) || distinct(got) != 2 {
		t.Fatalf("versions = %v, distinct %d", got, distinct(got))
	}
	want := map[string][]int64{"a": {1, 2, 2}, "b": {1}}
	if got := byAggregate(events); !maps.EqualFunc(got, want, slices.Equal) || !slices.Equal(sequence(3), []int64{1, 2, 3}) {
		t.Fatalf("byAggregate = %v, sequence(3) = %v", got, sequence(3))
	}
	if got := without([]string{"a/1", "b/1", "a/2"}, "b/"); !slices.Equal(got, []string{"a/1", "a/2"}) {
		t.Fatalf("without = %v", got)
	}
}

func TestViews_SubscriberNamesSkipsErrorsWithoutASubscriber(t *testing.T) {
	joined := errors.Join(&asynxmd.SubscriberError{Subscriber: "s#1", Err: errDown}, errDown)
	if got := subscriberNames(joined); !slices.Equal(got, []string{"s#1"}) {
		t.Fatalf("names = %v, want [s#1]", got)
	}
	if got := unjoin(errDown); len(got) != 1 {
		t.Fatalf("unjoin of a plain error = %v, want it alone", got)
	}
}

func TestViews_ParksAndResume(t *testing.T) {
	h := harness{factory: defaultFactory, patience: patience}
	d := h.build(t, config(&bus{}))
	d.Park("b", errImposed)
	d.Park("a", errImposed)
	if got := parked(d); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("parked = %v, want [a b]", got)
	}
	liftedOnce := resumeIfParked(d, "a") && !resumeIfParked(d, "a")
	if !liftedOnce || isClosed(make(chan struct{})) {
		t.Fatal("resume must lift a park once, and an open channel is not closed")
	}
}

// Event a2 comes after a1 finally succeeded, which took 3 failed attempts,
// and another aggregate is not held back by a's failure.
func TestDurable_FailedHandlerRetriesAndHoldsBackLaterEventsOfTheAggregate(t *testing.T) {
	var mu sync.Mutex
	var order []string
	var failuresLeft atomic.Int32
	failuresLeft.Store(3)

	var reports []int
	d := newDeliveryDurable(t, []asynxmd.DurableSub[int]{{Match: alwaysDelivery, Handler: func(
		_ context.Context,
		e asynxmd.Event[int],
	) error {
		mu.Lock()
		defer mu.Unlock()
		isFirstOfA := e.AggregateID == "a" && e.Version == 1
		if isFirstOfA && failuresLeft.Add(-1) >= 0 {
			order = append(order, "a1-fail")
			return errors.New("transient")
		}
		order = append(order, e.AggregateID+string(rune('0'+e.Version)))
		return nil
	}}}, withDeliveryErrorHandler[int](func(
		_ context.Context,
		_ asynxmd.Event[int],
		_ error,
		attempt int,
	) {
		mu.Lock()
		reports = append(reports, attempt)
		mu.Unlock()
	}))

	var acks atomic.Int32
	ack := func(context.Context) error { acks.Add(1); return nil }
	ctx := context.Background()
	enqueueDeliveryEvent(ctx, d, makeDeliveryEvent("a", 1), ack)
	enqueueDeliveryEvent(ctx, d, makeDeliveryEvent("a", 2), ack)
	doneB, _ := enqueueDeliveryEvent(ctx, d, makeDeliveryEvent("b", 1), ack)

	<-doneB
	waitForDelivery(t, "all three deliveries acked", func() bool { return acks.Load() == 3 })

	mu.Lock()
	defer mu.Unlock()
	idx := map[string]int{}
	for i, name := range order {
		idx[name] = i
	}
	a1, ok1 := idx["a1"]
	a2, ok2 := idx["a2"]
	deliveredInOrder := ok1 && ok2 && a2 >= a1
	if !deliveredInOrder {
		t.Fatalf("a2 was delivered before a1 succeeded: %v", order)
	}
	if slices.ContainsFunc(order[:a1], deliveredBeforeA1) {
		t.Fatalf("unexpected delivery before a1 succeeded: %v", order)
	}
	if !slices.Equal(reports, []int{1, 2, 3}) {
		t.Fatalf("delivery error reports = %v, want attempts [1 2 3]", reports)
	}
}

func TestDurable_DoneClosesAfterTheFirstAttemptEvenWhenItFails(t *testing.T) {
	release := make(chan struct{})
	d := newDeliveryDurable(t, []asynxmd.DurableSub[int]{{Match: alwaysDelivery, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		select {
		case <-release:
			return nil
		default:
			return errors.New("not yet")
		}
	}}})

	var acked atomic.Bool
	done, err := enqueueDeliveryEvent(context.Background(), d, makeDeliveryEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("done did not close after the first (failed) attempt")
	}
	if acked.Load() {
		t.Fatal("acked a delivery whose handler failed")
	}

	close(release)
	waitForDelivery(t, "ack after the retry succeeded", acked.Load)
}

func TestDurable_PanicIsAFailureThatRetriesAndReachesThePanicHandler(t *testing.T) {
	var attempts, panics atomic.Int32
	var lastErr atomic.Value
	d := newDeliveryDurable(t, []asynxmd.DurableSub[int]{{Match: alwaysDelivery, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		if attempts.Add(1) == 1 {
			panic("boom")
		}
		return nil
	}}},
		withDeliveryPanicHandler[int](func(
			context.Context,
			asynxmd.Event[int],
			any,
		) {
			panics.Add(1)
		}),
		withDeliveryErrorHandler[int](func(
			_ context.Context,
			_ asynxmd.Event[int],
			err error,
			_ int,
		) {
			lastErr.Store(err)
		}),
	)

	var acked atomic.Bool
	enqueueDeliveryEvent(context.Background(), d, makeDeliveryEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	waitForDelivery(t, "ack after panic then success", acked.Load)

	if panics.Load() != 1 {
		t.Errorf("panic handler calls = %d, want 1", panics.Load())
	}
	if err, _ := lastErr.Load().(error); !errors.Is(err, asynxmd.ErrHandlerPanic) {
		t.Errorf("reported error = %v, want ErrHandlerPanic", err)
	}
}

func TestDurable_APanickingPanicHandlerCannotKillTheWorker(t *testing.T) {
	var attempts atomic.Int32
	d := newDeliveryDurable(t, []asynxmd.DurableSub[int]{{Match: alwaysDelivery, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		if attempts.Add(1) == 1 {
			panic("boom")
		}
		return nil
	}}}, withDeliveryPanicHandler[int](func(
		context.Context,
		asynxmd.Event[int],
		any,
	) {
		panic("handler of handlers")
	}))

	var acked atomic.Bool
	enqueueDeliveryEvent(context.Background(), d, makeDeliveryEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	waitForDelivery(t, "recovery from a panicking panic handler", acked.Load)
}

// The timeout is reported, and the delivery still waits for a handler that
// outlives it.
func TestDurable_TimeoutIsReportedButTheHandlerIsStillWaitedFor(t *testing.T) {
	var running, overlapped atomic.Bool
	var timeouts atomic.Int32
	timedOut := make(chan struct{}, 8)
	d := newDeliveryDurable(t, []asynxmd.DurableSub[int]{{
		Match:   alwaysDelivery,
		Timeout: 10 * time.Millisecond,
		Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			if !running.CompareAndSwap(false, true) {
				overlapped.Store(true)
			}
			<-timedOut
			running.Store(false)
			return nil
		},
	}}, withDeliveryErrorHandler[int](func(
		_ context.Context,
		_ asynxmd.Event[int],
		err error,
		_ int,
	) {
		if errors.Is(err, asynxmd.ErrHandlerTimeout) {
			timeouts.Add(1)
			timedOut <- struct{}{}
		}
	}))

	var acks atomic.Int32
	ack := func(context.Context) error { acks.Add(1); return nil }
	enqueueDeliveryEvent(context.Background(), d, makeDeliveryEvent("a", 1), ack)
	enqueueDeliveryEvent(context.Background(), d, makeDeliveryEvent("a", 2), ack)
	waitForDelivery(t, "both acked", func() bool { return acks.Load() == 2 })

	if overlapped.Load() {
		t.Error("the next event's handler started while the timed-out one was still running")
	}
	if timeouts.Load() != 2 {
		t.Errorf("timeouts reported = %d, want 2", timeouts.Load())
	}
}

func TestDurable_OnlyFailedHandlersAreRetried(t *testing.T) {
	var goodRuns, flakyRuns atomic.Int32
	d := newDeliveryDurable(t, []asynxmd.DurableSub[int]{
		{Match: alwaysDelivery, Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			goodRuns.Add(1)
			return nil
		}},
		{Match: alwaysDelivery, Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			if flakyRuns.Add(1) < 3 {
				return errors.New("flaky")
			}
			return nil
		}},
		{Match: func(string) bool { return false }, Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			t.Error("a non-matching subscriber ran")
			return nil
		}},
	})

	var acked atomic.Bool
	enqueueDeliveryEvent(context.Background(), d, makeDeliveryEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	waitForDelivery(t, "ack", acked.Load)

	if goodRuns.Load() != 1 || flakyRuns.Load() != 3 {
		t.Fatalf("good ran %d times, flaky %d; want 1 and 3", goodRuns.Load(), flakyRuns.Load())
	}
}

func TestDurable_CloseInterruptsRetriesAndLeavesTheDeliveryUnacked(t *testing.T) {
	var attempts atomic.Int32
	d := newDeliveryTestDispatcher[int](&recordingDeliveryBus{},
		withDeliveryDurableSubs([]asynxmd.DurableSub[int]{{Match: alwaysDelivery, Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			attempts.Add(1)
			return errors.New("always failing")
		}}}),
		withDeliveryRetryBackoff[int](time.Hour, time.Hour),
	)

	var acked atomic.Bool
	done, _ := enqueueDeliveryEvent(context.Background(), d, makeDeliveryEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	<-done

	closed := make(chan error, 1)
	go func() { closed <- d.Close(context.Background()) }()
	select {
	case err := <-closed:
		checkClosed(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung on a retrying delivery")
	}
	if acked.Load() {
		t.Fatal("a failing delivery was acknowledged on close")
	}
}

func TestDurable_DeliveryWithNoMatchingSubscriberIsStillAcked(t *testing.T) {
	d := newDeliveryDurable(t, nil)
	var acked atomic.Bool
	done, _ := enqueueDeliveryEvent(context.Background(), d, makeDeliveryEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	<-done
	if !acked.Load() {
		t.Fatal("delivery with nothing to deliver was not acknowledged")
	}
}

func TestDurable_HandlerFinishingBeforeItsTimeoutReportsNothing(t *testing.T) {
	var reports atomic.Int32
	d := newDeliveryDurable(t, []asynxmd.DurableSub[int]{{
		Match:   alwaysDelivery,
		Timeout: time.Second,
		Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			return nil
		},
	}}, withDeliveryErrorHandler[int](func(
		context.Context,
		asynxmd.Event[int],
		error,
		int,
	) {
		reports.Add(1)
	}))

	var acked atomic.Bool
	enqueueDeliveryEvent(context.Background(), d, makeDeliveryEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	waitForDelivery(t, "ack", acked.Load)
	if reports.Load() != 0 {
		t.Fatalf("reports = %d, want 0", reports.Load())
	}
}

func TestDurable_ATimedOutHandlerThatThenFailsIsRetried(t *testing.T) {
	var attempts atomic.Int32
	timedOut := make(chan struct{}, 1)
	d := newDeliveryDurable(t, []asynxmd.DurableSub[int]{{
		Match:   alwaysDelivery,
		Timeout: 5 * time.Millisecond,
		Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			if attempts.Add(1) == 1 {
				<-timedOut
				return errors.New("failed late")
			}
			return nil
		},
	}}, withDeliveryErrorHandler[int](func(
		_ context.Context,
		_ asynxmd.Event[int],
		err error,
		_ int,
	) {
		if errors.Is(err, asynxmd.ErrHandlerTimeout) {
			timedOut <- struct{}{}
		}
	}))

	var acked atomic.Bool
	enqueueDeliveryEvent(context.Background(), d, makeDeliveryEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	waitForDelivery(t, "ack after the late failure was retried", acked.Load)
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want 2", attempts.Load())
	}
}

func TestShutdownDuringRetry_NeverDeliversALaterEventPastTheFailingOne(t *testing.T) {
	var rec deliveriesDelivery
	attempted := make(chan struct{}, 64)
	d := newDeliveryTestDispatcher[int](&recordingDeliveryBus{},
		withDeliveryDurableSubs([]asynxmd.DurableSub[int]{{Match: alwaysDelivery, Handler: func(
			_ context.Context,
			e asynxmd.Event[int],
		) error {
			rec.add(e)
			if e.Version == 1 {
				attempted <- struct{}{}
				return errors.New("poison")
			}
			return nil
		}}}),
		withDeliveryRetryBackoff[int](time.Hour, time.Hour),
	)

	var acked sync.Map
	ctx := context.Background()
	t1, _ := enqueueDeliveryTicket(ctx, d, makeDeliveryEvent("a", 1), ackDeliveryInto(&acked, makeDeliveryEvent("a", 1)))
	t2, _ := enqueueDeliveryTicket(ctx, d, makeDeliveryEvent("a", 2), ackDeliveryInto(&acked, makeDeliveryEvent("a", 2)))
	recvDelivery(t, attempted, "first attempt of v1")

	if err := d.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for v, got := range map[int]error{1: finalDelivery(t, t1, "v1 final"), 2: finalDelivery(t, t2, "v2 final")} {
		checkNotDelivered(t, v, got)
	}
	for _, v := range []int64{1, 2} {
		checkNotAcked(t, &acked, v)
	}
	if slices.Contains(rec.list(), "a/2") {
		t.Fatalf("v2 was delivered while v1 was still pending: %v", rec.list())
	}
}

// A later event of the parked aggregate is not queued either, while another
// aggregate keeps flowing.
func TestExhaustedAttempts_ParkTheAggregateAndLeaveOthersAlone(t *testing.T) {
	var attempts atomic.Int32
	var rec deliveriesDelivery
	var mu sync.Mutex
	var reports []error
	d := newDeliveryDurable(t, []asynxmd.DurableSub[int]{{Match: alwaysDelivery, Handler: func(
		_ context.Context,
		e asynxmd.Event[int],
	) error {
		rec.add(e)
		if e.AggregateID == "a" && e.Version == 1 {
			attempts.Add(1)
			return errors.New("poison")
		}
		return nil
	}}},
		withDeliveryMaxAttempts[int](4),
		withDeliveryErrorHandler[int](func(
			_ context.Context,
			_ asynxmd.Event[int],
			err error,
			_ int,
		) {
			mu.Lock()
			reports = append(reports, err)
			mu.Unlock()
		}),
	)

	var acked sync.Map
	ctx := context.Background()
	t1, _ := enqueueDeliveryTicket(ctx, d, makeDeliveryEvent("a", 1), ackDeliveryInto(&acked, makeDeliveryEvent("a", 1)))
	t2, _ := enqueueDeliveryTicket(ctx, d, makeDeliveryEvent("a", 2), ackDeliveryInto(&acked, makeDeliveryEvent("a", 2)))

	if err := finalDelivery(t, t1, "v1 final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v1 final = %v, want ErrDeliveryParked", err)
	}
	if err := finalDelivery(t, t2, "v2 final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v2 final = %v, want ErrDeliveryParked", err)
	}
	if n := attempts.Load(); n != 4 {
		t.Fatalf("attempts = %d, want exactly the cap (4)", n)
	}
	if got := haltedDelivery(d); len(got) != 1 || got[0] != "a" {
		t.Fatalf("Halted = %v, want [a]", got)
	}
	if slices.Contains(rec.list(), "a/2") {
		t.Fatal("v2 delivered although v1 was parked")
	}

	t3, _ := enqueueDeliveryTicket(ctx, d, makeDeliveryEvent("a", 3), ackDeliveryInto(&acked, makeDeliveryEvent("a", 3)))
	if err := finalDelivery(t, t3, "v3 final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v3 final = %v, want ErrDeliveryParked", err)
	}

	tb, _ := enqueueDeliveryTicket(ctx, d, makeDeliveryEvent("b", 1), ackDeliveryInto(&acked, makeDeliveryEvent("b", 1)))
	if err := finalDelivery(t, tb, "b final"); err != nil {
		t.Fatalf("aggregate b held back: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	parkedReports := len(slices.DeleteFunc(slices.Clone(reports), notParked))
	if parkedReports != 1 {
		t.Fatalf("parked reports = %d, want 1 (%v)", parkedReports, reports)
	}
}

func TestDeadLetter_AcceptingTheEventSkipsItAndLetsLaterEventsFlow(t *testing.T) {
	var rec deliveriesDelivery
	var dead atomic.Value
	d := newDeliveryDurable(t, []asynxmd.DurableSub[int]{{Match: alwaysDelivery, Handler: func(
		_ context.Context,
		e asynxmd.Event[int],
	) error {
		rec.add(e)
		if e.Version == 1 {
			return errors.New("poison")
		}
		return nil
	}}},
		withDeliveryMaxAttempts[int](3),
		withDeadLetterHandler[int](func(
			_ context.Context,
			e asynxmd.Event[int],
			err error,
		) error {
			dead.Store(e.Version)
			return nil
		}),
	)

	var acked sync.Map
	ctx := context.Background()
	t1, _ := enqueueDeliveryTicket(ctx, d, makeDeliveryEvent("a", 1), ackDeliveryInto(&acked, makeDeliveryEvent("a", 1)))
	t2, _ := enqueueDeliveryTicket(ctx, d, makeDeliveryEvent("a", 2), ackDeliveryInto(&acked, makeDeliveryEvent("a", 2)))

	if err := finalDelivery(t, t1, "v1"); err != nil {
		t.Fatalf("dead-lettered event final = %v, want nil", err)
	}
	if err := finalDelivery(t, t2, "v2"); err != nil {
		t.Fatalf("v2 final = %v", err)
	}
	if dead.Load() != int64(1) {
		t.Fatalf("dead letter saw %v, want v1", dead.Load())
	}
	if _, ok := acked.Load(int64(1)); !ok {
		t.Fatal("dead-lettered event stayed pending")
	}
	if len(haltedDelivery(d)) != 0 {
		t.Fatalf("aggregate parked despite the dead-letter handler: %v", haltedDelivery(d))
	}
}

func TestDeadLetter_RefusalParksTheAggregate(t *testing.T) {
	d := newDeliveryDurable(t, []asynxmd.DurableSub[int]{{Match: alwaysDelivery, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		return errors.New("poison")
	}}},
		withDeliveryMaxAttempts[int](2),
		withDeadLetterHandler[int](func(
			context.Context,
			asynxmd.Event[int],
			error,
		) error {
			panic("dead-letter store down")
		}),
	)
	tk, _ := enqueueDeliveryTicket(context.Background(), d, makeDeliveryEvent("a", 1), func(context.Context) error { return nil })
	if err := finalDelivery(t, tk, "final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
}

// Parking after a failed tombstone is Forget's job, not the dispatcher's, so
// the final ticket only reports the failure.
func TestTombstone_FinalReportsADurableSubscriberThatKeepsFailing(t *testing.T) {
	d := newDeliveryDurable(t, []asynxmd.DurableSub[int]{{Match: func(string) bool { return false },
		Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			return errors.New("cannot forget")
		}}},
		withDeliveryMaxAttempts[int](2))
	tomb := makeDeliveryEvent("a", 2)
	tomb.EventName = asynxmd.ForgetEventName
	tk, _ := enqueueDeliveryForget(context.Background(), d, tomb, nil)
	if err := finalDelivery(t, tk, "final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
}

func TestEnqueueForget_IsNeverDeadLettered(t *testing.T) {
	var dead atomic.Int32
	d := newDeliveryDurable(t, []asynxmd.DurableSub[int]{{Match: alwaysDelivery, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		return errors.New("cannot forget")
	}}}, withDeliveryMaxAttempts[int](2), withDeadLetterHandler[int](func(
		context.Context,
		asynxmd.Event[int],
		error,
	) error {
		dead.Add(1)
		return nil
	}))
	marker := makeDeliveryEvent("a", 2)
	marker.EventName = asynxmd.ForgetEventName
	tk, _ := enqueueDeliveryForget(context.Background(), d, marker, func(context.Context) error { return nil })
	if err := finalDelivery(t, tk, "marker"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked: a dead letter must not let Forget erase", err)
	}
	if dead.Load() != 0 {
		t.Fatalf("dead-letter handler called %d times for the marker", dead.Load())
	}
}

// A Forget that abandons the delivery while its last attempt runs leaves the
// aggregate unparked.
func TestGiveUp_AfterASupersedeDoesNotParkTheAggregateAgain(t *testing.T) {
	inHandler := make(chan struct{})
	release := make(chan struct{})
	var dead atomic.Int32
	d := newDeliveryDurable(t, []asynxmd.DurableSub[int]{{Match: alwaysDelivery, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		inHandler <- struct{}{}
		<-release
		return errors.New("down")
	}}}, withDeliveryMaxAttempts[int](1), withDeadLetterHandler[int](func(
		context.Context,
		asynxmd.Event[int],
		error,
	) error {
		dead.Add(1)
		return nil
	}))
	tk, _ := enqueueDeliveryTicket(context.Background(), d, makeDeliveryEvent("a", 1), func(context.Context) error { return nil })
	recvDelivery(t, inHandler, "last attempt running")

	d.Supersede("a")
	close(release)
	if err := finalDelivery(t, tk, "superseded delivery"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v", err)
	}
	if got := haltedDelivery(d); len(got) != 0 {
		t.Fatalf("halted = %v: the exhausted attempt re-parked an aggregate that was superseded", got)
	}
	if dead.Load() != 0 {
		t.Fatal("a superseded delivery was dead-lettered")
	}
}

func TestInterrupt_AfterASupersedeDoesNotParkTheAggregate(t *testing.T) {
	attempted := make(chan struct{}, 4)
	d := newDeliveryDurable(t, []asynxmd.DurableSub[int]{{Match: alwaysDelivery, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		attempted <- struct{}{}
		return errors.New("down")
	}}}, withDeliveryRetryBackoff[int](time.Hour, time.Hour), withDeliveryMaxAttempts[int](100))
	tk, _ := enqueueDeliveryTicket(context.Background(), d, makeDeliveryEvent("a", 1), func(context.Context) error { return nil })
	recvDelivery(t, attempted, "first attempt")
	d.Supersede("a")
	finalDelivery(t, tk, "superseded")
	d.Interrupt()
	if got := haltedDelivery(d); len(got) != 0 {
		t.Fatalf("halted = %v", got)
	}
}

func TestDeliveryErrorHandlerPanicDoesNotReleaseTheHold(t *testing.T) {
	var rec deliveriesDelivery
	var failures atomic.Int32
	failures.Store(2)
	d := newDeliveryDurable(t, []asynxmd.DurableSub[int]{{Match: alwaysDelivery, Handler: func(
		_ context.Context,
		e asynxmd.Event[int],
	) error {
		rec.add(e)
		if e.Version == 1 && failures.Add(-1) >= 0 {
			return errors.New("down")
		}
		return nil
	}}}, withDeliveryErrorHandler[int](func(
		context.Context,
		asynxmd.Event[int],
		error,
		int,
	) {
		panic("boom")
	}))

	ctx := context.Background()
	ack := func(context.Context) error { return nil }
	enqueueDeliveryTicket(ctx, d, makeDeliveryEvent("a", 1), ack)
	last, _ := enqueueDeliveryTicket(ctx, d, makeDeliveryEvent("a", 2), ack)
	if err := finalDelivery(t, last, "v2"); err != nil {
		t.Fatalf("v2 = %v", err)
	}
	want := []string{"a/1", "a/1", "a/1", "a/2"}
	if got := rec.list(); !slices.Equal(got, want) {
		t.Fatalf("delivered %v, want %v: a panicking handler released the hold", got, want)
	}
}

// Whoever is told that a delivery gave up may act on it at once, for instance by
// resuming; the aggregate must already be parked by then.
func TestGiveUp_TheAggregateIsParkedBeforeTheErrorHandlerIsTold(t *testing.T) {
	var d asynxmd.Dispatcher[int]
	parkedWhenTold := make(chan []string, 1)
	d = newDeliveryDurable(t,
		[]asynxmd.DurableSub[int]{{Match: alwaysDelivery, Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			return errors.New("down")
		}}},
		withDeliveryMaxAttempts[int](1),
		withDeliveryErrorHandler[int](func(
			_ context.Context,
			_ asynxmd.Event[int],
			err error,
			_ int,
		) {
			if errors.Is(err, asynxmd.ErrDeliveryParked) {
				parkedWhenTold <- haltedDelivery(d)
			}
		}))

	enqueueDeliveryTicket(context.Background(), d, makeDeliveryEvent("a", 1), func(context.Context) error { return nil })
	if got := recvDelivery(t, parkedWhenTold, "the park to be reported"); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("parked aggregates when the handler was told = %v, want [a]", got)
	}
}

func TestDurable_FailuresNameTheSubscriberAndDeadLetterGetsThemAll(t *testing.T) {
	billing, search := errors.New("billing down"), errors.New("search down")
	var mu sync.Mutex
	var reported []error
	deadLettered := make(chan error, 1)
	d := newDeliveryDurable(t, []asynxmd.DurableSub[int]{
		{Name: "OrderUpdated#1", Match: alwaysDelivery, Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			return billing
		}},
		{Name: "OrderUpdated#2", Match: alwaysDelivery, Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			return search
		}},
	},
		withDeliveryMaxAttempts[int](2),
		withDeliveryErrorHandler[int](func(
			_ context.Context,
			_ asynxmd.Event[int],
			err error,
			_ int,
		) {
			mu.Lock()
			reported = append(reported, err)
			mu.Unlock()
		}),
		withDeadLetterHandler[int](func(
			_ context.Context,
			_ asynxmd.Event[int],
			err error,
		) error {
			deadLettered <- err
			return nil
		}),
	)

	ticket, err := enqueueDeliveryTicket(context.Background(), d, makeDeliveryEvent("a", 1), func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := finalDelivery(t, ticket, "dead-lettered delivery"); err != nil {
		t.Fatalf("delivery ended with %v", err)
	}

	got := recvDelivery(t, deadLettered, "the dead-letter handler")
	if !errors.Is(got, billing) || !errors.Is(got, search) {
		t.Fatalf("dead-letter handler learned of %q, want both failures", got)
	}
	var names []string
	for _, part := range unjoinDelivery(got) {
		names = appendDeliverySubscriber(names, part)
	}
	if !slices.Equal(names, []string{"OrderUpdated#1", "OrderUpdated#2"}) {
		t.Fatalf("subscribers named in the dead-letter error = %v", names)
	}

	mu.Lock()
	defer mu.Unlock()
	var sub *asynxmd.SubscriberError
	namesFirstSubscriber := len(reported) > 0 && errors.As(reported[0], &sub) && sub.Subscriber == "OrderUpdated#1"
	if !namesFirstSubscriber {
		t.Fatalf("delivery error handler got %v, want the first failure wrapped with its subscriber", reported)
	}
}

func TestDurable_ATimeoutNamesItsSubscriber(t *testing.T) {
	release := make(chan struct{})
	reported := make(chan error, 4)
	d := newDeliveryDurable(t, []asynxmd.DurableSub[int]{{
		Name:    "slow#1",
		Match:   alwaysDelivery,
		Timeout: time.Millisecond,
		Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			<-release
			return nil
		},
	}}, withDeliveryErrorHandler[int](func(
		_ context.Context,
		_ asynxmd.Event[int],
		err error,
		_ int,
	) {
		reported <- err
	}))

	ticket, err := enqueueDeliveryTicket(context.Background(), d, makeDeliveryEvent("a", 1), func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	got := recvDelivery(t, reported, "the timeout report")
	close(release)
	<-ticket.Final

	var sub *asynxmd.SubscriberError
	namesSlow := errors.As(got, &sub) && sub.Subscriber == "slow#1"
	if !namesSlow || !errors.Is(got, asynxmd.ErrHandlerTimeout) {
		t.Fatalf("timeout reported as %q, want ErrHandlerTimeout from subscriber slow#1", got)
	}
}

func makeDeliveryEvent(
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

func newDeliveryTestDispatcher[T any](
	bus asynxmd.Bus[T],
	opts ...testDeliveryOpt[T],
) asynxmd.Dispatcher[T] {
	c := testDeliveryConfig[T]{cfg: asynxmd.DispatcherConfig[T]{
		Bus:          bus,
		RetryInitial: 100 * time.Millisecond,
		RetryMax:     30 * time.Second,
		MaxAttempts:  20,
	}}
	for _, opt := range opts {
		opt(&c)
	}
	return New(c.cfg, c.opts...)
}

func withDeliveryDurableSubs[T any](
	subs []asynxmd.DurableSub[T],
) testDeliveryOpt[T] {
	return func(
		c *testDeliveryConfig[T],
	) {
		c.cfg.DurableSubs = subs
	}
}

func withDeliveryErrorHandler[T any](
	fn asynxmd.DeliveryErrorHandler[T],
) testDeliveryOpt[T] {
	return func(
		c *testDeliveryConfig[T],
	) {
		c.cfg.OnDeliveryError = fn
	}
}

func withDeliveryPanicHandler[T any](
	fn asynxmd.PanicHandler[T],
) testDeliveryOpt[T] {
	return func(
		c *testDeliveryConfig[T],
	) {
		c.cfg.OnPanic = fn
	}
}

func withDeadLetterHandler[T any](
	fn asynxmd.DeadLetterHandler[T],
) testDeliveryOpt[T] {
	return func(
		c *testDeliveryConfig[T],
	) {
		c.cfg.DeadLetter = fn
	}
}

func withDeliveryRetryBackoff[T any](
	initial time.Duration,
	max time.Duration,
) testDeliveryOpt[T] {
	return func(
		c *testDeliveryConfig[T],
	) {
		c.cfg.RetryInitial, c.cfg.RetryMax = initial, max
	}
}

func withDeliveryMaxAttempts[T any](
	n int,
) testDeliveryOpt[T] {
	return func(
		c *testDeliveryConfig[T],
	) {
		c.cfg.MaxAttempts = n
	}
}

func enqueueDeliveryEvent[T any](
	ctx context.Context,
	d asynxmd.Dispatcher[T],
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (<-chan struct{}, error) {
	t, err := enqueueDeliveryTicket(ctx, d, event, ack)
	return t.Settled, err
}

func enqueueDeliveryTicket[T any](
	ctx context.Context,
	d asynxmd.Dispatcher[T],
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (asynxmd.Ticket, error) {
	return d.Enqueue(ctx, asynxmd.DispatchRequest[T]{Event: event, Ack: ack, Kind: asynxmd.DispatchEvent})
}

func enqueueDeliveryForget[T any](
	ctx context.Context,
	d asynxmd.Dispatcher[T],
	tombstone asynxmd.Event[T],
	ack func(context.Context) error,
) (asynxmd.Ticket, error) {
	return d.Enqueue(ctx, asynxmd.DispatchRequest[T]{Event: tombstone, Ack: ack, Kind: asynxmd.DispatchTombstone})
}

func haltedDelivery[T any](
	d asynxmd.Dispatcher[T],
) []string {
	return slices.Sorted(maps.Keys(d.Parks()))
}

func alwaysDelivery(string) bool { return true }

func newDeliveryDurable(
	t *testing.T,
	subs []asynxmd.DurableSub[int],
	opts ...testDeliveryOpt[int],
) asynxmd.Dispatcher[int] {
	t.Helper()
	all := append([]testDeliveryOpt[int]{withDeliveryDurableSubs(subs), withDeliveryRetryBackoff[int](time.Millisecond, 5*time.Millisecond)}, opts...)
	d := newDeliveryTestDispatcher[int](&recordingDeliveryBus{}, all...)
	t.Cleanup(func() { d.Close(context.Background()) })
	return d
}

func waitForDelivery(
	t *testing.T,
	what string,
	cond func() bool,
) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		failPastDelivery(t, deadline, what)
		runtime.Gosched()
	}
}

func recvDelivery[V any](
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

func finalDelivery(
	t *testing.T,
	tk asynxmd.Ticket,
	what string,
) error {
	t.Helper()
	recvDelivery(t, tk.Final, what)
	return tk.Err()
}

func ackDeliveryInto(
	acked *sync.Map,
	e asynxmd.Event[int],
) func(context.Context) error {
	return func(context.Context) error { acked.Store(e.Version, true); return nil }
}

func unjoinDelivery(err error) []error {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		return joined.Unwrap()
	}
	return []error{err}
}

func failPastDelivery(
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

func appendDeliverySubscriber(
	names []string,
	part error,
) []string {
	var sub *asynxmd.SubscriberError
	if !errors.As(part, &sub) {
		return names
	}
	return append(names, sub.Subscriber)
}

type recordingDeliveryBus struct {
	mu    sync.Mutex
	calls []asynxmd.Event[int]
	delay time.Duration
}

func (b *recordingDeliveryBus) PublishSync(
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

func (b *recordingDeliveryBus) Publish(
	context.Context,
	asynxmd.Event[int],
) error {
	return nil
}

func (b *recordingDeliveryBus) Subscribe(
	string,
	asynxmd.ProjectionHandler[int],
	...asynxmd.SubscriptionOpt[int],
) (string, error) {
	return "", nil
}

func (b *recordingDeliveryBus) Unsubscribe(string) error { return nil }

func (b *recordingDeliveryBus) Close(context.Context) error { return nil }

func (b *recordingDeliveryBus) WaitForHandlers() {}

func (b *recordingDeliveryBus) snapshot() []asynxmd.Event[int] {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]asynxmd.Event[int], len(b.calls))
	copy(out, b.calls)
	return out
}

type deliveriesDelivery struct {
	mu   sync.Mutex
	seen []string
}

func (r *deliveriesDelivery) add(e asynxmd.Event[int]) {
	r.mu.Lock()
	r.seen = append(r.seen, e.AggregateID+"/"+string(rune('0'+e.Version)))
	r.mu.Unlock()
}

func (r *deliveriesDelivery) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.seen...)
}

type testDeliveryConfig[T any] struct {
	cfg  asynxmd.DispatcherConfig[T]
	opts []Opt[T]
}

type testDeliveryOpt[T any] func(
	c *testDeliveryConfig[T],
)

func TestDispatch_OrderingGuarantee(t *testing.T) {
	bus := &recordingWorkerBus{}
	d := newWorkerTestDispatcher[int](bus)

	ctx := context.Background()
	for i := int64(1); i <= 10; i++ {
		dispatchOrFail(t, d, i)
	}

	if err := d.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	calls := bus.snapshot()
	if len(calls) != 10 {
		t.Fatalf("expected 10 calls, got %d", len(calls))
	}
	for i, e := range calls {
		checkWorkerCallVersion(t, i, e)
	}
}

// agg-fast, dispatched second, finishes before agg-slow, which has a 50ms
// delay.
func TestDispatch_CrossAggregateIndependence(t *testing.T) {
	var (
		mu       sync.Mutex
		finished []string
	)

	bus := &callbackWorkerBus{fn: func(
		_ context.Context,
		e asynxmd.Event[int],
	) error {
		if e.AggregateID == "agg-slow" {
			time.Sleep(50 * time.Millisecond)
		}
		mu.Lock()
		finished = append(finished, e.AggregateID)
		mu.Unlock()
		return nil
	}}

	d := newWorkerTestDispatcher[int](bus)
	ctx := context.Background()

	_ = dispatchWorker(ctx, d, makeWorkerEvent("agg-slow", 1), false)
	_ = dispatchWorker(ctx, d, makeWorkerEvent("agg-fast", 1), false)

	_ = d.Close(ctx)

	mu.Lock()
	defer mu.Unlock()
	if len(finished) != 2 {
		t.Fatalf("expected 2 finished, got %d", len(finished))
	}
	if finished[0] != "agg-fast" {
		t.Errorf("expected agg-fast to finish first, got %v", finished)
	}
}

func TestDispatch_SyncBlocking(t *testing.T) {
	delay := 30 * time.Millisecond
	bus := &recordingWorkerBus{delay: delay}
	d := newWorkerTestDispatcher[int](bus)
	ctx := context.Background()

	start := time.Now()
	_ = dispatchWorker(ctx, d, makeWorkerEvent("agg-1", 1), true)
	elapsed := time.Since(start)

	if elapsed < delay {
		t.Errorf("expected blocking for at least %v, returned in %v", delay, elapsed)
	}
	_ = d.Close(ctx)
}

func TestDispatch_AsyncNonBlocking(t *testing.T) {
	bus := &recordingWorkerBus{delay: 100 * time.Millisecond}
	d := newWorkerTestDispatcher[int](bus)
	ctx := context.Background()

	start := time.Now()
	_ = dispatchWorker(ctx, d, makeWorkerEvent("agg-1", 1), false)
	elapsed := time.Since(start)

	if elapsed > 20*time.Millisecond {
		t.Errorf("expected non-blocking return, took %v", elapsed)
	}
	_ = d.Close(ctx)
}

func TestDispatch_NilBusNoPanic(t *testing.T) {
	d := newWorkerTestDispatcher[int](nil)
	ctx := context.Background()

	if err := dispatchWorker(ctx, d, makeWorkerEvent("agg-1", 1), true); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = d.Close(ctx)
}

func TestDispatch_PublishErrorHandler(t *testing.T) {
	publishErr := errors.New("publish failed")
	bus := &errBus{err: publishErr}

	var (
		mu            sync.Mutex
		gotEvent      asynxmd.Event[int]
		gotErr        error
		handlerCalled bool
	)

	handler := func(
		_ context.Context,
		e asynxmd.Event[int],
		err error,
	) {
		mu.Lock()
		gotEvent = e
		gotErr = err
		handlerCalled = true
		mu.Unlock()
	}

	d := newWorkerTestDispatcher[int](bus, withPublishErrorHandler[int](handler))
	ctx := context.Background()

	ev := makeWorkerEvent("agg-1", 1)
	_ = dispatchWorker(ctx, d, ev, true)
	_ = d.Close(ctx)

	mu.Lock()
	defer mu.Unlock()
	if !handlerCalled {
		t.Fatal("error handler was not called")
	}
	if !errors.Is(gotErr, publishErr) {
		t.Errorf("expected %v, got %v", publishErr, gotErr)
	}
	if gotEvent.AggregateID != "agg-1" {
		t.Errorf("expected agg-1, got %s", gotEvent.AggregateID)
	}
}

// Three aggregates interleave five events each, and each aggregate sees its
// own events in order.
func TestDispatch_MultipleAggregatesOrdered(t *testing.T) {
	bus := &recordingWorkerBus{}
	d := newWorkerTestDispatcher[int](bus)
	ctx := context.Background()

	for v := int64(1); v <= 5; v++ {
		dispatchToEach(ctx, d, v, "a", "b", "c")
	}

	_ = d.Close(ctx)

	calls := bus.snapshot()
	if len(calls) != 15 {
		t.Fatalf("expected 15 calls, got %d", len(calls))
	}

	byAgg := make(map[string][]int64)
	for _, e := range calls {
		byAgg[e.AggregateID] = append(byAgg[e.AggregateID], e.Version)
	}

	for agg, versions := range byAgg {
		checkAggregateOrdered(t, agg, versions)
	}
}

// Events of one aggregate dispatched concurrently are each delivered once.
func TestDispatch_ConcurrentSameAggregate(t *testing.T) {
	bus := &recordingWorkerBus{}
	d := newWorkerTestDispatcher[int](bus)
	ctx := context.Background()

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(v int64) {
			defer wg.Done()
			_ = dispatchWorker(ctx, d, makeWorkerEvent("agg-1", v), false)
		}(int64(i))
	}
	wg.Wait()

	_ = d.Close(ctx)

	calls := bus.snapshot()
	if len(calls) != n {
		t.Fatalf("expected %d calls, got %d", n, len(calls))
	}

	seen := make(map[int64]bool)
	for _, e := range calls {
		checkNotSeen(t, seen, e.Version)
		seen[e.Version] = true
	}
}

// A panic while handling the first event does not stop the second from being
// delivered.
func TestDispatch_WorkerSurvivesPanic(t *testing.T) {
	var callCount atomic.Int64

	bus := &callbackWorkerBus{fn: func(
		_ context.Context,
		e asynxmd.Event[int],
	) error {
		n := callCount.Add(1)
		if n == 1 {
			panic("boom")
		}
		return nil
	}}

	d := newWorkerTestDispatcher[int](bus)
	ctx := context.Background()

	_ = dispatchWorker(ctx, d, makeWorkerEvent("agg-1", 1), true)
	_ = dispatchWorker(ctx, d, makeWorkerEvent("agg-1", 2), true)

	_ = d.Close(ctx)

	if got := callCount.Load(); got != 2 {
		t.Errorf("expected 2 PublishSync calls (panic recovered), got %d", got)
	}
}

func TestEnqueue_AckRunsAfterHandlersFinish(t *testing.T) {
	var handled, acked atomic.Bool
	gate := make(chan struct{})
	bus := &callbackWorkerBus{fn: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		<-gate
		handled.Store(true)
		return nil
	}}
	d := newWorkerTestDispatcher[int](bus)
	defer d.Close(context.Background())

	done, err := enqueueWorkerEvent(context.Background(), d, makeWorkerEvent("a", 1), func(context.Context) error {
		if !handled.Load() {
			t.Error("ack ran before the handlers finished")
		}
		acked.Store(true)
		return nil
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if acked.Load() {
		t.Fatal("ack ran while the handler was still blocked")
	}
	close(gate)
	<-done
	if !acked.Load() {
		t.Fatal("ack did not run before done closed")
	}
}

func TestEnqueue_AckErrorIsReported(t *testing.T) {
	ackErr := errors.New("ack failed")
	var got atomic.Value
	d := newWorkerTestDispatcher(
		asynxmd.Bus[int](&recordingWorkerBus{}),
		withPublishErrorHandler[int](func(
			_ context.Context,
			_ asynxmd.Event[int],
			err error,
		) {
			got.Store(err)
		}),
	)
	defer d.Close(context.Background())

	done, _ := enqueueWorkerEvent(context.Background(), d, makeWorkerEvent("a", 1), func(context.Context) error { return ackErr })
	<-done

	if err, _ := got.Load().(error); !errors.Is(err, ackErr) {
		t.Fatalf("reported error = %v, want ack error", err)
	}
}

// A job enqueued while the idle worker is about to retire must still run: each
// enqueue sleeps the idle timeout first, to land right on the idle deadline.
func TestEnqueue_NeverStrandedByIdleWorkerRetirement(t *testing.T) {
	d := newWorkerTestDispatcher[int](&recordingWorkerBus{}, withWorkerIdleTimeout[int](time.Millisecond))
	defer d.Close(context.Background())

	for i := int64(1); i <= 300; i++ {
		time.Sleep(time.Millisecond)
		enqueueOnTheIdleDeadline(t, d, i)
	}
}

// A panic of the dispatcher itself is reported, and the worker survives and
// keeps delivering.
func TestHandle_DispatcherPanicIsReportedNotSwallowed(t *testing.T) {
	var got atomic.Value
	bus := &callbackWorkerBus{fn: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		panic("bus exploded")
	}}
	d := newWorkerTestDispatcher[int](bus, withPublishErrorHandler[int](func(
		_ context.Context,
		_ asynxmd.Event[int],
		err error,
	) {
		got.Store(err)
	}))
	defer d.Close(context.Background())

	done, _ := enqueueWorkerEvent(context.Background(), d, makeWorkerEvent("a", 1), nil)
	<-done
	if err, _ := got.Load().(error); err == nil {
		t.Fatal("a panic in the dispatcher was swallowed silently")
	}

	done, _ = enqueueWorkerEvent(context.Background(), d, makeWorkerEvent("a", 2), nil)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker died after a panic")
	}
}

// A failing live subscriber must not skip the durable delivery: the next
// event of the aggregate would otherwise overtake this one.
func TestEnqueue_DurableDeliveryStillAcksWhenBusRejectsEvent(t *testing.T) {
	var acked atomic.Bool
	var reported atomic.Int32
	d := newWorkerTestDispatcher(
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

	ticket, err := enqueueWorkerTicket(context.Background(), d, makeWorkerEvent("a", 1), func(context.Context) error {
		acked.Store(true)
		return nil
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := finalWorker(t, ticket, "final"); err != nil {
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
	d := newWorkerDurable(t, []asynxmd.DurableSub[int]{{Match: alwaysWorker, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		t.Error("durable handler ran for a job without an ack")
		return nil
	}}})
	done, _ := enqueueWorkerEvent(context.Background(), d, makeWorkerEvent("a", 1), nil)
	<-done
}

func TestBusErrorDoesNotSkipTheDurableDelivery(t *testing.T) {
	var rec deliveriesWorker
	d := newWorkerTestDispatcher[int](&errBus{err: errors.New("bus down")},
		withWorkerDurableSubs([]asynxmd.DurableSub[int]{{Match: alwaysWorker, Handler: func(
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
		last, _ = enqueueWorkerTicket(context.Background(), d, makeWorkerEvent("a", v), ackWorkerInto(&acked, makeWorkerEvent("a", v)))
	}
	if err := finalWorker(t, last, "final"); err != nil {
		t.Fatalf("final = %v", err)
	}
	got := rec.list()
	inOrder := len(got) == 3 && got[0] == "a/1" && got[2] == "a/3"
	if !inOrder {
		t.Fatalf("durable deliveries = %v, want a/1 a/2 a/3 in order", got)
	}
}

func TestEnqueueRedelivery_GoesToTheDurableSubscribersOnly(t *testing.T) {
	bus := &recordingWorkerBus{}
	var seen deliveriesWorker
	d := newWorkerTestDispatcher[int](bus, withWorkerDurableSubs([]asynxmd.DurableSub[int]{{Match: func(name string) bool { return name == "test.event" },
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

	tk, _ := enqueueWorkerRedelivery(ctx, d, makeWorkerEvent("a", 1), ack)
	finalWorker(t, tk, "event")
	marker := makeWorkerEvent("a", 2)
	marker.EventName = asynxmd.ForgetEventName
	tk, _ = enqueueWorkerRedelivery(ctx, d, marker, ack)
	finalWorker(t, tk, "marker")

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
	d := newWorkerTestDispatcher[int](&panicBus{}, withWorkerDurableSubs([]asynxmd.DurableSub[int]{{Match: alwaysWorker, Handler: func(
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
	tk, _ := enqueueWorkerTicket(context.Background(), d, makeWorkerEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	if err := finalWorker(t, tk, "delivery"); err != nil {
		t.Fatalf("final = %v", err)
	}
	if !acked.Load() || reported.Load() != 1 {
		t.Fatalf("acked=%v reported=%d, want the panic reported and the delivery completed", acked.Load(), reported.Load())
	}
}

func TestPublishErrorHandlerPanicDoesNotCrashTheWorker(t *testing.T) {
	d := newWorkerTestDispatcher[int](&errBus{err: errors.New("down")}, withPublishErrorHandler[int](func(
		context.Context,
		asynxmd.Event[int],
		error,
	) {
		panic("boom")
	}))
	t.Cleanup(func() { d.Close(context.Background()) })
	done, err := enqueueWorkerEvent(context.Background(), d, makeWorkerEvent("a", 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	recvWorker(t, done, "settled")
	done, _ = enqueueWorkerEvent(context.Background(), d, makeWorkerEvent("a", 2), nil)
	recvWorker(t, done, "the worker survives")
}

// A job popped just before a Forget supersedes its aggregate belongs to the old
// generation: it must not run its retry loop ahead of the marker.
func TestSupersede_AfterThePopStillSkipsTheSupersededJob(t *testing.T) {
	bus := &gateBus{entered: make(chan struct{}), release: make(chan struct{})}
	var oldRuns atomic.Int32
	d := newWorkerTestDispatcher[int](bus, withWorkerDurableSubs([]asynxmd.DurableSub[int]{{Match: alwaysWorker, Handler: func(
		_ context.Context,
		e asynxmd.Event[int],
	) error {
		if e.EventName != asynxmd.ForgetEventName {
			oldRuns.Add(1)
			return errors.New("down")
		}
		return nil
	}}}), withWorkerRetryBackoff[int](50*time.Millisecond, 50*time.Millisecond), withWorkerMaxAttempts[int](5))
	t.Cleanup(func() { d.Close(context.Background()) })

	ctx := context.Background()
	enqueueWorkerTicket(ctx, d, makeWorkerEvent("a", 1), func(context.Context) error { return nil })
	recvWorker(t, bus.entered, "v1 popped and publishing")

	d.Supersede("a")
	marker := makeWorkerEvent("a", 2)
	marker.EventName = asynxmd.ForgetEventName
	tk, _ := enqueueWorkerForget(ctx, d, marker, func(context.Context) error { return nil })
	close(bus.release)

	start := time.Now()
	if err := finalWorker(t, tk, "marker"); err != nil {
		t.Fatalf("marker = %v", err)
	}
	if time.Since(start) > 40*time.Millisecond || oldRuns.Load() != 0 {
		t.Fatalf("the superseded job ran %d attempts and held the marker back %v", oldRuns.Load(), time.Since(start))
	}
	if got := haltedWorker(d); len(got) != 0 {
		t.Fatalf("halted = %v", got)
	}
}

func TestAckFailure_TheAggregateIsParkedBeforeThePublishErrorHandlerIsTold(t *testing.T) {
	var d asynxmd.Dispatcher[int]
	parkedWhenTold := make(chan []string, 1)
	d = newWorkerDurable(t,
		[]asynxmd.DurableSub[int]{{Match: alwaysWorker, Handler: func(
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
			parkedWhenTold <- haltedWorker(d)
		}))

	enqueueWorkerTicket(context.Background(), d, makeWorkerEvent("a", 1), func(context.Context) error { return errors.New("store down") })
	if got := recvWorker(t, parkedWhenTold, "the ack failure to be reported"); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("parked aggregates when the handler was told = %v, want [a]", got)
	}
}

// If the ack of v1 fails, v2 must not be delivered and acknowledged ahead of it.
func TestAckFailure_ParksTheAggregateSoALaterEventDoesNotOvertake(t *testing.T) {
	var rec deliveriesWorker
	var reported atomic.Int32
	d := newWorkerDurable(t, []asynxmd.DurableSub[int]{{Match: alwaysWorker, Handler: func(
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
	t1, _ := enqueueWorkerTicket(ctx, d, makeWorkerEvent("a", 1), func(context.Context) error { return errors.New("store down") })
	t2, _ := enqueueWorkerTicket(ctx, d, makeWorkerEvent("a", 2), func(context.Context) error { return nil })
	if err := finalWorker(t, t1, "v1"); err == nil {
		t.Fatal("v1 ended as acknowledged")
	}
	if err := finalWorker(t, t2, "v2"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v2 = %v, want ErrDeliveryParked", err)
	}
	if got := rec.list(); !slices.Equal(got, []string{"a/1"}) {
		t.Fatalf("delivered %v, want only v1", got)
	}
	if got := haltedWorker(d); !slices.Equal(got, []string{"a"}) || reported.Load() != 1 {
		t.Fatalf("halted=%v reported=%d", got, reported.Load())
	}
}

func makeWorkerEvent(
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

func newWorkerTestDispatcher[T any](
	bus asynxmd.Bus[T],
	opts ...testWorkerOpt[T],
) asynxmd.Dispatcher[T] {
	c := testWorkerConfig[T]{cfg: asynxmd.DispatcherConfig[T]{
		Bus:          bus,
		RetryInitial: 100 * time.Millisecond,
		RetryMax:     30 * time.Second,
		MaxAttempts:  20,
	}}
	for _, opt := range opts {
		opt(&c)
	}
	return New(c.cfg, c.opts...)
}

func withWorkerIdleTimeout[T any](
	t time.Duration,
) testWorkerOpt[T] {
	return func(
		c *testWorkerConfig[T],
	) {
		c.opts = append(c.opts, WithIdleTimeout[T](t))
	}
}

func withPublishErrorHandler[T any](
	fn asynxmd.PublishErrorHandler[T],
) testWorkerOpt[T] {
	return func(
		c *testWorkerConfig[T],
	) {
		c.cfg.OnPublishError = fn
	}
}

func withWorkerDurableSubs[T any](
	subs []asynxmd.DurableSub[T],
) testWorkerOpt[T] {
	return func(
		c *testWorkerConfig[T],
	) {
		c.cfg.DurableSubs = subs
	}
}

func withWorkerRetryBackoff[T any](
	initial time.Duration,
	max time.Duration,
) testWorkerOpt[T] {
	return func(
		c *testWorkerConfig[T],
	) {
		c.cfg.RetryInitial, c.cfg.RetryMax = initial, max
	}
}

func withWorkerMaxAttempts[T any](
	n int,
) testWorkerOpt[T] {
	return func(
		c *testWorkerConfig[T],
	) {
		c.cfg.MaxAttempts = n
	}
}

func dispatchWorker[T any](
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

func enqueueWorkerEvent[T any](
	ctx context.Context,
	d asynxmd.Dispatcher[T],
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (<-chan struct{}, error) {
	t, err := enqueueWorkerTicket(ctx, d, event, ack)
	return t.Settled, err
}

func enqueueWorkerTicket[T any](
	ctx context.Context,
	d asynxmd.Dispatcher[T],
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (asynxmd.Ticket, error) {
	return d.Enqueue(ctx, asynxmd.DispatchRequest[T]{Event: event, Ack: ack, Kind: asynxmd.DispatchEvent})
}

func enqueueWorkerRedelivery[T any](
	ctx context.Context,
	d asynxmd.Dispatcher[T],
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (asynxmd.Ticket, error) {
	return d.Enqueue(ctx, asynxmd.DispatchRequest[T]{Event: event, Ack: ack, Kind: asynxmd.DispatchRedelivery})
}

func enqueueWorkerForget[T any](
	ctx context.Context,
	d asynxmd.Dispatcher[T],
	tombstone asynxmd.Event[T],
	ack func(context.Context) error,
) (asynxmd.Ticket, error) {
	return d.Enqueue(ctx, asynxmd.DispatchRequest[T]{Event: tombstone, Ack: ack, Kind: asynxmd.DispatchTombstone})
}

func haltedWorker[T any](
	d asynxmd.Dispatcher[T],
) []string {
	return slices.Sorted(maps.Keys(d.Parks()))
}

func alwaysWorker(string) bool { return true }

func newWorkerDurable(
	t *testing.T,
	subs []asynxmd.DurableSub[int],
	opts ...testWorkerOpt[int],
) asynxmd.Dispatcher[int] {
	t.Helper()
	all := append([]testWorkerOpt[int]{withWorkerDurableSubs(subs), withWorkerRetryBackoff[int](time.Millisecond, 5*time.Millisecond)}, opts...)
	d := newWorkerTestDispatcher[int](&recordingWorkerBus{}, all...)
	t.Cleanup(func() { d.Close(context.Background()) })
	return d
}

func recvWorker[V any](
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

func finalWorker(
	t *testing.T,
	tk asynxmd.Ticket,
	what string,
) error {
	t.Helper()
	recvWorker(t, tk.Final, what)
	return tk.Err()
}

func ackWorkerInto(
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
	if err := dispatchWorker(context.Background(), d, makeWorkerEvent("agg-1", i), false); err != nil {
		t.Fatalf("dispatch v%d: %v", i, err)
	}
}

func checkWorkerCallVersion(
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
		_ = dispatchWorker(ctx, d, makeWorkerEvent(agg, v), false)
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
	done, err := enqueueWorkerEvent(context.Background(), d, makeWorkerEvent("a", i), nil)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("event %d was stranded on a retired queue", i)
	}
}

type callbackWorkerBus struct {
	fn func(
		context.Context,
		asynxmd.Event[int],
	) error
}

func (b *callbackWorkerBus) PublishSync(
	ctx context.Context,
	e asynxmd.Event[int],
) error {
	return b.fn(ctx, e)
}

func (b *callbackWorkerBus) Publish(
	context.Context,
	asynxmd.Event[int],
) error {
	return nil
}

func (b *callbackWorkerBus) Subscribe(
	string,
	asynxmd.ProjectionHandler[int],
	...asynxmd.SubscriptionOpt[int],
) (string, error) {
	return "", nil
}

func (b *callbackWorkerBus) Unsubscribe(string) error { return nil }

func (b *callbackWorkerBus) Close(context.Context) error { return nil }

func (b *callbackWorkerBus) WaitForHandlers() {}

type deliveriesWorker struct {
	mu   sync.Mutex
	seen []string
}

func (r *deliveriesWorker) add(e asynxmd.Event[int]) {
	r.mu.Lock()
	r.seen = append(r.seen, e.AggregateID+"/"+string(rune('0'+e.Version)))
	r.mu.Unlock()
}

func (r *deliveriesWorker) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.seen...)
}

type errBus struct {
	err error
}

func (b *errBus) PublishSync(
	context.Context,
	asynxmd.Event[int],
) error {
	return b.err
}

func (b *errBus) Publish(
	context.Context,
	asynxmd.Event[int],
) error {
	return nil
}

func (b *errBus) Subscribe(
	string,
	asynxmd.ProjectionHandler[int],
	...asynxmd.SubscriptionOpt[int],
) (string, error) {
	return "", nil
}

func (b *errBus) Unsubscribe(string) error { return nil }

func (b *errBus) Close(context.Context) error { return nil }

func (b *errBus) WaitForHandlers() {}

type gateBus struct {
	recordingWorkerBus
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *gateBus) PublishSync(
	ctx context.Context,
	e asynxmd.Event[int],
) error {
	b.once.Do(func() {
		close(b.entered)
		<-b.release
	})
	return nil
}

type panicBus struct{ recordingWorkerBus }

func (*panicBus) PublishSync(
	context.Context,
	asynxmd.Event[int],
) error {
	panic("bus exploded")
}

type recordingWorkerBus struct {
	mu    sync.Mutex
	calls []asynxmd.Event[int]
	delay time.Duration
}

func (b *recordingWorkerBus) PublishSync(
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

func (b *recordingWorkerBus) Publish(
	context.Context,
	asynxmd.Event[int],
) error {
	return nil
}

func (b *recordingWorkerBus) Subscribe(
	string,
	asynxmd.ProjectionHandler[int],
	...asynxmd.SubscriptionOpt[int],
) (string, error) {
	return "", nil
}

func (b *recordingWorkerBus) Unsubscribe(string) error { return nil }

func (b *recordingWorkerBus) Close(context.Context) error { return nil }

func (b *recordingWorkerBus) WaitForHandlers() {}

func (b *recordingWorkerBus) snapshot() []asynxmd.Event[int] {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]asynxmd.Event[int], len(b.calls))
	copy(out, b.calls)
	return out
}

type testWorkerConfig[T any] struct {
	cfg  asynxmd.DispatcherConfig[T]
	opts []Opt[T]
}

type testWorkerOpt[T any] func(
	c *testWorkerConfig[T],
)
