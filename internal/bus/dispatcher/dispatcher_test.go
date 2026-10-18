package dispatcher

import (
	"context"
	"errors"
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

// WaitIdle returns once the job is handled; workers.Wait waits for the
// goroutine to exit after its idle timeout.
// Dispatching again should still work (new worker spins up).
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

// Second close must not panic (double close of channels).
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

// Bus blocks forever on PublishSync, signalling when it starts.
// block forever
// Wait until worker is actually inside PublishSync.
func TestClose_ContextTimeout(t *testing.T) {
	started := make(chan struct{})
	bus := &callbackBus{fn: func(_ context.Context, _ asynxmd.Event[int]) error {
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

// Enqueue must never block on a slow handler: callers enqueue while holding
// their per-aggregate command lock.
func TestEnqueue_NeverBlocksBehindASlowHandler(t *testing.T) {
	release := make(chan struct{})
	var mu sync.Mutex
	var seen []int64
	bus := &callbackBus{fn: func(_ context.Context, e asynxmd.Event[int]) error {
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

// the first job is inside its handler, two more are queued behind it
// Once Interrupted closes, Close has closed the registry and is waiting for
// the workers.
func TestClose_DrainsJobsQueuedBehindASlowHandler(t *testing.T) {
	release := make(chan struct{})
	running := make(chan struct{}, 3)
	var handled atomic.Int32
	bus := &callbackBus{fn: func(context.Context, asynxmd.Event[int]) error {
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
	if _, found := d.TicketFor("a", 1); found {
		t.Fatal("TicketFor kept an event whose delivery is over")
	}
}

func TestTicketFor_ForgetsEventsLeftBehindByAPark(t *testing.T) {
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		return nil
	}}})
	d.Park("a")

	ticket, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	<-ticket.Final
	if _, found := d.TicketFor("a", 1); found {
		t.Fatal("TicketFor kept an event that was never queued because its aggregate is parked")
	}
}

func TestUnrun_RemembersEventsLeftUndeliveredByAPark(t *testing.T) {
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		return nil
	}}})
	d.Park("a")

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

	d.Park("a")
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

	d.Park("a", 3, 5)

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
// "unrun": a handler took it.
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

	d.Park("a")
	<-queued.Final
	if _, found := d.TicketFor("a", 2); found {
		t.Fatal("TicketFor kept a queued event that parking took out of memory")
	}
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

// Still open: a non-durable event is delivered normally.
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

	d.Park("a")
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
