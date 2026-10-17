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

// recv waits for a value or fails the test; it is how these tests synchronise
// instead of sleeping.
func recv[V any](t *testing.T, ch <-chan V, what string) V {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

// failingFor fails every delivery of the named aggregate and version 1 only,
// recording the (aggregate, version) pairs it was asked to deliver.
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

func ackInto(acked *sync.Map, e asynxmd.Event[int]) func(context.Context) error {
	return func(context.Context) error { acked.Store(e.Version, true); return nil }
}

func TestShutdownDuringRetry_NeverDeliversALaterEventPastTheFailingOne(t *testing.T) {
	var rec deliveries
	attempted := make(chan struct{}, 64)
	d := New[int](&recordingBus{},
		WithDurableSubs([]DurableSub[int]{{Match: always, Handler: func(_ context.Context, e asynxmd.Event[int]) error {
			rec.add(e)
			if e.Version == 1 {
				attempted <- struct{}{}
				return errors.New("poison")
			}
			return nil
		}}}),
		WithRetryBackoff[int](time.Hour, time.Hour),
	)

	var acked sync.Map
	ctx := context.Background()
	t1, _ := d.EnqueueTicket(ctx, makeEvent("a", 1), ackInto(&acked, makeEvent("a", 1)))
	t2, _ := d.EnqueueTicket(ctx, makeEvent("a", 2), ackInto(&acked, makeEvent("a", 2)))
	recv(t, attempted, "first attempt of v1")

	if err := d.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for v, got := range map[int]error{1: recv(t, t1.Final, "v1 final"), 2: recv(t, t2.Final, "v2 final")} {
		if got == nil {
			t.Errorf("v%d ended as delivered", v)
		}
	}
	for _, v := range []int64{1, 2} {
		if _, ok := acked.Load(v); ok {
			t.Errorf("v%d was acknowledged although v1 never succeeded", v)
		}
	}
	for _, s := range rec.list() {
		if s == "a/2" {
			t.Fatalf("v2 was delivered while v1 was still pending: %v", rec.list())
		}
	}
}

func TestInterrupt_ReleasesWaitersWithoutClosingTheDispatcher(t *testing.T) {
	attempted := make(chan struct{}, 8)
	d := New[int](&recordingBus{},
		WithDurableSubs([]DurableSub[int]{{Match: always, Handler: func(context.Context, asynxmd.Event[int]) error {
			attempted <- struct{}{}
			return errors.New("poison")
		}}}),
		WithRetryBackoff[int](time.Hour, time.Hour),
	)
	defer d.Close(context.Background())

	tk, err := d.EnqueueTicket(context.Background(), makeEvent("a", 1), func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	recv(t, attempted, "first attempt")

	d.Interrupt()
	if got := recv(t, tk.Final, "final"); !errors.Is(got, asynxmd.ErrDispatcherClosed) {
		t.Fatalf("final = %v, want ErrDispatcherClosed", got)
	}
	// Still open: a non-durable event is delivered normally.
	done, err := d.Enqueue(context.Background(), makeEvent("b", 1), nil)
	if err != nil {
		t.Fatalf("Enqueue after Interrupt: %v", err)
	}
	recv(t, done, "live event after Interrupt")
}

func TestBusErrorDoesNotSkipTheDurableDelivery(t *testing.T) {
	var rec deliveries
	d := New[int](&errBus{err: errors.New("bus down")},
		WithDurableSubs([]DurableSub[int]{{Match: always, Handler: func(_ context.Context, e asynxmd.Event[int]) error {
			rec.add(e)
			return nil
		}}}),
	)
	defer d.Close(context.Background())

	var acked sync.Map
	var last Ticket
	for v := int64(1); v <= 3; v++ {
		last, _ = d.EnqueueTicket(context.Background(), makeEvent("a", v), ackInto(&acked, makeEvent("a", v)))
	}
	if err := recv(t, last.Final, "final"); err != nil {
		t.Fatalf("final = %v", err)
	}
	if got := rec.list(); len(got) != 3 || got[0] != "a/1" || got[2] != "a/3" {
		t.Fatalf("durable deliveries = %v, want a/1 a/2 a/3 in order", got)
	}
}

func TestExhaustedAttempts_ParkTheAggregateAndLeaveOthersAlone(t *testing.T) {
	var attempts atomic.Int32
	var rec deliveries
	var mu sync.Mutex
	var reports []error
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(_ context.Context, e asynxmd.Event[int]) error {
		rec.add(e)
		if e.AggregateID == "a" && e.Version == 1 {
			attempts.Add(1)
			return errors.New("poison")
		}
		return nil
	}}},
		WithMaxAttempts[int](4),
		WithDeliveryErrorHandler[int](func(_ context.Context, _ asynxmd.Event[int], err error, _ int) {
			mu.Lock()
			reports = append(reports, err)
			mu.Unlock()
		}),
	)

	var acked sync.Map
	ctx := context.Background()
	t1, _ := d.EnqueueTicket(ctx, makeEvent("a", 1), ackInto(&acked, makeEvent("a", 1)))
	t2, _ := d.EnqueueTicket(ctx, makeEvent("a", 2), ackInto(&acked, makeEvent("a", 2)))

	if err := recv(t, t1.Final, "v1 final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v1 final = %v, want ErrDeliveryParked", err)
	}
	if err := recv(t, t2.Final, "v2 final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v2 final = %v, want ErrDeliveryParked", err)
	}
	if n := attempts.Load(); n != 4 {
		t.Fatalf("attempts = %d, want exactly the cap (4)", n)
	}
	if got := d.Halted(); len(got) != 1 || got[0] != "a" {
		t.Fatalf("Halted = %v, want [a]", got)
	}
	for _, s := range rec.list() {
		if s == "a/2" {
			t.Fatal("v2 delivered although v1 was parked")
		}
	}

	// A later event of the parked aggregate is not queued either.
	t3, _ := d.EnqueueTicket(ctx, makeEvent("a", 3), ackInto(&acked, makeEvent("a", 3)))
	if err := recv(t, t3.Final, "v3 final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v3 final = %v, want ErrDeliveryParked", err)
	}

	// Another aggregate flows.
	tb, _ := d.EnqueueTicket(ctx, makeEvent("b", 1), ackInto(&acked, makeEvent("b", 1)))
	if err := recv(t, tb.Final, "b final"); err != nil {
		t.Fatalf("aggregate b held back: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	parkedReports := 0
	for _, e := range reports {
		if errors.Is(e, asynxmd.ErrDeliveryParked) {
			parkedReports++
		}
	}
	if parkedReports != 1 {
		t.Fatalf("parked reports = %d, want 1 (%v)", parkedReports, reports)
	}
}

func TestResume_LiftsAParkAndDeliversAgain(t *testing.T) {
	var healthy atomic.Bool
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(context.Context, asynxmd.Event[int]) error {
		if healthy.Load() {
			return nil
		}
		return errors.New("down")
	}}}, WithMaxAttempts[int](2))

	var acked sync.Map
	tk, _ := d.EnqueueTicket(context.Background(), makeEvent("a", 1), ackInto(&acked, makeEvent("a", 1)))
	recv(t, tk.Final, "parked")

	healthy.Store(true)
	d.Resume("a")
	tk, err := d.EnqueueRecovered(context.Background(), makeEvent("a", 1), ackInto(&acked, makeEvent("a", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if err := recv(t, tk.Final, "redelivery"); err != nil {
		t.Fatalf("redelivery = %v", err)
	}
	if _, ok := acked.Load(int64(1)); !ok {
		t.Fatal("redelivered event was not acknowledged")
	}
	if len(d.Halted()) != 0 {
		t.Fatalf("still halted: %v", d.Halted())
	}
}

func TestDeadLetter_AcceptingTheEventSkipsItAndLetsLaterEventsFlow(t *testing.T) {
	var rec deliveries
	var dead atomic.Value
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(_ context.Context, e asynxmd.Event[int]) error {
		rec.add(e)
		if e.Version == 1 {
			return errors.New("poison")
		}
		return nil
	}}},
		WithMaxAttempts[int](3),
		WithDeadLetterHandler[int](func(_ context.Context, e asynxmd.Event[int], err error) error {
			dead.Store(e.Version)
			return nil
		}),
	)

	var acked sync.Map
	ctx := context.Background()
	t1, _ := d.EnqueueTicket(ctx, makeEvent("a", 1), ackInto(&acked, makeEvent("a", 1)))
	t2, _ := d.EnqueueTicket(ctx, makeEvent("a", 2), ackInto(&acked, makeEvent("a", 2)))

	if err := recv(t, t1.Final, "v1"); err != nil {
		t.Fatalf("dead-lettered event final = %v, want nil", err)
	}
	if err := recv(t, t2.Final, "v2"); err != nil {
		t.Fatalf("v2 final = %v", err)
	}
	if dead.Load() != int64(1) {
		t.Fatalf("dead letter saw %v, want v1", dead.Load())
	}
	if _, ok := acked.Load(int64(1)); !ok {
		t.Fatal("dead-lettered event stayed pending")
	}
	if len(d.Halted()) != 0 {
		t.Fatalf("aggregate parked despite the dead-letter handler: %v", d.Halted())
	}
}

func TestDeadLetter_RefusalParksTheAggregate(t *testing.T) {
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(context.Context, asynxmd.Event[int]) error {
		return errors.New("poison")
	}}},
		WithMaxAttempts[int](2),
		WithDeadLetterHandler[int](func(context.Context, asynxmd.Event[int], error) error {
			panic("dead-letter store down")
		}),
	)
	tk, _ := d.EnqueueTicket(context.Background(), makeEvent("a", 1), func(context.Context) error { return nil })
	if err := recv(t, tk.Final, "final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
}

func TestQueueBound_ParksTheAggregateInsteadOfGrowingWithoutLimit(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	var rec deliveries
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(_ context.Context, e asynxmd.Event[int]) error {
		rec.add(e)
		if e.Version == 1 {
			select {
			case started <- struct{}{}:
			default:
			}
			<-gate
			return errors.New("poison")
		}
		return nil
	}}}, WithMaxQueued[int](3), WithMaxAttempts[int](1000))

	var acked sync.Map
	ctx := context.Background()
	first, _ := d.EnqueueTicket(ctx, makeEvent("a", 1), ackInto(&acked, makeEvent("a", 1)))
	recv(t, started, "v1 running")

	var tickets []Ticket
	for v := int64(2); v <= 6; v++ {
		tk, _ := d.EnqueueTicket(ctx, makeEvent("a", v), ackInto(&acked, makeEvent("a", v)))
		tickets = append(tickets, tk)
	}
	// v2..v4 fit behind the running v1; v5 overflows and parks everything.
	for i, tk := range tickets {
		if err := recv(t, tk.Final, "queued ticket"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
			t.Fatalf("ticket %d final = %v, want ErrDeliveryParked", i+2, err)
		}
	}
	close(gate)
	if err := recv(t, first.Final, "v1 final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v1 final = %v, want ErrDeliveryParked (interrupted by the park)", err)
	}
	for _, s := range rec.list() {
		if s != "a/1" {
			t.Fatalf("delivered %q although the aggregate was parked", s)
		}
	}
}

func TestSupersede_LetsTheTombstoneThroughAFailingDelivery(t *testing.T) {
	var seen deliveries
	attempted := make(chan struct{}, 8)
	d := newDurable(t, []DurableSub[int]{{Match: func(name string) bool { return name == "test.event" },
		Handler: func(_ context.Context, e asynxmd.Event[int]) error {
			seen.add(e)
			if e.EventName == "test.event" {
				attempted <- struct{}{}
				return errors.New("poison")
			}
			return nil
		}}}, WithRetryBackoff[int](time.Hour, time.Hour))

	var acked sync.Map
	ctx := context.Background()
	d.EnqueueTicket(ctx, makeEvent("a", 1), ackInto(&acked, makeEvent("a", 1)))
	d.EnqueueTicket(ctx, makeEvent("a", 2), ackInto(&acked, makeEvent("a", 2)))
	recv(t, attempted, "poison attempt")

	d.Supersede("a")
	tomb := makeEvent("a", 3)
	tomb.EventName = asynxmd.ForgetEventName
	tk, err := d.EnqueueForget(ctx, tomb)
	if err != nil {
		t.Fatal(err)
	}
	if err := recv(t, tk.Final, "tombstone"); err != nil {
		t.Fatalf("tombstone final = %v", err)
	}
	if len(d.Halted()) != 0 {
		t.Fatalf("Supersede left the aggregate parked: %v", d.Halted())
	}
	// The tombstone reached a subscriber whose pattern does not match it.
	found := false
	for _, s := range seen.list() {
		if s == "a/3" {
			found = true
		}
	}
	if !found {
		t.Fatalf("durable subscriber never saw the tombstone: %v", seen.list())
	}
}

func TestTombstone_FinalReportsADurableSubscriberThatKeepsFailing(t *testing.T) {
	d := newDurable(t, []DurableSub[int]{{Match: func(string) bool { return false },
		Handler: func(context.Context, asynxmd.Event[int]) error { return errors.New("cannot forget") }}},
		WithMaxAttempts[int](2))
	tomb := makeEvent("a", 2)
	tomb.EventName = asynxmd.ForgetEventName
	tk, _ := d.EnqueueForget(context.Background(), tomb)
	if err := recv(t, tk.Final, "final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
	if len(d.Halted()) != 0 {
		t.Fatal("a failed tombstone must not park the aggregate (Forget fails instead)")
	}
}

func TestPark_OfAnAggregateWithoutAQueueStillBlocksItsDurableEvents(t *testing.T) {
	d := newDurable(t, nil)
	d.Park("ghost")
	d.Park("ghost") // idempotent
	d.Resume("never-parked")

	var acked sync.Map
	tk, _ := d.EnqueueTicket(context.Background(), makeEvent("ghost", 1), ackInto(&acked, makeEvent("ghost", 1)))
	if err := recv(t, tk.Final, "final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
	if _, ok := acked.Load(int64(1)); ok {
		t.Fatal("event of a parked aggregate was acknowledged")
	}

	d.Resume("ghost")
	tk, _ = d.EnqueueTicket(context.Background(), makeEvent("ghost", 1), ackInto(&acked, makeEvent("ghost", 1)))
	if err := recv(t, tk.Final, "final after resume"); err != nil {
		t.Fatalf("final after resume = %v", err)
	}
}

func TestPark_KeepsLiveOnlyJobsQueuedAndDropsDurableOnes(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(_ context.Context, e asynxmd.Event[int]) error {
		if e.Version == 1 {
			started <- struct{}{}
			<-gate
		}
		return nil
	}}})
	ctx := context.Background()
	d.EnqueueTicket(ctx, makeEvent("a", 1), func(context.Context) error { return nil })
	recv(t, started, "v1 running")
	durable, _ := d.EnqueueTicket(ctx, makeEvent("a", 2), func(context.Context) error { return nil })
	live, _ := d.EnqueueTicket(ctx, makeEvent("a", 3), nil)

	d.Park("a")
	if err := recv(t, durable.Final, "durable ticket"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("durable final = %v, want ErrDeliveryParked", err)
	}
	close(gate)
	if err := recv(t, live.Final, "live ticket"); err != nil {
		t.Fatalf("live-only job final = %v, want delivered", err)
	}
}

func TestEnqueueRecovered_IsExemptFromTheQueueBound(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(_ context.Context, e asynxmd.Event[int]) error {
		if e.Version == 1 {
			started <- struct{}{}
			<-gate
		}
		return nil
	}}}, WithMaxQueued[int](2))
	ctx := context.Background()
	ack := func(context.Context) error { return nil }
	first, _ := d.EnqueueRecovered(ctx, makeEvent("a", 1), ack)
	recv(t, started, "v1 running")
	var last Ticket
	for v := int64(2); v <= 6; v++ {
		last, _ = d.EnqueueRecovered(ctx, makeEvent("a", v), ack)
	}
	close(gate)
	if err := recv(t, last.Final, "last"); err != nil {
		t.Fatalf("recovered backlog was parked: %v", err)
	}
	recv(t, first.Final, "first")
	if len(d.Halted()) != 0 {
		t.Fatalf("halted = %v", d.Halted())
	}
}

func TestOptions_NonPositiveBoundsAreIgnored(t *testing.T) {
	d := New[int](nil, WithMaxAttempts[int](0), WithMaxQueued[int](-1))
	if d.maxAttempts != defaultMaxAttempts || d.maxQueued != defaultMaxQueued {
		t.Fatalf("maxAttempts=%d maxQueued=%d, want the defaults", d.maxAttempts, d.maxQueued)
	}
}
