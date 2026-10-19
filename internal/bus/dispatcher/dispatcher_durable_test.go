package dispatcher

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

// The tombstone reached a subscriber whose pattern does not match it.
func TestSupersede_LetsTheTombstoneThroughAFailingDelivery(t *testing.T) {
	var seen deliveries
	attempted := make(chan struct{}, 8)
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: func(name string) bool { return name == "test.event" },
		Handler: func(_ context.Context, e asynxmd.Event[int]) error {
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

// Parking an aggregate twice is idempotent.
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
