package workers

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
)

func TestPool_PublishesLiveThenDeliversThenAcks(t *testing.T) {
	f := newFixture(t, time.Second)
	ticket := f.enqueue(t, event(1, func(context.Context) error {
		f.journal.add("ack")
		return nil
	}))

	if err := final(t, ticket); err != nil {
		t.Fatalf("final = %v", err)
	}
	if got := f.journal.list(); !slices.Equal(got, []string{"live", "deliver", "ack"}) {
		t.Fatalf("steps = %v, want live, deliver, ack", got)
	}
}

func TestPool_NeverAcksADeliveryThatDidNotSucceed(t *testing.T) {
	f := newFixture(t, time.Second)
	down := errors.New("down")
	f.deliverer.deliver = func(jobs.Job[int]) error { return down }
	var acks atomic.Int32
	ticket := f.enqueue(t, event(1, func(context.Context) error {
		acks.Add(1)
		return nil
	}))

	if err := final(t, ticket); !errors.Is(err, down) {
		t.Fatalf("final = %v, want the delivery error", err)
	}
	if acks.Load() != 0 {
		t.Fatal("a delivery that failed was acknowledged")
	}
}

func TestPool_AnAckFailureParksBeforeItIsReported(t *testing.T) {
	f := newFixture(t, time.Second)
	storeDown := errors.New("store down")
	ticket := f.enqueue(t, event(1, func(context.Context) error { return storeDown }))

	if err := final(t, ticket); !errors.Is(err, storeDown) {
		t.Fatalf("final = %v, want the ack error", err)
	}
	if got := f.journal.list(); !slices.Equal(got, []string{"live", "deliver", "park", "report"}) {
		t.Fatalf("steps = %v, want the park before the report", got)
	}
	if _, parked := f.registry.Parks()["a"]; !parked {
		t.Fatal("the aggregate was not parked after its ack failed")
	}
}

func TestPool_ALiveJobRunsNoDurableDelivery(t *testing.T) {
	f := newFixture(t, time.Second)
	ticket := f.enqueue(t, event(1, nil))

	if err := final(t, ticket); err != nil {
		t.Fatalf("final = %v", err)
	}
	if got := f.journal.list(); !slices.Equal(got, []string{"live"}) {
		t.Fatalf("steps = %v, want only the live publish", got)
	}
}

func TestPool_ATombstoneWithoutAnAckIsDeliveredAndNeverAcked(t *testing.T) {
	f := newFixture(t, time.Second)
	request := event(1, nil)
	request.Kind = asynxmd.DispatchTombstone
	ticket := f.enqueue(t, request)

	if err := final(t, ticket); err != nil {
		t.Fatalf("final = %v", err)
	}
	if got := f.journal.list(); !slices.Equal(got, []string{"live", "deliver"}) {
		t.Fatalf("steps = %v, want the live publish and the durable delivery", got)
	}
}

func TestPool_ARedeliverySkipsTheLiveSubscribers(t *testing.T) {
	f := newFixture(t, time.Second)
	request := event(1, func(context.Context) error { return nil })
	request.Kind = asynxmd.DispatchRedelivery
	ticket := f.enqueue(t, request)

	if err := final(t, ticket); err != nil {
		t.Fatalf("final = %v", err)
	}
	if got := f.journal.list(); !slices.Equal(got, []string{"deliver"}) {
		t.Fatalf("steps = %v, want only the durable delivery", got)
	}
}

func TestPool_ABusPanicIsReportedAndTheDeliveryGoesOn(t *testing.T) {
	f := newFixture(t, time.Second)
	f.bus.publish = func(asynxmd.Event[int]) error { panic("bus exploded") }
	ticket := f.enqueue(t, event(1, func(context.Context) error { return nil }))

	if err := final(t, ticket); err != nil {
		t.Fatalf("final = %v", err)
	}
	published := f.reports.published()
	if len(published) != 1 || !strings.Contains(published[0].Error(), "bus panic") {
		t.Fatalf("published errors = %v, want the bus panic", published)
	}
	if got := f.journal.list(); !slices.Contains(got, "deliver") {
		t.Fatalf("steps = %v: the bus panic skipped the durable delivery", got)
	}
}

func TestPool_ADispatcherPanicIsReportedAndEndsTheJob(t *testing.T) {
	f := newFixture(t, time.Second)
	f.deliverer.deliver = func(jobs.Job[int]) error { panic("dispatcher bug") }
	ticket := f.enqueue(t, event(1, func(context.Context) error { return nil }))

	err := final(t, ticket)

	if err == nil || !strings.Contains(err.Error(), "dispatcher panic") {
		t.Fatalf("final = %v, want the dispatcher panic", err)
	}
	if published := f.reports.published(); len(published) != 1 {
		t.Fatalf("published errors = %v, want the panic reported once", published)
	}
	if next := f.enqueue(t, event(2, nil)); final(t, next) != nil {
		t.Fatal("the worker did not survive the panic")
	}
}

func TestPool_AParkedAckedJobRunsNothing(t *testing.T) {
	f := newFixture(t, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	job := jobs.New(ctx, event(1, func(context.Context) error { return nil }), cancel)
	halt := make(chan struct{})
	close(halt)

	f.pool.(*aggregatePool[int]).handle("a", halt, job)

	if err := final(t, job.Ticket()); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
	if err := job.Ticket().Unrun(); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("unrun = %v, want ErrDeliveryParked", err)
	}
	if got := f.journal.list(); len(got) != 0 {
		t.Fatalf("steps = %v: a job popped under a closed halt ran", got)
	}
}

func TestPool_HandlesAnAggregatesJobsInQueueOrder(t *testing.T) {
	f := newFixture(t, time.Second)
	var order []int64
	f.bus.publish = func(e asynxmd.Event[int]) error {
		order = append(order, e.Version)
		return nil
	}
	var last asynxmd.Ticket
	for v := int64(1); v <= 20; v++ {
		last = f.enqueue(t, event(v, nil))
	}

	if err := final(t, last); err != nil {
		t.Fatalf("final = %v", err)
	}
	for i, v := range order {
		assertVersion(t, i, v)
	}
}

func TestPool_AnIdleWorkerRetires(t *testing.T) {
	f := newFixture(t, time.Millisecond)
	if err := final(t, f.enqueue(t, event(1, nil))); err != nil {
		t.Fatal(err)
	}

	f.pool.Wait()

	if f.registry.Wake("a") != nil {
		t.Fatal("the record of a retired, unparked aggregate survived")
	}
	if err := final(t, f.enqueue(t, event(2, nil))); err != nil {
		t.Fatalf("a job after the retirement = %v", err)
	}
}

func TestPool_StopDrainsWhatIsQueued(t *testing.T) {
	f := newFixture(t, time.Hour)
	gate := make(chan struct{})
	f.bus.publish = func(asynxmd.Event[int]) error {
		<-gate
		return nil
	}
	tickets := []asynxmd.Ticket{f.enqueue(t, event(1, nil)), f.enqueue(t, event(2, nil))}
	f.registry.Close()
	f.pool.Stop()
	close(gate)

	f.pool.Wait()

	for _, ticket := range tickets {
		assertCompleted(t, ticket)
	}
}

func assertVersion(
	t *testing.T,
	index int,
	version int64,
) {
	t.Helper()
	if version != int64(index+1) {
		t.Fatalf("job %d handled as version %d: out of order", index, version)
	}
}

func assertCompleted(
	t *testing.T,
	ticket asynxmd.Ticket,
) {
	t.Helper()
	select {
	case <-ticket.Final:
	default:
		t.Fatal("Stop let a worker exit with a job still queued")
	}
}
