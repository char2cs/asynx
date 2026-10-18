package processor_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/bus/dispatcher"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)


func seedPending(t *testing.T, s store.Memory, es *eventstore.EventStore[order], ids ...string) {
	t.Helper()
	ctx := context.Background()
	for _, id := range ids {
		if _, err := es.Write(ctx, mocks.CreateOrderCmd{ID: id, Total: 1}); err != nil {
			t.Fatal(err)
		}
		s.Atomically(ctx, func(tx asynxmd.Tx) error { return tx.EnqueueDelivery(ctx, id, 1) })
	}
}

func TestProcessor_Recover_RedeliversEveryPendingEventAndAcksIt(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	seedPending(t, s, es, "a", "b", "c")

	b := bus.NewChannelBus[order]()
	var live atomic.Int32
	b.Subscribe("OrderCreated", func(context.Context, asynxmd.Event[order]) { live.Add(1) })
	var got atomic.Int32
	p := processor.New(es, b, processor.WithDurableSubscribers([]dispatcher.DurableSub[order]{{
		Match:   always,
		Handler: func(context.Context, asynxmd.Event[order]) error { got.Add(1); return nil },
	}}))
	t.Cleanup(func() { p.Shutdown(context.Background()) })

	// batch <= 0 falls back to the default; batch 2 forces multiple rounds.
	if err := p.Recover(context.Background(), 2); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	p.WaitPublish()
	if got.Load() != 3 {
		t.Errorf("redelivered %d events, want 3", got.Load())
	}
	if live.Load() != 0 {
		t.Errorf("live subscribers saw %d recovered events, want none", live.Load())
	}
	if pending, _ := s.PendingDeliveries(context.Background(), 10); len(pending) != 0 {
		t.Errorf("pending = %v, want none", pending)
	}
	if err := p.Recover(context.Background(), 0); err != nil {
		t.Errorf("Recover on an empty log: %v", err)
	}
}

func TestProcessor_Recover_WithoutBusIsNoOp(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	p := processor.New[order](es, nil)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	if err := p.Recover(context.Background(), 10); err != nil {
		t.Fatalf("Recover: %v", err)
	}
}

func TestProcessor_Recover_ReportsReadAndReplayFailures(t *testing.T) {
	ctx := context.Background()

	// Unreadable delivery log.
	es := eventstore.New[order](&mocks.ErrStore{Err: errors.New("log down")}, store.NewSnapshots(), nil, 1, nil)
	p := processor.New(es, bus.NewChannelBus[order]())
	t.Cleanup(func() { p.Shutdown(ctx) })
	if err := p.Recover(ctx, 10); err == nil {
		t.Error("Recover succeeded with an unreadable delivery log")
	}

}

// One corrupt event must not stop the process from starting or the other
// aggregates from being recovered: its aggregate is parked and reported.
func TestProcessor_Recover_ParksAnAggregateWhoseEventCannotBeRebuilt(t *testing.T) {
	ctx := context.Background()
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	seedPending(t, s, es, "good")
	s.Append(ctx, "events:bad", 1, []byte("!!!"))
	s.Atomically(ctx, func(tx asynxmd.Tx) error {
		if err := tx.EnqueueDelivery(ctx, "bad", 1); err != nil {
			return err
		}
		return tx.EnqueueDelivery(ctx, "bad", 2) // behind the failed one: stays pending
	})

	var got atomic.Int32
	reported := make(chan error, 4)
	p := processor.New(es, bus.NewChannelBus[order](),
		processor.WithPublishErrorHandler(func(_ context.Context, e asynxmd.Event[order], err error) {
			if e.AggregateID == "bad" {
				reported <- err
			}
		}),
		processor.WithDurableSubscribers([]dispatcher.DurableSub[order]{{
			Match:   always,
			Handler: func(context.Context, asynxmd.Event[order]) error { got.Add(1); return nil },
		}}))
	t.Cleanup(func() { p.Shutdown(ctx) })

	if err := p.Recover(ctx, 10); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	p.WaitPublish()
	if got.Load() != 1 {
		t.Errorf("delivered %d events, want only the healthy aggregate's", got.Load())
	}
	select {
	case <-reported:
	case <-time.After(5 * time.Second):
		t.Fatal("the unreadable event was not reported")
	}
	if paused := p.Paused(); len(paused) != 1 || paused[0] != "bad" {
		t.Fatalf("Paused = %v, want [bad]", paused)
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 2 {
		t.Fatalf("pending = %v, want both of the bad aggregate's entries kept", pending)
	}

	// Resume retries it, fails again, and says so.
	if err := p.Resume(ctx, 10); err == nil {
		t.Fatal("Resume hid the unreadable event")
	}
	if paused := p.Paused(); len(paused) != 1 {
		t.Fatalf("Paused = %v after a failed Resume, want it still parked", paused)
	}
}

func TestProcessor_Recover_ReturnsWithoutWaitingForHandlers(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	seedPending(t, s, es, "a")

	b := bus.NewChannelBus[order]()
	release := make(chan struct{})
	b.Subscribe("OrderCreated", func(context.Context, asynxmd.Event[order]) { <-release })
	p := processor.New(es, b)
	t.Cleanup(func() { close(release); p.Shutdown(context.Background()) })

	returned := make(chan error, 1)
	go func() { returned <- p.Recover(context.Background(), 10) }()
	select {
	case err := <-returned:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Recover waited for a blocked handler")
	}
}

type ackErrStore struct{ store.Memory }

func (ackErrStore) AckDelivery(context.Context, string, int64) error { return errors.New("ack failed") }

func TestProcessor_Recover_ReportsAnAckFailureForAVanishedEvent(t *testing.T) {
	s := ackErrStore{store.New()}
	ctx := context.Background()
	s.Atomically(ctx, func(tx asynxmd.Tx) error { return tx.EnqueueDelivery(ctx, "ghost", 1) })
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	reported := make(chan error, 1)
	p := processor.New(es, bus.NewChannelBus[order](),
		processor.WithPublishErrorHandler(func(_ context.Context, _ asynxmd.Event[order], err error) { reported <- err }))
	t.Cleanup(func() { p.Shutdown(ctx) })

	if err := p.Recover(ctx, 10); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	select {
	case <-reported:
	case <-time.After(5 * time.Second):
		t.Fatal("Recover hid an ack failure")
	}
}

func updateCmd(id string, total float64) mocks.UpdateOrderCmd {
	return mocks.UpdateOrderCmd{ID: id, NewState: order{ID: id, Total: total}}
}

// parkingOpts makes a delivery give up after two attempts unless healthy is set.
func parkingOpts(healthy *atomic.Bool, extra ...processor.ProcessorOpt[order]) []processor.ProcessorOpt[order] {
	return append([]processor.ProcessorOpt[order]{
		processor.WithDurableDelivery[order](func(name string) bool { return name == "OrderUpdated" }),
		processor.WithDurableSubscribers([]dispatcher.DurableSub[order]{{
			Match: always,
			Handler: func(context.Context, asynxmd.Event[order]) error {
				if healthy.Load() {
					return nil
				}
				return errors.New("down")
			},
		}}),
		processor.WithDeliveryRetry[order](time.Millisecond, 2*time.Millisecond),
		processor.WithDeliveryMaxAttempts[order](2),
	}, extra...)
}

func TestProcessor_Resume_RedeliversParkedAggregatesInOrder(t *testing.T) {
	var healthy atomic.Bool
	parked := make(chan struct{}, 4)
	f := newFixture(t, store.New(), parkingOpts(&healthy,
		processor.WithDeliveryErrorHandler(func(_ context.Context, _ asynxmd.Event[order], err error, _ int) {
			if errors.Is(err, asynxmd.ErrDeliveryParked) {
				parked <- struct{}{}
			}
		}))...)
	ctx := context.Background()

	for i := range 2 {
		if _, err := f.p.Send(ctx, updateCmd("o1", float64(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	<-parked
	if pending, _ := f.s.PendingDeliveries(ctx, 10); len(pending) != 2 {
		t.Fatalf("pending = %v, want both events", pending)
	}

	healthy.Store(true)
	if err := f.p.Resume(ctx, 1); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	f.p.WaitPublish()
	if pending, _ := f.s.PendingDeliveries(ctx, 10); len(pending) != 0 {
		t.Fatalf("pending after Resume = %v", pending)
	}
	// Nothing parked any more: a second Resume is a no-op.
	if err := f.p.Resume(ctx, 0); err != nil {
		t.Fatalf("second Resume: %v", err)
	}
}

func TestProcessor_Resume_WithoutDispatcherIsNoOp(t *testing.T) {
	es := eventstore.New[order](store.New(), store.NewSnapshots(), nil, 1, nil)
	p := processor.New[order](es, nil)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	if err := p.Resume(context.Background(), 10); err != nil {
		t.Fatalf("Resume: %v", err)
	}
}

type flakyPendingStore struct {
	store.Memory
	fail atomic.Bool
}

func (s *flakyPendingStore) PendingDeliveries(ctx context.Context, limit int) ([]asynxmd.Delivery, error) {
	if s.fail.Load() {
		return nil, errors.New("log unreadable")
	}
	return s.Memory.PendingDeliveries(ctx, limit)
}

func TestProcessor_Resume_ReportsAFailureReadingTheLogAndStaysParked(t *testing.T) {
	var healthy atomic.Bool
	parked := make(chan struct{}, 4)
	s := &flakyPendingStore{Memory: store.New()}
	f := newFixture(t, s, parkingOpts(&healthy,
		processor.WithDeliveryErrorHandler(func(_ context.Context, _ asynxmd.Event[order], err error, _ int) {
			if errors.Is(err, asynxmd.ErrDeliveryParked) {
				parked <- struct{}{}
			}
		}))...)
	ctx := context.Background()
	if _, err := f.p.Send(ctx, updateCmd("o1", 1)); err != nil {
		t.Fatal(err)
	}
	<-parked

	s.fail.Store(true)
	if err := f.p.Resume(ctx, 10); err == nil {
		t.Fatal("Resume succeeded although the delivery log is unreadable")
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	s.fail.Store(false)
	if err := f.p.Resume(cancelled, 10); err == nil {
		t.Fatal("Resume with a cancelled context should fail taking the aggregate lock")
	}
}

func TestProcessor_Paused_ListsParkedAggregatesAndSendWaitSaysSo(t *testing.T) {
	var healthy atomic.Bool
	parked := make(chan struct{}, 4)
	f := newFixture(t, store.New(), parkingOpts(&healthy,
		processor.WithDeliveryErrorHandler(func(_ context.Context, _ asynxmd.Event[order], err error, _ int) {
			if errors.Is(err, asynxmd.ErrDeliveryParked) {
				parked <- struct{}{}
			}
		}))...)
	ctx := context.Background()
	if got := f.p.Paused(); len(got) != 0 {
		t.Fatalf("Paused = %v before anything failed", got)
	}

	if _, err := f.p.Send(ctx, updateCmd("o1", 1)); err != nil {
		t.Fatal(err)
	}
	<-parked
	if got := f.p.Paused(); len(got) != 1 || got[0] != "o1" {
		t.Fatalf("Paused = %v, want [o1]", got)
	}
	evt, err := f.p.SendWait(ctx, updateCmd("o1", 2))
	if !errors.Is(err, asynxmd.ErrDeliveryParked) || evt.Version != 2 {
		t.Fatalf("SendWait = v%d, %v; want the committed v2 with ErrDeliveryParked", evt.Version, err)
	}
}

func TestProcessor_Paused_WithoutDispatcherIsEmpty(t *testing.T) {
	es := eventstore.New[order](store.New(), store.NewSnapshots(), nil, 1, nil)
	p := processor.New[order](es, nil)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	if got := p.Paused(); got != nil {
		t.Fatalf("Paused = %v", got)
	}
}

func TestProcessor_Recover_AfterShutdownFails(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	seedPending(t, s, es, "a")
	p := processor.New(es, bus.NewChannelBus[order]())
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.Recover(context.Background(), 10); !errors.Is(err, asynxmd.ErrDispatcherClosed) {
		t.Fatalf("Recover on a closed dispatcher = %v, want ErrDispatcherClosed", err)
	}
}

func TestProcessor_Resume_StopsWhenItsContextEnds(t *testing.T) {
	var healthy atomic.Bool
	parked := make(chan struct{}, 4)
	f := newFixture(t, store.New(), parkingOpts(&healthy,
		processor.WithDeliveryErrorHandler(func(_ context.Context, _ asynxmd.Event[order], err error, _ int) {
			if errors.Is(err, asynxmd.ErrDeliveryParked) {
				parked <- struct{}{}
			}
		}))...)
	for _, id := range []string{"o1", "o2"} {
		if _, err := f.p.Send(context.Background(), updateCmd(id, 1)); err != nil {
			t.Fatal(err)
		}
		<-parked
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	err := f.p.Resume(cancelled, 10)
	if err == nil {
		t.Fatal("Resume ignored its cancelled context")
	}
	if strings.Contains(err.Error(), "\n") {
		t.Fatalf("Resume kept going after its context ended, reporting every aggregate: %v", err)
	}
	if got := f.p.Paused(); len(got) != 2 {
		t.Fatalf("Paused = %v, want both still parked", got)
	}
}

// Two Resumes must not both redeliver: the second would queue the same entries
// behind the first's and invert their order.
func TestProcessor_Resume_ConcurrentCallsRedeliverOnce(t *testing.T) {
	var healthy atomic.Bool
	var mu sync.Mutex
	var delivered []int64
	parked := make(chan struct{}, 4)
	release := make(chan struct{})
	f := newFixture(t, store.New(),
		processor.WithDurableDelivery[order](func(name string) bool { return name == "OrderUpdated" }),
		processor.WithDurableSubscribers([]dispatcher.DurableSub[order]{{
			Match: always,
			Handler: func(_ context.Context, e asynxmd.Event[order]) error {
				if !healthy.Load() {
					return errors.New("down")
				}
				<-release
				mu.Lock()
				delivered = append(delivered, e.Version)
				mu.Unlock()
				return nil
			},
		}}),
		processor.WithDeliveryRetry[order](time.Millisecond, 2*time.Millisecond),
		processor.WithDeliveryMaxAttempts[order](2),
		processor.WithDeliveryErrorHandler(func(_ context.Context, _ asynxmd.Event[order], err error, _ int) {
			if errors.Is(err, asynxmd.ErrDeliveryParked) {
				parked <- struct{}{}
			}
		}))
	ctx := context.Background()
	for i := range 3 {
		if _, err := f.p.Send(ctx, updateCmd("o1", float64(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	<-parked
	healthy.Store(true)

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() { defer wg.Done(); f.p.Resume(ctx, 10) }()
	}
	wg.Wait()
	close(release)
	f.p.WaitPublish()
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(delivered, []int64{1, 2, 3}) {
		t.Fatalf("delivered %v, want [1 2 3] exactly once", delivered)
	}
}

// rowCountingStore counts the event rows its reads return.
type rowCountingStore struct {
	store.Memory
	rows atomic.Int64
}

func (s *rowCountingStore) ReadFrom(
	ctx context.Context,
	streamID string,
	fromVersion int64,
) ([][]byte, error) {
	blobs, err := s.Memory.ReadFrom(ctx, streamID, fromVersion)
	s.rows.Add(int64(len(blobs)))
	return blobs, err
}

// A long backlog of one aggregate costs one pass over its stream, not one pass
// per entry: the work must grow linearly with the backlog.
func TestProcessor_Recover_ReplaysAnAggregatesStreamOnce(t *testing.T) {
	const events = 300
	ctx := context.Background()
	s := &rowCountingStore{Memory: store.New()}
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	if _, err := es.Write(ctx, mocks.CreateOrderCmd{ID: "a", Total: 1}); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < events; i++ {
		if _, err := es.Write(ctx, updateCmd("a", float64(i))); err != nil {
			t.Fatal(err)
		}
	}
	s.Atomically(ctx, func(tx asynxmd.Tx) error { return enqueueUpTo(ctx, tx, "a", events) })

	var mu sync.Mutex
	var delivered []int64
	p := processor.New(es, bus.NewChannelBus[order](), processor.WithDurableSubscribers([]dispatcher.DurableSub[order]{{
		Match: always,
		Handler: func(_ context.Context, e asynxmd.Event[order]) error {
			mu.Lock()
			defer mu.Unlock()
			delivered = append(delivered, e.Version)
			return nil
		},
	}}))
	t.Cleanup(func() { p.Shutdown(ctx) })

	s.rows.Store(0)
	if err := p.Recover(ctx, 50); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	p.WaitPublish()

	mu.Lock()
	defer mu.Unlock()
	if len(delivered) != events || !slices.IsSorted(delivered) {
		t.Fatalf("delivered %d events (sorted: %v), want %d in order", len(delivered), slices.IsSorted(delivered), events)
	}
	if rows := s.rows.Load(); rows > 2*events {
		t.Fatalf("recovery read %d stream rows for %d pending entries, want a single pass", rows, events)
	}
}

func TestProcessor_Resume_ReplaysAnAggregatesStreamOnce(t *testing.T) {
	const events = 60
	var healthy atomic.Bool
	parked := make(chan struct{}, 4)
	s := &rowCountingStore{Memory: store.New()}
	f := newFixture(t, s, parkingOpts(&healthy,
		processor.WithDeliveryErrorHandler(func(_ context.Context, _ asynxmd.Event[order], err error, _ int) {
			if errors.Is(err, asynxmd.ErrDeliveryParked) {
				parked <- struct{}{}
			}
		}))...)
	ctx := context.Background()
	for i := range events {
		if _, err := f.p.Send(ctx, updateCmd("o1", float64(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	<-parked

	healthy.Store(true)
	s.rows.Store(0)
	if err := f.p.Resume(ctx, 10); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	f.p.WaitPublish()

	if pending, _ := s.PendingDeliveries(ctx, 1000); len(pending) != 0 {
		t.Fatalf("pending after Resume = %d entries", len(pending))
	}
	if rows := s.rows.Load(); rows > 2*events {
		t.Fatalf("resume read %d stream rows for %d pending entries, want a single pass", rows, events)
	}
}

// logCountingStore counts the delivery-log rows its reads return.
type logCountingStore struct {
	store.Memory
	rows  atomic.Int64
	reads atomic.Int64
}

func (s *logCountingStore) PendingDeliveries(
	ctx context.Context,
	limit int,
) ([]asynxmd.Delivery, error) {
	pending, err := s.Memory.PendingDeliveries(ctx, limit)
	s.reads.Add(1)
	s.rows.Add(int64(len(pending)))
	return pending, err
}

// Resuming many parked aggregates reads the delivery log once for all of them,
// not once per aggregate and not once per group of them: an outage that parks
// many chats must not make the recovery quadratic in them.
func TestProcessor_Resume_ReadsTheDeliveryLogOnceForAllAggregates(t *testing.T) {
	const aggregates = 600
	var healthy atomic.Bool
	parked := make(chan struct{}, aggregates)
	s := &logCountingStore{Memory: store.New()}
	f := newFixture(t, s, parkingOpts(&healthy,
		processor.WithDeliveryErrorHandler(func(_ context.Context, _ asynxmd.Event[order], err error, _ int) {
			if errors.Is(err, asynxmd.ErrDeliveryParked) {
				parked <- struct{}{}
			}
		}))...)
	ctx := context.Background()
	for i := range aggregates {
		if _, err := f.p.Send(ctx, updateCmd(fmt.Sprintf("o%03d", i), 1)); err != nil {
			t.Fatal(err)
		}
	}
	for range aggregates {
		select {
		case <-parked:
		case <-time.After(10 * time.Second):
			t.Fatalf("%d of %d aggregates parked", len(f.p.Paused()), aggregates)
		}
	}

	healthy.Store(true)
	s.rows.Store(0)
	if err := f.p.Resume(ctx, 10); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	f.p.WaitPublish()

	if len(f.p.Paused()) != 0 {
		t.Fatalf("still parked: %d aggregates", len(f.p.Paused()))
	}
	rows, reads := s.rows.Load(), s.reads.Load()
	if rows > 3*aggregates {
		t.Fatalf("resume read %d delivery-log rows for %d pending entries, want a single pass over the log", rows, aggregates)
	}
	if err := f.p.Resume(ctx, 10); err != nil {
		t.Fatalf("second Resume: %v", err)
	}
	if again := s.reads.Load(); again != reads {
		t.Fatalf("a Resume with nothing parked read the delivery log %d more times", again-reads)
	}
}

// A command on one aggregate may send to another from inside its validation. A
// resume must not hold one aggregate's lock while waiting for another's, or the
// two wait for each other forever.
func TestProcessor_Resume_DoesNotDeadlockWithACommandSendingToAnotherAggregate(t *testing.T) {
	var healthy atomic.Bool
	parked := make(chan struct{}, 2)
	var readA sync.Once
	redelivering := make(chan struct{})
	s := &readHookStore{Memory: store.New(), onRead: func(streamID string) {
		if streamID == "events:a" {
			readA.Do(func() { close(redelivering) })
		}
	}}
	f := newFixture(t, s, parkingOpts(&healthy,
		processor.WithDeliveryErrorHandler(func(_ context.Context, _ asynxmd.Event[order], err error, _ int) {
			if errors.Is(err, asynxmd.ErrDeliveryParked) {
				parked <- struct{}{}
			}
		}))...)
	ctx := context.Background()
	for _, id := range []string{"a", "b"} {
		if _, err := f.p.Send(ctx, updateCmd(id, 1)); err != nil {
			t.Fatal(err)
		}
		<-parked
	}
	healthy.Store(true)

	validating := make(chan struct{})
	cmd := validatingUpdate{UpdateOrderCmd: updateCmd("b", 2), validate: func() error {
		close(validating)
		select {
		case <-redelivering:
		case <-time.After(3 * time.Second):
		}
		sendCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		_, err := f.p.Send(sendCtx, updateCmd("a", 3))
		return err
	}}
	sent := make(chan error, 1)
	go func() { _, err := f.p.Send(ctx, cmd); sent <- err }()
	<-validating

	resumed := make(chan error, 1)
	go func() { resumed <- f.p.Resume(ctx, 10) }()
	for _, c := range []struct {
		name string
		done <-chan error
	}{{"command", sent}, {"Resume", resumed}} {
		select {
		case err := <-c.done:
			if err != nil {
				t.Fatalf("%s: %v", c.name, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s is deadlocked", c.name)
		}
	}
}

type validatingUpdate struct {
	mocks.UpdateOrderCmd
	validate func() error
}

func (c validatingUpdate) Validate(*order) error { return c.validate() }

// readHookStore reports every stream read.
type readHookStore struct {
	store.Memory
	onRead func(streamID string)
}

func (s *readHookStore) ReadFrom(
	ctx context.Context,
	streamID string,
	fromVersion int64,
) ([][]byte, error) {
	s.onRead(streamID)
	return s.Memory.ReadFrom(ctx, streamID, fromVersion)
}

// staleLogStore returns the delivery log as it was before afterRead ran, which
// commits more events: the read of the log predates them.
type staleLogStore struct {
	store.Memory
	afterRead func()
	ran       atomic.Bool
	sender    *processor.Processor[order]
}

func (s *staleLogStore) PendingDeliveries(
	ctx context.Context,
	limit int,
) ([]asynxmd.Delivery, error) {
	pending, err := s.Memory.PendingDeliveries(ctx, limit)
	if s.ran.CompareAndSwap(false, true) {
		s.afterRead()
	}
	return pending, err
}

// An event committed while the delivery log is being read is on no scanned
// entry; the resume must still deliver it, ahead of anything later.
func TestProcessor_Resume_DeliversAnEventCommittedAfterItReadTheLog(t *testing.T) {
	var healthy atomic.Bool
	parked := make(chan struct{}, 1)
	var mu sync.Mutex
	var delivered []int64
	var s *staleLogStore
	s = &staleLogStore{Memory: store.New(), afterRead: func() {
		sendCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := s.sender.Send(sendCtx, updateCmd("o1", 2)); err != nil {
			t.Errorf("a command during the resume: %v", err)
		}
	}}
	f := newFixture(t, s, parkingOpts(&healthy,
		processor.WithDeliveryErrorHandler(func(_ context.Context, _ asynxmd.Event[order], err error, _ int) {
			if errors.Is(err, asynxmd.ErrDeliveryParked) {
				parked <- struct{}{}
			}
		}),
		processor.WithDurableSubscribers([]dispatcher.DurableSub[order]{{
			Match: always,
			Handler: func(_ context.Context, e asynxmd.Event[order]) error {
				if !healthy.Load() {
					return errors.New("down")
				}
				mu.Lock()
				delivered = append(delivered, e.Version)
				mu.Unlock()
				return nil
			},
		}}))...)
	s.sender = f.p
	ctx := context.Background()
	if _, err := f.p.Send(ctx, updateCmd("o1", 1)); err != nil {
		t.Fatal(err)
	}
	<-parked

	healthy.Store(true)
	if err := f.p.Resume(ctx, 10); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	f.p.WaitPublish()
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(delivered, []int64{1, 2}) {
		t.Fatalf("delivered %v, want [1 2]", delivered)
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 0 {
		t.Fatalf("pending after Resume = %v", pending)
	}
}

// A resume that finds an aggregate already resumed by another leaves it alone:
// redelivering what its own, stale, read of the log lists would deliver the same
// events twice.
func TestProcessor_Resume_SkipsAnAggregateAnotherResumeLiftedMeanwhile(t *testing.T) {
	var healthy atomic.Bool
	parked := make(chan struct{}, 1)
	var mu sync.Mutex
	var delivered []int64
	var s *staleLogStore
	s = &staleLogStore{Memory: store.New(), afterRead: func() {
		if err := s.sender.Resume(context.Background(), 10); err != nil {
			t.Errorf("the second Resume: %v", err)
		}
	}}
	f := newFixture(t, s, parkingOpts(&healthy, parkedSignal(parked),
		processor.WithDurableSubscribers([]dispatcher.DurableSub[order]{{
			Match: always,
			Handler: func(_ context.Context, e asynxmd.Event[order]) error {
				if !healthy.Load() {
					return errors.New("down")
				}
				mu.Lock()
				delivered = append(delivered, e.Version)
				mu.Unlock()
				return nil
			},
		}}))...)
	s.sender = f.p
	if _, err := f.p.Send(context.Background(), updateCmd("o1", 1)); err != nil {
		t.Fatal(err)
	}
	<-parked

	healthy.Store(true)
	if err := f.p.Resume(context.Background(), 10); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	f.p.WaitPublish()
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(delivered, []int64{1}) {
		t.Fatalf("delivered %v, want [1] once", delivered)
	}
}

// A park lifted by another resume and imposed again before this one reaches the
// aggregate is a different park: the log read this resume made describes the old
// one. Redelivering from it would deliver an acknowledged event twice and miss
// the one that parked the aggregate again, so the aggregate is left for the next
// Resume.
func TestProcessor_Resume_LeavesAParkImposedAgainSinceItReadTheLog(t *testing.T) {
	var healthy atomic.Bool
	parked := make(chan struct{}, 2)
	var mu sync.Mutex
	var delivered []int64
	var s *staleLogStore
	s = &staleLogStore{Memory: store.New(), afterRead: func() {
		ctx := context.Background()
		if err := s.sender.Resume(ctx, 10); err != nil {
			t.Errorf("the inner Resume: %v", err)
		}
		s.sender.WaitPublish()
		healthy.Store(false)
		if _, err := s.sender.Send(ctx, updateCmd("o1", 2)); err != nil {
			t.Errorf("the command that parks again: %v", err)
		}
		<-parked
		healthy.Store(true)
	}}
	f := newFixture(t, s, parkingOpts(&healthy, parkedSignal(parked),
		processor.WithDurableSubscribers([]dispatcher.DurableSub[order]{{
			Match: always,
			Handler: func(_ context.Context, e asynxmd.Event[order]) error {
				if !healthy.Load() {
					return errors.New("down")
				}
				mu.Lock()
				delivered = append(delivered, e.Version)
				mu.Unlock()
				return nil
			},
		}}))...)
	s.sender = f.p
	ctx := context.Background()
	if _, err := f.p.Send(ctx, updateCmd("o1", 1)); err != nil {
		t.Fatal(err)
	}
	<-parked

	healthy.Store(true)
	if err := f.p.Resume(ctx, 10); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	f.p.WaitPublish()
	if paused := f.p.Paused(); !slices.Equal(paused, []string{"o1"}) {
		t.Fatalf("Paused = %v, want o1 left parked for the next Resume", paused)
	}
	if err := f.p.Resume(ctx, 10); err != nil {
		t.Fatalf("second Resume: %v", err)
	}
	f.p.WaitPublish()
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(delivered, []int64{1, 2}) {
		t.Fatalf("delivered %v, want [1 2], each once and in order", delivered)
	}
}

// A Resume whose context ends part-way stops there instead of going on to the
// aggregates it did not get to.
func TestProcessor_Resume_StopsAtTheAggregateWhereItsContextEnds(t *testing.T) {
	var healthy atomic.Bool
	parked := make(chan struct{}, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &readHookStore{Memory: store.New(), onRead: func(string) { cancel() }}
	f := newFixture(t, s, parkingOpts(&healthy, parkedSignal(parked))...)
	for _, id := range []string{"o1", "o2", "o3"} {
		if _, err := f.p.Send(context.Background(), updateCmd(id, 1)); err != nil {
			t.Fatal(err)
		}
		<-parked
	}

	healthy.Store(true)
	f.p.Resume(ctx, 10)
	paused := f.p.Paused()
	if !slices.Contains(paused, "o2") || !slices.Contains(paused, "o3") {
		t.Fatalf("Paused = %v, want o2 and o3 left alone once the context ended during o1", paused)
	}
}

type idemOrderUpdate struct {
	mocks.UpdateOrderCmd
	Cmd string
}

func (c idemOrderUpdate) CommandID() string { return c.Cmd }

// failingReadsStore fails event-stream reads while fail is set: of every stream,
// or only of stream when that is not empty.
type failingReadsStore struct {
	store.Memory
	fail   atomic.Bool
	stream string
}

func (s *failingReadsStore) ReadFrom(
	ctx context.Context,
	streamID string,
	fromVersion int64,
) ([][]byte, error) {
	if s.fail.Load() && (s.stream == "" || s.stream == streamID) {
		return nil, errors.New("stream unreadable")
	}
	return s.Memory.ReadFrom(ctx, streamID, fromVersion)
}

// parkWithTwoCommands parks o1 on the failure of command c-1, whose handler ran
// and failed, then commits c-2 behind the park, which no handler took.
func parkWithTwoCommands(
	t *testing.T,
	f fixture,
	parked <-chan struct{},
) {
	t.Helper()
	ctx := context.Background()
	if _, err := f.p.Send(ctx, idemOrderUpdate{updateCmd("o1", 1), "c-1"}); err != nil {
		t.Fatal(err)
	}
	<-parked
	_, err := f.p.SendWait(ctx, idemOrderUpdate{updateCmd("o1", 2), "c-2"})
	if !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("c-2 behind the park = %v, want ErrDeliveryParked", err)
	}
}

func parkedSignal(parked chan<- struct{}) processor.ProcessorOpt[order] {
	return processor.WithDeliveryErrorHandler(func(_ context.Context, _ asynxmd.Event[order], err error, _ int) {
		if errors.Is(err, asynxmd.ErrDeliveryParked) {
			select {
			case parked <- struct{}{}:
			default:
			}
		}
	})
}

func retryOutcomes(
	t *testing.T,
	f fixture,
) (ranAndFailed, neverTaken error) {
	t.Helper()
	ctx := context.Background()
	_, ranAndFailed = f.p.SendWait(ctx, idemOrderUpdate{updateCmd("o1", 1), "c-1"})
	_, neverTaken = f.p.SendWait(ctx, idemOrderUpdate{updateCmd("o1", 2), "c-2"})
	return ranAndFailed, neverTaken
}

// A resume that cannot read the delivery log changes nothing: the retry of an
// event no handler took is still told so.
func TestProcessor_Resume_AfterALogReadFailureRetriesStillReportTheParkedEvent(t *testing.T) {
	var healthy atomic.Bool
	parked := make(chan struct{}, 1)
	s := &flakyPendingStore{Memory: store.New()}
	f := newFixture(t, s, parkingOpts(&healthy, parkedSignal(parked))...)
	parkWithTwoCommands(t, f, parked)

	s.fail.Store(true)
	if err := f.p.Resume(context.Background(), 10); err == nil {
		t.Fatal("Resume hid the unreadable log")
	}
	ranAndFailed, neverTaken := retryOutcomes(t, f)
	if ranAndFailed != nil || !errors.Is(neverTaken, asynxmd.ErrDeliveryParked) {
		t.Fatalf("retries = %v, %v; want nil for c-1 (its handler ran) and ErrDeliveryParked for c-2", ranAndFailed, neverTaken)
	}
}

// A resume that fails to replay the stream leaves the aggregate parked with the
// same account of what ran: the event whose handler failed stays "ran".
func TestProcessor_Resume_AfterAReplayFailureRetriesKeepTheirAnswers(t *testing.T) {
	var healthy atomic.Bool
	parked := make(chan struct{}, 1)
	s := &failingReadsStore{Memory: store.New()}
	f := newFixture(t, s, parkingOpts(&healthy, parkedSignal(parked))...)
	parkWithTwoCommands(t, f, parked)

	s.fail.Store(true)
	if err := f.p.Resume(context.Background(), 10); err == nil {
		t.Fatal("Resume hid the unreadable stream")
	}
	s.fail.Store(false)
	if paused := f.p.Paused(); len(paused) != 1 {
		t.Fatalf("Paused = %v, want o1 parked again", paused)
	}
	ranAndFailed, neverTaken := retryOutcomes(t, f)
	if ranAndFailed != nil || !errors.Is(neverTaken, asynxmd.ErrDeliveryParked) {
		t.Fatalf("retries = %v, %v; want nil for c-1 (its handler ran) and ErrDeliveryParked for c-2", ranAndFailed, neverTaken)
	}
}

// ackSignalStore reports every acknowledged delivery.
type ackSignalStore struct {
	store.Memory
	acked chan struct{}
}

func (s *ackSignalStore) AckDelivery(
	ctx context.Context,
	aggregateID string,
	version int64,
) error {
	err := s.Memory.AckDelivery(ctx, aggregateID, version)
	select {
	case s.acked <- struct{}{}:
	default:
	}
	return err
}

// One aggregate that cannot be redelivered does not hold the others back: they
// are resumed, it stays parked, and the error names it.
func TestProcessor_Resume_AnAggregateThatFailsDoesNotStopTheOthers(t *testing.T) {
	var healthy atomic.Bool
	parked := make(chan struct{}, 2)
	s := &failingReadsStore{Memory: store.New(), stream: "events:o1"}
	f := newFixture(t, s, parkingOpts(&healthy, parkedSignal(parked))...)
	ctx := context.Background()
	for _, id := range []string{"o1", "o2"} {
		if _, err := f.p.Send(ctx, updateCmd(id, 1)); err != nil {
			t.Fatal(err)
		}
		<-parked
	}

	healthy.Store(true)
	s.fail.Store(true)
	err := f.p.Resume(ctx, 10)
	if err == nil || !strings.Contains(err.Error(), "o1") {
		t.Fatalf("Resume = %v, want an error naming o1", err)
	}
	f.p.WaitPublish()
	if paused := f.p.Paused(); !slices.Equal(paused, []string{"o1"}) {
		t.Fatalf("Paused = %v, want only o1", paused)
	}
	pending, _ := s.PendingDeliveries(ctx, 10)
	if len(pending) != 1 || pending[0].AggregateID != "o1" {
		t.Fatalf("pending = %v, want only o1's entry: o2 was redelivered", pending)
	}
}

// A recovery that gets through only part of an aggregate's backlog does not
// claim that the events it delivered went undelivered. The third event's
// upcaster fails once the first two have been handled and acknowledged, which
// makes the recovery fail halfway, deterministically.
func TestProcessor_Recover_PartialRecoveryDoesNotReportDeliveredEventsAsParked(t *testing.T) {
	ctx := context.Background()
	s := &ackSignalStore{Memory: store.New(), acked: make(chan struct{}, 8)}
	var healthy atomic.Bool
	parked := make(chan struct{}, 1)
	first := newFixture(t, s, parkingOpts(&healthy, parkedSignal(parked))...)
	for i, id := range []string{"c-1", "c-2", "c-3"} {
		if _, err := first.p.Send(ctx, idemOrderUpdate{updateCmd("o1", float64(i+1)), id}); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			<-parked
		}
	}
	if err := first.p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	var upcasts atomic.Int32
	upcaster := func(_ context.Context, _ string, patches []byte) ([]byte, error) {
		if upcasts.Add(1) != 3 {
			return patches, nil
		}
		for range 2 {
			select {
			case <-s.acked:
			case <-time.After(5 * time.Second):
			}
		}
		return nil, errors.New("event cannot be upcast")
	}
	es := eventstore.New[order](s, store.NewSnapshots(), map[int]asynxmd.Upcaster{1: upcaster}, 2, nil)
	reported := make(chan struct{}, 1)
	p := processor.New(es, bus.NewChannelBus[order](),
		processor.WithDurableDelivery[order](func(name string) bool { return name == "OrderUpdated" }),
		processor.WithPublishErrorHandler(func(context.Context, asynxmd.Event[order], error) { reported <- struct{}{} }),
		processor.WithDurableSubscribers([]dispatcher.DurableSub[order]{{
			Match:   always,
			Handler: func(context.Context, asynxmd.Event[order]) error { return nil },
		}}))
	t.Cleanup(func() { p.Shutdown(ctx) })

	if err := p.Recover(ctx, 10); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	select {
	case <-reported:
	case <-time.After(5 * time.Second):
		t.Fatal("the failure was not reported")
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 1 || pending[0].Version != 3 {
		t.Fatalf("pending = %v, want only version 3", pending)
	}
	_, delivered := p.SendWait(ctx, idemOrderUpdate{updateCmd("o1", 1), "c-1"})
	_, behind := p.SendWait(ctx, idemOrderUpdate{updateCmd("o1", 3), "c-3"})
	if delivered != nil || !errors.Is(behind, asynxmd.ErrDeliveryParked) {
		t.Fatalf("retries = %v, %v; want nil for c-1 (delivered and acknowledged) and ErrDeliveryParked for c-3", delivered, behind)
	}
}

// Entries behind an event that cannot be rebuilt stay pending while the entries
// before it are still delivered.
func TestProcessor_Recover_DeliversWhatPrecedesAnUnreadableEvent(t *testing.T) {
	ctx := context.Background()
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	for _, cmd := range []asynxmd.Command[order]{mocks.CreateOrderCmd{ID: "a", Total: 1}, updateCmd("a", 2)} {
		if _, err := es.Write(ctx, cmd); err != nil {
			t.Fatal(err)
		}
	}
	s.Append(ctx, "events:a", 3, []byte("!!!"))
	s.Atomically(ctx, func(tx asynxmd.Tx) error {
		return errors.Join(tx.EnqueueDelivery(ctx, "a", 1), tx.EnqueueDelivery(ctx, "a", 3))
	})

	var got atomic.Int32
	reported := make(chan error, 1)
	p := processor.New(es, bus.NewChannelBus[order](),
		processor.WithPublishErrorHandler(func(_ context.Context, _ asynxmd.Event[order], err error) { reported <- err }),
		processor.WithDurableSubscribers([]dispatcher.DurableSub[order]{{
			Match:   always,
			Handler: func(context.Context, asynxmd.Event[order]) error { got.Add(1); return nil },
		}}))
	t.Cleanup(func() { p.Shutdown(ctx) })

	if err := p.Recover(ctx, 10); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	select {
	case <-reported:
	case <-time.After(5 * time.Second):
		t.Fatal("the unreadable event was not reported")
	}
	p.WaitPublish()
	pending, _ := s.PendingDeliveries(ctx, 10)
	versions := make([]int64, len(pending))
	for i, d := range pending {
		versions[i] = d.Version
	}
	if !slices.Contains(versions, 3) {
		t.Fatalf("pending = %v, want version 3 (behind the unreadable event) kept", versions)
	}
	delivered := got.Load() == 1
	if delivered == slices.Contains(versions, 1) {
		t.Fatalf("delivered %d, pending %v: version 1 must be either delivered and acknowledged, or still pending", got.Load(), versions)
	}
}

func enqueueUpTo(
	ctx context.Context,
	tx asynxmd.Tx,
	aggregateID string,
	last int64,
) error {
	for v := int64(1); v <= last; v++ {
		if err := tx.EnqueueDelivery(ctx, aggregateID, v); err != nil {
			return err
		}
	}
	return nil
}
