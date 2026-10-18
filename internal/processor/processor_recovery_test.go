package processor_test

import (
	"context"
	"errors"
	"slices"
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
	if err := f.p.Resume(cancelled, 10); err == nil {
		t.Fatal("Resume ignored its cancelled context")
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
