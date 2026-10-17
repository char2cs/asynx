package processor_test

import (
	"context"
	"errors"
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

func updateCmd(id string, total float64) mocks.UpdateOrderCmd {
	return mocks.UpdateOrderCmd{ID: id, NewState: order{ID: id, Total: total}}
}

func always(string) bool { return true }

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

func TestProcessor_Options_MaxQueuedAndDeadLetter(t *testing.T) {
	var healthy atomic.Bool
	dead := make(chan int64, 4)
	f := newFixture(t, store.New(), parkingOpts(&healthy,
		processor.WithDeliveryMaxQueued[order](100),
		processor.WithDeadLetterHandler(func(_ context.Context, e asynxmd.Event[order], _ error) error {
			dead <- e.Version
			return nil
		}))...)
	if _, err := f.p.Send(context.Background(), updateCmd("o1", 1)); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-dead:
		if v != 1 {
			t.Fatalf("dead-lettered v%d", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("dead-letter handler never called")
	}
}

func TestProcessor_SendWait_GivesUpWaitingButReturnsTheCommittedEvent(t *testing.T) {
	f := newFixture(t, store.New())
	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	f.b.Subscribe("OrderUpdated", func(context.Context, asynxmd.Event[order]) {
		close(entered)
		<-release
	})

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		evt asynxmd.Event[order]
		err error
	}
	got := make(chan result, 1)
	go func() {
		evt, err := f.p.SendWait(ctx, updateCmd("o1", 1))
		got <- result{evt, err}
	}()
	<-entered
	cancel()
	r := <-got
	if !errors.Is(r.err, asynxmd.ErrContextCancelled) {
		t.Fatalf("err = %v, want ErrContextCancelled", r.err)
	}
	if r.evt.AggregateID != "o1" || r.evt.Version != 1 {
		t.Fatalf("event = %+v, want the committed v1", r.evt)
	}
}

func TestProcessor_Shutdown_WaitsForForgetInFlightUntilItsContextEnds(t *testing.T) {
	f := newFixture(t, store.New())
	ctx := context.Background()
	if _, err := f.p.Send(ctx, mocks.CreateOrderCmd{ID: "o1", Total: 1}); err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	f.b.Subscribe("asynx\\.aggregate\\.forget", func(context.Context, asynxmd.Event[order]) {
		close(entered)
		<-release
	})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		f.p.Forget(ctx, "o1")
	}()
	<-entered

	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := f.p.Shutdown(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown err = %v, want the context's deadline while a Forget is in flight", err)
	}
	close(release)
	wg.Wait()
}

var _ = bus.NewChannelBus[order]
