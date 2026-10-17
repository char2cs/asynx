package processor_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor"
	"github.com/char2cs/asynx/internal/processor/exec"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

type fixture struct {
	p    *processor.Processor[order]
	s    store.Memory
	snap store.SnapshotMemory
	b    *bus.ChannelBus[order]
}

func newFixture(t *testing.T, s store.Memory, opts ...processor.ProcessorOpt[order]) fixture {
	t.Helper()
	snap := store.NewSnapshots()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, snap, nil, 1, nil)
	p := processor.New(es, b, opts...)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return fixture{p: p, s: s, snap: snap, b: b}
}

func TestProcessor_Forget_ErasesAndNotifiesHandlers(t *testing.T) {
	f := newFixture(t, store.New())
	ctx := context.Background()

	var tombstones atomic.Int32
	f.b.Subscribe("asynx\\.aggregate\\.forget", func(context.Context, asynxmd.Event[order]) { tombstones.Add(1) })

	if _, err := f.p.Send(ctx, mocks.CreateOrderCmd{ID: "o1", Total: 5}); err != nil {
		t.Fatal(err)
	}
	if err := f.p.Forget(ctx, "o1"); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if tombstones.Load() != 1 {
		t.Errorf("forget handler calls = %d, want 1", tombstones.Load())
	}
	if blobs, _ := f.s.ReadFrom(ctx, "events:o1", 1); len(blobs) != 0 {
		t.Errorf("stream not erased: %d blobs", len(blobs))
	}
	if err := f.p.Forget(ctx, "o1"); !errors.Is(err, asynxmd.ErrValidation) {
		t.Errorf("second Forget err = %v, want ErrValidation", err)
	}
}

func TestProcessor_Forget_RejectedWhenShuttingDownOrCancelled(t *testing.T) {
	f := newFixture(t, store.New())

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.p.Forget(cancelled, "o1"); !errors.Is(err, asynxmd.ErrContextCancelled) {
		t.Errorf("cancelled: err = %v, want ErrContextCancelled", err)
	}

	f.p.Shutdown(context.Background())
	if err := f.p.Forget(context.Background(), "o1"); !errors.Is(err, asynxmd.ErrShuttingDown) {
		t.Errorf("shutting down: err = %v, want ErrShuttingDown", err)
	}
}

func TestProcessor_Options_SyncProjectionsDurableDeliverySnapshotErrors(t *testing.T) {
	var projected, snapErrs atomic.Int32
	s := store.New()
	f := newFixture(t, s,
		processor.WithSyncProjections([]exec.SyncProjection[order]{{
			Match: func(string) bool { return true },
			Fn: func(context.Context, asynxmd.Tx, asynxmd.Event[order]) error {
				projected.Add(1)
				return nil
			},
		}}),
		processor.WithDurableDelivery[order](func(string) bool { return true }),
		processor.WithSnapshotErrorHandler(func(context.Context, asynxmd.Event[order], error) { snapErrs.Add(1) }),
	)
	f.snap.SetError("o1", errors.New("snapshot down"))

	ctx := context.Background()
	cmd := mocks.UpdateOrderCmd{ID: "o1", NewState: order{ID: "o1", Total: 1}, Snapshot: true}
	if _, err := f.p.SendWait(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	f.p.WaitPublish()

	if projected.Load() != 1 || snapErrs.Load() != 1 {
		t.Errorf("projected=%d snapshotErrors=%d, want 1 and 1", projected.Load(), snapErrs.Load())
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 0 {
		t.Errorf("delivery not acked after SendWait: %v", pending)
	}
}

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
	var got atomic.Int32
	b.Subscribe("OrderCreated", func(context.Context, asynxmd.Event[order]) { got.Add(1) })
	p := processor.New(es, b)
	t.Cleanup(func() { p.Shutdown(context.Background()) })

	// batch <= 0 falls back to the default; batch 2 forces multiple rounds.
	if err := p.Recover(context.Background(), 2); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got.Load() != 3 {
		t.Errorf("redelivered %d events, want 3", got.Load())
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

	// Event blob that cannot be replayed.
	s := store.New()
	s.Append(ctx, "events:bad", 1, []byte("!!!"))
	s.Atomically(ctx, func(tx asynxmd.Tx) error { return tx.EnqueueDelivery(ctx, "bad", 1) })
	es2 := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	p2 := processor.New(es2, bus.NewChannelBus[order]())
	t.Cleanup(func() { p2.Shutdown(ctx) })
	if err := p2.Recover(ctx, 10); err == nil {
		t.Error("Recover succeeded with an unreplayable event")
	}
}

func TestProcessor_Recover_HonoursContextWhileWaiting(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	seedPending(t, s, es, "a")

	b := bus.NewChannelBus[order]()
	release := make(chan struct{})
	b.Subscribe("OrderCreated", func(context.Context, asynxmd.Event[order]) { <-release })
	p := processor.New(es, b)
	t.Cleanup(func() { close(release); p.Shutdown(context.Background()) })

	ctx, cancel := context.WithCancel(context.Background())
	go func() { cancel() }()
	if err := p.Recover(ctx, 10); err == nil {
		t.Fatal("Recover ignored a cancelled context")
	}
}

type ackErrStore struct{ store.Memory }

func (ackErrStore) AckDelivery(context.Context, string, int64) error { return errors.New("ack failed") }

func TestProcessor_Recover_SurfacesAckFailureForVanishedEvent(t *testing.T) {
	s := ackErrStore{store.New()}
	ctx := context.Background()
	s.Atomically(ctx, func(tx asynxmd.Tx) error { return tx.EnqueueDelivery(ctx, "ghost", 1) })
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	p := processor.New(es, bus.NewChannelBus[order]())
	t.Cleanup(func() { p.Shutdown(ctx) })

	if err := p.Recover(ctx, 10); err == nil {
		t.Fatal("Recover hid an ack failure")
	}
}

type noAckStore struct{ store.Memory }

func (noAckStore) AckDelivery(context.Context, string, int64) error { return nil }

func TestProcessor_Recover_FailsWhenAcknowledgementsDoNotStick(t *testing.T) {
	s := noAckStore{store.New()}
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	seedPending(t, s.Memory, es, "a")

	b := bus.NewChannelBus[order]()
	p := processor.New(es, b)
	t.Cleanup(func() { p.Shutdown(context.Background()) })

	if err := p.Recover(context.Background(), 10); err == nil {
		t.Fatal("Recover looped or succeeded although the ack never stuck")
	}
}
