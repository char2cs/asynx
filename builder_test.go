package asynx_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

func TestNewBuilderDefaults(t *testing.T) {
	b := asynx.New[mocks.Order]()
	if b == nil {
		t.Fatal("New returned nil")
	}
}

func TestBuildRequiresEventStore(t *testing.T) {
	_, err := asynx.New[mocks.Order]().Build()
	if err != models.ErrMissingEventStore {
		t.Errorf("expected ErrMissingEventStore, got %v", err)
	}
}

func TestBuildSucceeds(t *testing.T) {
	instance, err := asynx.New[mocks.Order]().
		WithEventStore(&mocks.Store{}).
		WithSnapshotStore(&mocks.SnapshotStore{}).
		Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	if instance == nil {
		t.Fatal("Build() returned nil instance")
	}
}

func TestBuildMultipleTimesIndependent(t *testing.T) {
	b := asynx.New[mocks.Order]().
		WithEventStore(&mocks.Store{}).
		WithSnapshotStore(&mocks.SnapshotStore{})

	i1, _ := b.Build()
	i2, _ := b.Build()

	if i1 == i2 {
		t.Error("Build() must return a new instance each call")
	}
}

func TestBuilderFluent(t *testing.T) {
	upcaster := func(ctx context.Context, eventName string, raw []byte) ([]byte, error) { return raw, nil }
	panicHandler := func(_ context.Context, _ models.Event[mocks.Order], _ any) {}

	_, err := asynx.New[mocks.Order]().
		WithEventStore(&mocks.Store{}).
		WithSnapshotStore(&mocks.SnapshotStore{}).
		WithBus(&mocks.Bus[mocks.Order]{}).
		WithShardingOpts(asynx.ShardingOpts{Shards: 16, QueueDepth: 100, WorkersPerShard: 4}).
		WithSchemaVersion(3).
		WithUpcaster(1, upcaster).
		WithUpcaster(2, upcaster).
		WithPanicHandler(panicHandler).
		Build()

	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
}

func TestBuilderWithCorruptionHook(t *testing.T) {
	called := false
	hook := func(err error) { called = true }

	_, err := asynx.New[mocks.Order]().
		WithEventStore(&mocks.Store{}).
		WithSnapshotStore(&mocks.SnapshotStore{}).
		WithCorruptionHook(hook).
		Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	_ = called // hook is wired; invocation tested at integration level
}

func TestBuilderWithPublishErrorHandler(t *testing.T) {
	called := false
	handler := func(_ context.Context, _ models.Event[mocks.Order], _ error) { called = true }

	_, err := asynx.New[mocks.Order]().
		WithEventStore(&mocks.Store{}).
		WithSnapshotStore(&mocks.SnapshotStore{}).
		WithPublishErrorHandler(handler).
		Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	_ = called // handler is wired; invocation tested at the exec layer
}

func TestShardingOptsZeroValue(t *testing.T) {
	opts := asynx.ShardingOpts{}
	if opts.Shards != 0 || opts.QueueDepth != 0 || opts.WorkersPerShard != 0 {
		t.Errorf("unexpected zero ShardingOpts: %+v", opts)
	}
}

func TestBuildWithWorkersPerShard(t *testing.T) {
	instance, err := asynx.New[mocks.Order]().
		WithEventStore(store.New()).
		WithSnapshotStore(store.NewSnapshots()).
		WithShardingOpts(asynx.ShardingOpts{Shards: 8, WorkersPerShard: 1}).
		Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	if instance == nil {
		t.Fatal("Build() returned nil instance")
	}
	t.Cleanup(func() { instance.Shutdown(context.Background()) })

	if _, err := instance.Send(context.Background(), mocks.CreateOrderCmd{ID: "order-1", Total: 10.0}); err != nil {
		t.Fatalf("Send: %v", err)
	}
}

func TestBuild_NilForgetHandler_ReturnsError(t *testing.T) {
	_, err := asynx.New[mocks.Order]().
		WithEventStore(&mocks.Store{}).
		WithSnapshotStore(&mocks.SnapshotStore{}).
		WithForgetHandler(nil).
		Build()
	if err == nil {
		t.Fatal("expected error for nil ForgetHandler, got nil")
	}
}

func TestWithForgetHandler_IsCalledOnForget(t *testing.T) {
	var gotTotal float64

	instance, err := asynx.New[mocks.Order]().
		WithEventStore(store.New()).
		WithSnapshotStore(store.NewSnapshots()).
		WithShardingOpts(asynx.ShardingOpts{Shards: 8, QueueDepth: 1000}).
		WithForgetHandler(func(_ context.Context, e models.Event[mocks.Order]) {
			gotTotal = e.Aggregate.Total
		}).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { instance.Shutdown(context.Background()) })

	ctx := context.Background()
	if _, err := instance.Send(ctx, mocks.CreateOrderCmd{ID: "order-1", Total: 77.0}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := instance.Forget(ctx, "order-1"); err != nil {
		t.Fatalf("Forget: %v", err)
	}

	if gotTotal != 77.0 {
		t.Errorf("ForgetHandler got Total=%v, want 77.0", gotTotal)
	}
}

func TestBuilder_MissingSnapshotStoreIsAnError(t *testing.T) {
	_, err := asynx.New[mocks.Order]().
		WithEventStore(store.New()).
		Build()

	if !errors.Is(err, models.ErrMissingSnapshotStore) {
		t.Errorf("err = %v, want ErrMissingSnapshotStore", err)
	}
}

func TestBuilder_SnapshotStoreSatisfiesBuild(t *testing.T) {
	ax, err := asynx.New[mocks.Order]().
		WithEventStore(store.New()).
		WithSnapshotStore(store.NewSnapshots()).
		Build()

	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if ax == nil {
		t.Fatal("instance = nil")
	}
}

type idemCmd struct {
	mocks.UpdateOrderCmd
	Cmd string
}

func (c idemCmd) CommandID() string { return c.Cmd }

type idemCreate struct {
	mocks.CreateOrderCmd
	Cmd string
}

func (c idemCreate) CommandID() string { return c.Cmd }

func upd(id string, total float64) mocks.UpdateOrderCmd {
	return mocks.UpdateOrderCmd{ID: id, NewState: mocks.Order{ID: id, Total: total}}
}

func build(t *testing.T, b *asynx.Builder[mocks.Order]) asynx.Asynx[mocks.Order] {
	t.Helper()
	inst, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	t.Cleanup(func() { inst.Shutdown(context.Background()) })
	return inst
}

func eight(s store.Memory) *asynx.Builder[mocks.Order] {
	return asynx.New[mocks.Order]().
		WithEventStore(s).
		WithSnapshotStore(store.NewSnapshots()).
		WithShardingOpts(asynx.ShardingOpts{Shards: 1, WorkersPerShard: 8, QueueDepth: 1000})
}

// Review focus 1: same-aggregate flood at WorkersPerShard 8.
func TestGuarantee_SameAggregateFloodWithManyWorkers(t *testing.T) {
	inst := build(t, eight(store.New()))

	var mu sync.Mutex
	var seen []int64
	inst.Subscribe("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) {
		time.Sleep(100 * time.Microsecond)
		mu.Lock()
		seen = append(seen, e.Version)
		mu.Unlock()
	})

	const n = 100
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := inst.Send(context.Background(), upd("agg", float64(i+1))); err != nil {
				t.Errorf("Send: %v", err)
			}
		}()
	}
	wg.Wait()
	waitPublish(t, inst)

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != n {
		t.Fatalf("handler saw %d events, want %d", len(seen), n)
	}
	for i, v := range seen {
		if v != int64(i+1) {
			t.Fatalf("out-of-order delivery: %v", seen)
		}
	}
}

// Review focus 2: crash between commit and ack, restart on the same store.
func TestGuarantee_DurableSubscriberRecoversAfterCrash(t *testing.T) {
	s := store.New()
	ctx := context.Background()

	stuck := make(chan struct{})
	first := build(t, eight(s).WithSubscriber("OrderUpdated", func(context.Context, models.Event[mocks.Order]) error {
		<-stuck // the "process" dies while handling v1
		return nil
	}))
	t.Cleanup(func() { close(stuck) })
	for i := range 3 {
		if _, err := first.Send(ctx, upd("agg", float64(i+1))); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	var versions []int64
	second := build(t, eight(s).WithSubscriber("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) error {
		mu.Lock()
		versions = append(versions, e.Version)
		mu.Unlock()
		return nil
	}))

	// Recovery is queued before Build returns, so a command sent right away is
	// delivered after the recovered events of its aggregate.
	if _, err := second.Send(ctx, upd("agg", 4)); err != nil {
		t.Fatal(err)
	}
	waitPublish(t, second)

	mu.Lock()
	defer mu.Unlock()
	if len(versions) != 4 || versions[0] != 1 || versions[1] != 2 || versions[2] != 3 || versions[3] != 4 {
		t.Fatalf("delivered versions = %v, want [1 2 3 4]", versions)
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 0 {
		t.Fatalf("pending after recovery = %v, want none", pending)
	}
}

func TestGuarantee_RecoverySkipsDeliveriesWhoseEventIsGone(t *testing.T) {
	s := store.New()
	ctx := context.Background()
	s.Atomically(ctx, func(tx models.Tx) error { return tx.EnqueueDelivery(ctx, "ghost", 1) })

	var calls atomic.Int32
	build(t, eight(s).WithSubscriber("OrderUpdated", func(context.Context, models.Event[mocks.Order]) error { calls.Add(1); return nil }))

	if calls.Load() != 0 {
		t.Fatalf("handler called %d times for a vanished event", calls.Load())
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 0 {
		t.Fatalf("pending = %v, want none", pending)
	}
}

type ackErrStore struct{ store.Memory }

func (ackErrStore) AckDelivery(context.Context, string, int64) error { return errors.New("ack failed") }

func TestGuarantee_AckFailuresAreReportedAndTheDeliveryStaysPending(t *testing.T) {
	s := store.New()
	ctx := context.Background()
	var reported atomic.Int32
	inst := build(t, eight(nil).
		WithEventStore(ackErrStore{s}).
		WithPublishErrorHandler(func(context.Context, models.Event[mocks.Order], error) { reported.Add(1) }).
		WithSubscriber("OrderUpdated", func(context.Context, models.Event[mocks.Order]) error { return nil }))

	if _, err := inst.SendWait(ctx, upd("agg", 1)); err != nil {
		t.Fatal(err)
	}
	waitPublish(t, inst)

	if reported.Load() != 1 {
		t.Fatalf("ack failures reported = %d, want 1", reported.Load())
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 1 {
		t.Fatalf("pending = %v, want the unacked delivery to stay for the next start", pending)
	}
}

type pendingErrStore struct{ store.Memory }

func (pendingErrStore) PendingDeliveries(context.Context, int) ([]models.Delivery, error) {
	return nil, errors.New("delivery log unreadable")
}

func TestGuarantee_RecoveryReadFailureFailsBuild(t *testing.T) {
	_, err := eight(nil).
		WithEventStore(pendingErrStore{store.New()}).
		WithSubscriber("OrderUpdated", func(context.Context, models.Event[mocks.Order]) error { return nil }).
		Build()
	if err == nil {
		t.Fatal("Build succeeded with an unreadable delivery log")
	}
}

func TestGuarantee_NoDurableSubscribersWritesNoDeliveries(t *testing.T) {
	s := store.New()
	inst := build(t, eight(s))
	if _, err := inst.Send(context.Background(), upd("agg", 1)); err != nil {
		t.Fatal(err)
	}
	waitPublish(t, inst)
	if pending, _ := s.PendingDeliveries(context.Background(), 10); len(pending) != 0 {
		t.Fatalf("pending = %v, want none", pending)
	}
}

// Review focus 3: a failing sync projection leaves nothing behind.
func TestGuarantee_SyncProjectionFailureRollsEverythingBack(t *testing.T) {
	s := store.New()
	var calls atomic.Int32
	inst := build(t, eight(s).
		WithSyncProjection("OrderUpdated", func(context.Context, models.Tx, models.Event[mocks.Order]) error {
			return errors.New("read model write failed")
		}).
		WithSubscriber("OrderUpdated", func(context.Context, models.Event[mocks.Order]) error { calls.Add(1); return nil }))

	_, err := inst.Send(context.Background(), idemCmd{UpdateOrderCmd: upd("agg", 1), Cmd: "c1"})
	if !errors.Is(err, models.ErrPipelineFailed) || !errors.Is(err, models.ErrSyncProjection) {
		t.Fatalf("err = %v, want ErrPipelineFailed + ErrSyncProjection", err)
	}
	waitPublish(t, inst)

	if ok, _ := inst.Exists(context.Background(), "agg"); ok {
		t.Error("event survived a failed sync projection")
	}
	if _, found, _ := s.GetReceipt(context.Background(), "c1"); found {
		t.Error("receipt survived a failed sync projection")
	}
	if pending, _ := s.PendingDeliveries(context.Background(), 10); len(pending) != 0 {
		t.Errorf("pending = %v, want none", pending)
	}
	if calls.Load() != 0 {
		t.Errorf("subscriber called %d times", calls.Load())
	}
}

func TestGuarantee_SyncProjectionPatternsRegexAndOrder(t *testing.T) {
	var mu sync.Mutex
	var order []string
	rec := func(name string) func(context.Context, models.Tx, models.Event[mocks.Order]) error {
		return func(_ context.Context, _ models.Tx, e models.Event[mocks.Order]) error {
			mu.Lock()
			order = append(order, name+":"+e.EventName)
			mu.Unlock()
			return nil
		}
	}
	inst := build(t, eight(store.New()).
		WithSyncProjection("^Order(Created|Updated)$", rec("regex")).
		WithSyncProjection("OrderUpdated", rec("exact")))

	ctx := context.Background()
	inst.Send(ctx, mocks.CreateOrderCmd{ID: "a", Total: 1})
	inst.Send(ctx, upd("a", 2))

	mu.Lock()
	defer mu.Unlock()
	want := []string{"regex:OrderCreated", "regex:OrderUpdated", "exact:OrderUpdated"}
	if len(order) != len(want) {
		t.Fatalf("calls = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("calls = %v, want %v", order, want)
		}
	}
}

func TestGuarantee_BuildRejectsBadProjectionsAndSubscribers(t *testing.T) {
	noop := func(context.Context, models.Tx, models.Event[mocks.Order]) error { return nil }
	cases := map[string]struct {
		b    *asynx.Builder[mocks.Order]
		want error
	}{
		"empty projection pattern": {eight(store.New()).WithSyncProjection("", noop), models.ErrEmptyPattern},
		"nil projection fn":        {eight(store.New()).WithSyncProjection("X", nil), models.ErrNilHandler},
		"empty subscriber pattern": {eight(store.New()).WithSubscriber("", func(context.Context, models.Event[mocks.Order]) error { return nil }), models.ErrEmptyPattern},
		"nil subscriber handler":   {eight(store.New()).WithSubscriber("X", nil), models.ErrNilHandler},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := tc.b.Build(); !errors.Is(err, tc.want) {
				t.Fatalf("Build err = %v, want %v", err, tc.want)
			}
		})
	}

	if _, err := eight(store.New()).WithSyncProjection("(unclosed", noop).Build(); err == nil {
		t.Fatal("invalid regexp pattern was accepted")
	}
	if _, err := eight(store.New()).WithSubscriber("(unclosed", func(context.Context, models.Event[mocks.Order]) error { return nil }).Build(); err == nil {
		t.Fatal("invalid subscriber pattern was accepted")
	}
}

// Review focus 4 and 6 through the public API.
func TestGuarantee_IdempotentCommands(t *testing.T) {
	s := store.New()
	inst := build(t, eight(s))
	ctx := context.Background()

	first, err := inst.Send(ctx, idemCmd{UpdateOrderCmd: upd("agg", 7), Cmd: "c1"})
	if err != nil {
		t.Fatal(err)
	}
	retry, err := inst.Send(ctx, idemCmd{UpdateOrderCmd: upd("agg", 7), Cmd: "c1"})
	if err != nil || retry.ID != first.ID || retry.Version != 1 {
		t.Fatalf("retry = %+v, %v; want the original event", retry, err)
	}

	if _, err := inst.Send(ctx, idemCreate{CreateOrderCmd: mocks.CreateOrderCmd{ID: "bad", Total: -1}, Cmd: "c2"}); !errors.Is(err, models.ErrValidation) {
		t.Fatalf("err = %v, want ErrValidation", err)
	}
	if _, err := inst.Send(ctx, idemCreate{CreateOrderCmd: mocks.CreateOrderCmd{ID: "bad", Total: 5}, Cmd: "c2"}); !errors.Is(err, models.ErrValidation) {
		t.Fatalf("retry err = %v, want the rejection to stick", err)
	}

	other, err := inst.Send(ctx, idemCmd{UpdateOrderCmd: upd("other", 9), Cmd: "c1"})
	if err != nil || other.ID != first.ID {
		t.Fatalf("same id on another aggregate = %+v, %v; want the original event", other, err)
	}
	if ok, _ := inst.Exists(ctx, "other"); ok {
		t.Error("second aggregate was written")
	}
}

func TestGuarantee_SnapshotFailureIsReportedNotFatal(t *testing.T) {
	ss := store.NewSnapshots()
	var reported atomic.Int32
	inst := build(t, asynx.New[mocks.Order]().
		WithEventStore(store.New()).
		WithSnapshotStore(ss).
		WithSnapshotErrorHandler(func(context.Context, models.Event[mocks.Order], error) { reported.Add(1) }))

	ss.SetError("agg", errors.New("snapshot disk full"))
	evt, err := inst.Send(context.Background(), mocks.UpdateOrderCmd{ID: "agg", NewState: mocks.Order{ID: "agg"}, Snapshot: true})
	if err != nil || evt.Version != 1 {
		t.Fatalf("Send = %+v, %v; want success", evt, err)
	}
	if reported.Load() != 1 {
		t.Fatalf("reported = %d, want 1", reported.Load())
	}
}

// Review focus 5: Forget racing commands, plus pending deliveries.
func TestGuarantee_ForgetFencedAgainstCommands(t *testing.T) {
	s := store.New()
	inst := build(t, eight(s).WithSubscriber("OrderUpdated", func(context.Context, models.Event[mocks.Order]) error { return nil }))
	ctx := context.Background()
	if _, err := inst.Send(ctx, upd("agg", 1)); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := range 30 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if i == 15 {
				if err := inst.Forget(ctx, "agg"); err != nil {
					t.Errorf("Forget: %v", err)
				}
				return
			}
			inst.Send(ctx, upd("agg", float64(i)))
		}()
	}
	wg.Wait()
	waitPublish(t, inst)

	// Whatever exists now must be readable (contiguous from version 1).
	if ok, _ := inst.Exists(ctx, "agg"); ok {
		if _, err := inst.Get(ctx, "agg"); err != nil {
			t.Fatalf("surviving aggregate is unreadable: %v", err)
		}
	}
}

func TestGuarantee_EventsNoDurableSubscriberWantsGetNoDeliveryEntry(t *testing.T) {
	s := store.New()
	release := make(chan struct{})
	inst := build(t, eight(s).WithSubscriber("OrderUpdated", func(context.Context, models.Event[mocks.Order]) error { <-release; return nil }))
	t.Cleanup(func() { close(release) })
	ctx := context.Background()

	if _, err := inst.Send(ctx, mocks.CreateOrderCmd{ID: "a", Total: 1}); err != nil {
		t.Fatal(err)
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 0 {
		t.Fatalf("OrderCreated got a delivery entry: %v", pending)
	}
}

func TestGuarantee_FailedBuildShutsTheProcessorDown(t *testing.T) {
	before := runtime.NumGoroutine()
	for range 5 {
		// Recovery fails (the delivery log is unreadable) after the processor is running.
		_, err := eight(nil).
			WithEventStore(pendingErrStore{store.New()}).
			WithSubscriber("OrderUpdated", func(context.Context, models.Event[mocks.Order]) error { return nil }).
			Build()
		if err == nil {
			t.Fatal("Build should fail")
		}
	}
	eventually(t, "goroutines to settle after failed builds", func() bool {
		return runtime.NumGoroutine() <= before+5
	})
}

// Durable subscribers can fail: the delivery stays pending, retries, and holds
// back the aggregate's later events.
func TestGuarantee_DurableSubscriberFailureRetriesAndKeepsOrder(t *testing.T) {
	s := store.New()
	ctx := context.Background()

	var mu sync.Mutex
	var seen []int64
	var failures atomic.Int32
	var reported atomic.Int32
	inst := build(t, eight(s).
		WithDeliveryRetry(time.Millisecond, 5*time.Millisecond).
		WithDeliveryErrorHandler(func(context.Context, models.Event[mocks.Order], error, int) { reported.Add(1) }).
		WithSubscriber("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) error {
			if e.Version == 1 && failures.Add(1) <= 2 {
				return errors.New("read model unavailable")
			}
			mu.Lock()
			seen = append(seen, e.Version)
			mu.Unlock()
			return nil
		}))

	for i := range 3 {
		if _, err := inst.Send(ctx, upd("agg", float64(i+1))); err != nil {
			t.Fatal(err)
		}
	}

	eventually(t, "deliveries to complete", func() bool {
		pending, _ := s.PendingDeliveries(ctx, 10)
		return len(pending) == 0
	})

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 3 || seen[0] != 1 || seen[1] != 2 || seen[2] != 3 {
		t.Fatalf("delivered versions = %v, want [1 2 3] (v2 must wait for v1)", seen)
	}
	if reported.Load() != 2 {
		t.Fatalf("failures reported = %d, want 2", reported.Load())
	}
}

func TestGuarantee_SendWaitReturnsAfterTheFirstAttemptEvenIfTheHandlerFails(t *testing.T) {
	var attempts atomic.Int32
	inst := build(t, eight(store.New()).
		WithDeliveryRetry(time.Hour, time.Hour).
		WithSubscriber("OrderUpdated", func(context.Context, models.Event[mocks.Order]) error {
			attempts.Add(1)
			return errors.New("down")
		}))

	done := make(chan error, 1)
	go func() {
		_, err := inst.SendWait(context.Background(), upd("agg", 1))
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SendWait: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SendWait blocked on a failing durable handler's retries")
	}
	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d, want 1", attempts.Load())
	}
}

func TestGuarantee_BuildDoesNotHangOnAPoisonDelivery(t *testing.T) {
	s := store.New()
	ctx := context.Background()
	dies := make(chan struct{})
	first := build(t, eight(s).WithSubscriber("OrderUpdated", func(context.Context, models.Event[mocks.Order]) error {
		<-dies // never finishes during the test: the "process" dies
		return nil
	}))
	t.Cleanup(func() { close(dies) })
	if _, err := first.Send(ctx, upd("agg", 1)); err != nil {
		t.Fatal(err)
	}

	var attempts atomic.Int32
	built := make(chan struct{})
	go func() {
		build(t, eight(s).
			WithDeliveryRetry(time.Hour, time.Hour).
			WithSubscriber("OrderUpdated", func(context.Context, models.Event[mocks.Order]) error {
				attempts.Add(1)
				return errors.New("poison")
			}))
		close(built)
	}()
	select {
	case <-built:
	case <-time.After(3 * time.Second):
		t.Fatal("Build hung on a poison delivery")
	}
	eventually(t, "the recovered delivery to be attempted", func() bool { return attempts.Load() >= 1 })
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 1 {
		t.Fatalf("pending = %v, want the poison delivery to stay pending", pending)
	}
}

func TestGuarantee_DurableFallbackIsRejectedAtBuild(t *testing.T) {
	_, err := eight(store.New()).
		WithSubscriber("OrderUpdated",
			func(context.Context, models.Event[mocks.Order]) error { return nil },
			models.WithFallback(func(context.Context, models.Event[mocks.Order]) {})).
		Build()
	if !errors.Is(err, models.ErrFallbackUnsupported) {
		t.Fatalf("Build err = %v, want ErrFallbackUnsupported", err)
	}
}

func TestGuarantee_DurablePanicIsAFailureNotADelivery(t *testing.T) {
	var attempts atomic.Int32
	var panics atomic.Int32
	s := store.New()
	inst := build(t, eight(s).
		WithDeliveryRetry(time.Millisecond, 2*time.Millisecond).
		WithPanicHandler(func(context.Context, models.Event[mocks.Order], any) { panics.Add(1) }).
		WithSubscriber("OrderUpdated", func(context.Context, models.Event[mocks.Order]) error {
			if attempts.Add(1) == 1 {
				panic("boom")
			}
			return nil
		}, models.WithHandlerTimeout[mocks.Order](time.Second)))

	if _, err := inst.Send(context.Background(), upd("agg", 1)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the delivery to complete after the panic", func() bool {
		p, _ := s.PendingDeliveries(context.Background(), 1)
		return attempts.Load() >= 2 && len(p) == 0
	})
	if panics.Load() != 1 {
		t.Fatalf("panic handler calls = %d, want 1", panics.Load())
	}
}

// eventually polls cond until it holds, failing after a generous deadline. It is
// for state with no event to wait on (store contents, goroutine counts).
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

type gatedUpdate struct {
	mocks.UpdateOrderCmd
	entered chan<- struct{}
	gate    <-chan struct{}
}

func (c gatedUpdate) Validate(*mocks.Order) error {
	c.entered <- struct{}{}
	<-c.gate
	return nil
}

// A busy aggregate holds one worker, however many commands are queued behind
// it: the others keep running, as WorkersPerShard promises.
func TestShardingOpts_ABusyAggregateDoesNotStallItsShard(t *testing.T) {
	ctx := context.Background()
	inst := build(t, asynx.New[mocks.Order]().WithEventStore(store.New()).WithSnapshotStore(store.NewSnapshots()).
		WithShardingOpts(asynx.ShardingOpts{Shards: 1, WorkersPerShard: 2, QueueDepth: 1000}))
	if _, err := inst.SendWait(ctx, mocks.CreateOrderCmd{ID: "hot", Total: 1}); err != nil {
		t.Fatal(err)
	}

	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	defer close(gate)
	go inst.Send(ctx, gatedUpdate{UpdateOrderCmd: upd("hot", 2), entered: entered, gate: gate})
	within(t, entered, "the hot command to start")
	var flooding sync.WaitGroup
	for i := range 20 {
		flooding.Add(1)
		go func() {
			flooding.Done()
			inst.Send(ctx, upd("hot", float64(i+3)))
		}()
	}
	flooding.Wait()

	for i := range 50 {
		cold, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err := inst.SendWait(cold, mocks.CreateOrderCmd{ID: fmt.Sprintf("cold-%d", i), Total: 1})
		cancel()
		if err != nil {
			t.Fatalf("an unrelated aggregate on the same shard, probe %d: %v", i, err)
		}
	}
}

func sendHot(
	ctx context.Context,
	inst asynx.Asynx[mocks.Order],
	refused chan<- struct{},
) {
	_, err := inst.Send(ctx, upd("hot", 3))
	if errors.Is(err, models.ErrQueueFull) {
		refused <- struct{}{}
	}
}

// QueueDepth bounds the commands that may wait behind a running one, per
// aggregate: a flood to one aggregate is refused with ErrQueueFull, and other
// aggregates are unaffected.
func TestShardingOpts_QueueDepthBoundsTheBacklogOfOneAggregate(t *testing.T) {
	ctx := context.Background()
	inst := build(t, asynx.New[mocks.Order]().WithEventStore(store.New()).WithSnapshotStore(store.NewSnapshots()).
		WithShardingOpts(asynx.ShardingOpts{Shards: 1, WorkersPerShard: 2}))
	if _, err := inst.SendWait(ctx, mocks.CreateOrderCmd{ID: "hot", Total: 1}); err != nil {
		t.Fatal(err)
	}

	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	defer close(gate)
	go inst.Send(ctx, gatedUpdate{UpdateOrderCmd: upd("hot", 2), entered: entered, gate: gate})
	within(t, entered, "the hot command to start")

	refused := make(chan struct{}, 32)
	for range 20 {
		go sendHot(ctx, inst, refused)
	}
	for range 18 {
		within(t, refused, "a hot Send to be refused with ErrQueueFull")
	}

	if _, err := inst.Send(ctx, mocks.CreateOrderCmd{ID: "cold", Total: 1}); err != nil {
		t.Fatalf("an unrelated aggregate got %v while another was flooded", err)
	}
}

func noopDurable(
	context.Context,
	models.Event[mocks.Order],
) error {
	return nil
}

func TestBuild_UsesTheSuppliedDispatcher(t *testing.T) {
	ctx := context.Background()
	s := store.New()
	fake := mocks.NewDispatcher[mocks.Order]()
	inst := build(t, eight(s).WithDispatcher(fake.Factory()).WithSubscriber("OrderCreated", noopDurable))

	if _, err := inst.Send(ctx, mocks.CreateOrderCmd{ID: "a", Total: 1}); err != nil {
		t.Fatal(err)
	}

	requests := fake.Requests()
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(requests))
	}
	request := requests[0]
	wrongRequest := request.Kind != models.DispatchEvent || request.Event.AggregateID != "a" || request.Event.Version != 1
	if wrongRequest {
		t.Errorf("request = %+v, want the DispatchEvent of a v1", request)
	}
	if request.Ack == nil {
		t.Fatal("a durable event was enqueued without an Ack")
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 1 {
		t.Fatalf("pending before the Ack = %v, want the entry", pending)
	}
	if err := request.Ack(ctx); err != nil {
		t.Fatal(err)
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 0 {
		t.Errorf("pending after the Ack = %v, want none", pending)
	}
}

func TestBuild_RecoverEnqueuesRedeliveries(t *testing.T) {
	ctx := context.Background()
	s := store.New()
	stuck := make(chan struct{})
	first := build(t, eight(s).WithSubscriber("OrderUpdated", func(
		context.Context,
		models.Event[mocks.Order],
	) error {
		<-stuck
		return nil
	}))
	t.Cleanup(func() { close(stuck) })
	for _, cmd := range []models.Command[mocks.Order]{mocks.CreateOrderCmd{ID: "a", Total: 1}, upd("a", 2), upd("a", 3)} {
		if _, err := first.Send(ctx, cmd); err != nil {
			t.Fatal(err)
		}
	}

	fake := mocks.NewDispatcher[mocks.Order]()
	build(t, eight(s).WithDispatcher(fake.Factory()).WithSubscriber("OrderUpdated", noopDurable))

	requests := fake.Requests()
	if len(requests) != 2 {
		t.Fatalf("requests = %d, want the 2 pending deliveries", len(requests))
	}
	for i, request := range requests {
		wrongRedelivery := request.Kind != models.DispatchRedelivery || request.Event.Version != int64(i+2) || request.Ack == nil
		if wrongRedelivery {
			t.Errorf("request %d = %+v, want the redelivery of v%d with an Ack", i, request, i+2)
		}
	}
}

func TestForget_SupersedesThenEnqueuesATombstone(t *testing.T) {
	ctx := context.Background()
	fake := mocks.NewDispatcher[mocks.Order]()
	inst := build(t, eight(store.New()).WithDispatcher(fake.Factory()))
	if _, err := inst.Send(ctx, mocks.CreateOrderCmd{ID: "a", Total: 1}); err != nil {
		t.Fatal(err)
	}

	if err := inst.Forget(ctx, "a"); err != nil {
		t.Fatal(err)
	}

	calls := fake.Calls()
	supersede := slices.Index(calls, "Supersede")
	if supersede < 0 || calls[supersede+1] != "Enqueue" {
		t.Fatalf("calls = %v, want Supersede immediately followed by Enqueue", calls)
	}
	requests := fake.Requests()
	tombstone := requests[len(requests)-1]
	wrongTombstone := tombstone.Kind != models.DispatchTombstone || tombstone.Event.AggregateID != "a" || tombstone.Ack == nil
	if wrongTombstone {
		t.Errorf("last request = %+v, want a tombstone of a with an Ack", tombstone)
	}
}

func TestBuild_ADispatcherFactoryErrorFailsBuild(t *testing.T) {
	sentinel := errors.New("no dispatcher today")
	factory := func(
		models.DispatcherConfig[mocks.Order],
	) (models.Dispatcher[mocks.Order], error) {
		return nil, sentinel
	}
	bus := &closingBus{}

	inst, err := eight(store.New()).WithBus(bus).WithDispatcher(factory).Build()

	if !errors.Is(err, sentinel) || inst != nil {
		t.Fatalf("Build = (%v, %v), want the factory error and no instance", inst, err)
	}
	if !strings.HasPrefix(err.Error(), "asynx: dispatcher factory: ") {
		t.Errorf("error = %q, want the %q prefix", err, "asynx: dispatcher factory: ")
	}
	if got := bus.closed.Load(); got != 1 {
		t.Errorf("bus closed %d times, want 1", got)
	}
}

func TestShutdown_InterruptsThenClosesTheDispatcher(t *testing.T) {
	fake := mocks.NewDispatcher[mocks.Order]()
	inst, err := eight(store.New()).WithDispatcher(fake.Factory()).Build()
	if err != nil {
		t.Fatal(err)
	}

	if err := inst.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	got := slices.DeleteFunc(fake.Calls(), func(call string) bool { return call != "Interrupt" && call != "Close" })
	if !slices.Equal(got, []string{"Interrupt", "Close"}) {
		t.Errorf("shutdown calls = %v, want Interrupt then Close", got)
	}
}

func TestSend_AnEnqueueFailureAfterCommitParksThroughTheSuppliedDispatcher(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("enqueue refused")
	fake := mocks.NewDispatcher[mocks.Order]()
	fake.FailEnqueue(sentinel)
	inst := build(t, eight(store.New()).WithDispatcher(fake.Factory()).WithSubscriber("OrderCreated", noopDurable))

	_, err := inst.Send(ctx, mocks.CreateOrderCmd{ID: "a", Total: 1})

	if !errors.Is(err, models.ErrPipelineFailed) || !errors.Is(err, sentinel) {
		t.Fatalf("Send = %v, want ErrPipelineFailed wrapping the enqueue failure", err)
	}
	if got := fake.ParkCalls(); len(got) != 1 || got[0].AggregateID != "a" {
		t.Errorf("Park calls = %v, want the one park of a", got)
	}
	if got := inst.PausedAggregates(); !slices.Equal(got, []string{"a"}) {
		t.Errorf("PausedAggregates = %v, want [a]", got)
	}
}

func TestBuild_DefaultsAreAppliedBeforeTheFactoryRuns(t *testing.T) {
	defaults := models.DispatcherConfig[mocks.Order]{MaxAttempts: 20, RetryInitial: 100 * time.Millisecond, RetryMax: 30 * time.Second}
	tests := []struct {
		name     string
		retry    [2]time.Duration
		attempts int
		want     models.DispatcherConfig[mocks.Order]
	}{
		{
			name:     "non-positive builder values are ignored",
			retry:    [2]time.Duration{-1, 0},
			attempts: -3,
			want:     defaults,
		},
		{
			name: "unset builder values keep the defaults",
			want: defaults,
		},
		{
			name:     "positive builder values are carried",
			retry:    [2]time.Duration{5 * time.Millisecond, 50 * time.Millisecond},
			attempts: 3,
			want:     models.DispatcherConfig[mocks.Order]{MaxAttempts: 3, RetryInitial: 5 * time.Millisecond, RetryMax: 50 * time.Millisecond},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := mocks.NewDispatcher[mocks.Order]()
			b := eight(store.New()).
				WithDispatcher(fake.Factory()).
				WithSubscriber("OrderUpdated", noopDurable).
				WithDeliveryRetry(tt.retry[0], tt.retry[1]).
				WithDeliveryMaxAttempts(tt.attempts)
			build(t, b)

			got := fake.Config()
			configDiffers := got.MaxAttempts != tt.want.MaxAttempts || got.RetryInitial != tt.want.RetryInitial || got.RetryMax != tt.want.RetryMax
			if configDiffers {
				t.Errorf("config = (%d, %v, %v), want (%d, %v, %v)", got.MaxAttempts, got.RetryInitial, got.RetryMax,
					tt.want.MaxAttempts, tt.want.RetryInitial, tt.want.RetryMax)
			}
			if got.Bus == nil || len(got.DurableSubs) != 1 {
				t.Errorf("config = %+v, want the bus and the one durable subscriber", got)
			}
		})
	}
}

func TestBuild_ATypedNilDispatcherFailsBuild(t *testing.T) {
	factory := func(
		models.DispatcherConfig[mocks.Order],
	) (models.Dispatcher[mocks.Order], error) {
		var d *mocks.Dispatcher[mocks.Order]
		return d, nil
	}

	inst, err := eight(store.New()).WithBus(&closingBus{}).WithDispatcher(factory).Build()

	notRejected := inst != nil || err == nil || !strings.Contains(err.Error(), "returned no dispatcher")
	if notRejected {
		t.Fatalf("Build = (%v, %v), want the no-dispatcher error and no instance", inst, err)
	}
}
