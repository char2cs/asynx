package asynx_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

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
	time.Sleep(200 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before+5 {
		t.Fatalf("goroutines grew from %d to %d across failed builds", before, after)
	}
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

	deadline := time.Now().Add(3 * time.Second)
	for {
		pending, _ := s.PendingDeliveries(ctx, 10)
		if len(pending) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("deliveries never completed: %v", pending)
		}
		time.Sleep(2 * time.Millisecond)
	}

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
	deadline := time.Now().Add(3 * time.Second)
	for attempts.Load() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("the recovered delivery was never attempted")
		}
		time.Sleep(time.Millisecond)
	}
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
	deadline := time.Now().Add(3 * time.Second)
	for attempts.Load() < 2 || func() bool { p, _ := s.PendingDeliveries(context.Background(), 1); return len(p) > 0 }() {
		if time.Now().After(deadline) {
			t.Fatalf("attempts = %d; delivery never completed after the panic", attempts.Load())
		}
		time.Sleep(2 * time.Millisecond)
	}
	if panics.Load() != 1 {
		t.Fatalf("panic handler calls = %d, want 1", panics.Load())
	}
}
