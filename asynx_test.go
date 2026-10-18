package asynx_test

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

	"github.com/char2cs/asynx"
	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

// publishWaiter is satisfied by *asynxImpl[T] via its WaitPublish method.
type publishWaiter interface {
	WaitPublish()
}

func waitPublish(t *testing.T, instance asynx.Asynx[mocks.Order]) {
	t.Helper()
	instance.(publishWaiter).WaitPublish()
}

func newInstance(t *testing.T) asynx.Asynx[mocks.Order] {
	s := store.New()
	instance, err := asynx.New[mocks.Order]().
		WithEventStore(s).
		WithSnapshotStore(store.NewSnapshots()).
		WithShardingOpts(asynx.ShardingOpts{Shards: 8, QueueDepth: 1000}).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { instance.Shutdown(context.Background()) })
	return instance
}

// Unit tests

func TestSend_Success(t *testing.T) {
	instance := newInstance(t)
	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	_, err := instance.Send(context.Background(), cmd)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}
}

func TestSend_ValidationFails(t *testing.T) {
	instance := newInstance(t)
	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: -5.0} // Invalid total
	_, err := instance.Send(context.Background(), cmd)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if err != models.ErrValidation {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestSend_ContextCancelled(t *testing.T) {
	instance := newInstance(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	_, err := instance.Send(ctx, cmd)
	if err == nil {
		t.Fatal("expected context cancelled error, got nil")
	}
	if err != models.ErrContextCancelled {
		t.Fatalf("expected ErrContextCancelled, got %v", err)
	}
}

func TestSend_AfterShutdown(t *testing.T) {
	instance := newInstance(t)
	instance.Shutdown(context.Background())
	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	_, err := instance.Send(context.Background(), cmd)
	if err == nil {
		t.Fatal("expected shutdown error, got nil")
	}
	if err != models.ErrShuttingDown {
		t.Fatalf("expected ErrShuttingDown, got %v", err)
	}
}

func TestGet_NotFound(t *testing.T) {
	instance := newInstance(t)
	_, err := instance.Get(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected ErrNotFound, got nil")
	}
	if err != models.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestGet_Success(t *testing.T) {
	instance := newInstance(t)
	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	instance.Send(context.Background(), cmd)

	state, err := instance.Get(context.Background(), "order-1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if state.ID != "order-1" {
		t.Fatalf("expected order-1, got %s", state.ID)
	}
	if state.Total != 100.0 {
		t.Fatalf("expected total 100.0, got %f", state.Total)
	}
}

func TestExists_False(t *testing.T) {
	instance := newInstance(t)
	exists, err := instance.Exists(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Fatal("expected false, got true")
	}
}

func TestExists_True(t *testing.T) {
	instance := newInstance(t)
	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	instance.Send(context.Background(), cmd)

	exists, err := instance.Exists(context.Background(), "order-1")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Fatal("expected true, got false")
	}
}

func TestPreload_NotFoundIsNoop(t *testing.T) {
	instance := newInstance(t)
	err := instance.Preload(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("Preload failed: %v", err)
	}
}

func TestPreload_ExistingAggregate(t *testing.T) {
	instance := newInstance(t)
	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	instance.Send(context.Background(), cmd)

	err := instance.Preload(context.Background(), "order-1")
	if err != nil {
		t.Fatalf("Preload failed: %v", err)
	}
}

func TestSubscribe_EmptyPattern(t *testing.T) {
	instance := newInstance(t)
	handler := func(ctx context.Context, evt models.Event[mocks.Order]) {
	}
	_, err := instance.Subscribe("", handler)
	if err == nil {
		t.Fatal("expected ErrEmptyPattern, got nil")
	}
	if err != models.ErrEmptyPattern {
		t.Fatalf("expected ErrEmptyPattern, got %v", err)
	}
}

func TestSubscribe_NilHandler(t *testing.T) {
	instance := newInstance(t)
	var nilHandler models.ProjectionHandler[mocks.Order]
	_, err := instance.Subscribe("OrderCreated", nilHandler)
	if err == nil {
		t.Fatal("expected ErrNilHandler, got nil")
	}
	if err != models.ErrNilHandler {
		t.Fatalf("expected ErrNilHandler, got %v", err)
	}
}

func TestSubscribe_Success(t *testing.T) {
	instance := newInstance(t)
	handler := func(ctx context.Context, evt models.Event[mocks.Order]) {
	}
	id, err := instance.Subscribe("OrderCreated", handler)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty subscription ID")
	}
}

func TestUnsubscribe_Success(t *testing.T) {
	instance := newInstance(t)
	handler := func(ctx context.Context, evt models.Event[mocks.Order]) {
	}
	id, err := instance.Subscribe("OrderCreated", handler)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	err = instance.Unsubscribe(id)
	if err != nil {
		t.Fatalf("Unsubscribe failed: %v", err)
	}
}

func TestUnsubscribe_UnknownID(t *testing.T) {
	instance := newInstance(t)
	err := instance.Unsubscribe("unknown-id")
	if err != nil {
		t.Fatalf("expected nil for unknown subscription, got %v", err)
	}
}

func TestReplay_Success(t *testing.T) {
	instance := newInstance(t)
	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	instance.Send(context.Background(), cmd)

	count := 0
	fn := func(ctx context.Context, evt models.Event[mocks.Order]) {
		count++
	}
	err := instance.Replay(context.Background(), "order-1", 1, 0, fn)
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}
	if count == 0 {
		t.Fatal("expected at least one event, got 0")
	}
}

func TestReplay_EmptyRange(t *testing.T) {
	instance := newInstance(t)
	count := 0
	fn := func(ctx context.Context, evt models.Event[mocks.Order]) {
		count++
	}
	err := instance.Replay(context.Background(), "nonexistent", 1, 0, fn)
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 events for nonexistent aggregate, got %d", count)
	}
}

func TestShutdown_Success(t *testing.T) {
	instance := newInstance(t)
	err := instance.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}
}

func TestShutdown_Twice(t *testing.T) {
	instance := newInstance(t)
	instance.Shutdown(context.Background())
	err := instance.Shutdown(context.Background())
	if err == nil {
		t.Fatal("expected ErrAlreadyShuttingDown, got nil")
	}
	if err != models.ErrAlreadyShuttingDown {
		t.Fatalf("expected ErrAlreadyShuttingDown, got %v", err)
	}
}

func TestShardingOpts_CustomShards(t *testing.T) {
	s := store.New()
	instance, err := asynx.New[mocks.Order]().
		WithEventStore(s).
		WithSnapshotStore(store.NewSnapshots()).
		WithShardingOpts(asynx.ShardingOpts{Shards: 16, QueueDepth: 100}).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Shutdown(context.Background())

	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	_, err = instance.Send(context.Background(), cmd)
	if err != nil {
		t.Fatalf("Send with custom shards failed: %v", err)
	}
}

// Integration tests

func TestIntegration_FullCommandCycle(t *testing.T) {
	instance := newInstance(t)

	handlerCalled := atomic.Int32{}
	handler := func(ctx context.Context, evt models.Event[mocks.Order]) {
		handlerCalled.Add(1)
	}

	subID, err := instance.Subscribe("OrderCreated", handler)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer instance.Unsubscribe(subID)

	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	_, err = instance.Send(context.Background(), cmd)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Wait for async publication to complete
	waitPublish(t, instance)

	state, err := instance.Get(context.Background(), "order-1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if state.Total != 100.0 {
		t.Fatalf("expected total 100.0, got %f", state.Total)
	}

	if handlerCalled.Load() == 0 {
		t.Fatal("handler was not called")
	}
}

func TestIntegration_SerialOrderingForSameAggregate(t *testing.T) {
	instance := newInstance(t)

	orderID := "order-1"
	cmd1 := mocks.CreateOrderCmd{ID: orderID, Total: 100.0}
	instance.Send(context.Background(), cmd1)

	for i := 0; i < 5; i++ {
		newTotal := float64((i + 2) * 100.0)
		cmd := mocks.UpdateOrderCmd{
			ID:       orderID,
			NewState: mocks.Order{ID: orderID, Total: newTotal, Status: "Pending"},
		}
		_, err := instance.Send(context.Background(), cmd)
		if err != nil {
			t.Fatalf("Send update %d failed: %v", i, err)
		}
	}

	state, err := instance.Get(context.Background(), orderID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	expected := 600.0
	if state.Total != expected {
		t.Fatalf("expected final total %f, got %f", expected, state.Total)
	}
}

func TestIntegration_ConcurrentCommandsDifferentAggregates(t *testing.T) {
	instance := newInstance(t)

	numGoroutines := 10
	var wg sync.WaitGroup
	errChan := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			orderID := fmt.Sprintf("order-%d", idx)
			cmd := mocks.CreateOrderCmd{ID: orderID, Total: float64((idx + 1) * 100.0)}
			_, err := instance.Send(context.Background(), cmd)
			errChan <- err
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			t.Fatalf("Send failed: %v", err)
		}
	}

	// Verify all aggregates were created
	for i := 0; i < numGoroutines; i++ {
		orderID := fmt.Sprintf("order-%d", i)
		exists, err := instance.Exists(context.Background(), orderID)
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if !exists {
			t.Fatalf("aggregate %s not found", orderID)
		}
	}
}

func TestIntegration_MultipleProjections(t *testing.T) {
	instance := newInstance(t)

	counters := [4]atomic.Int32{}
	handlers := []func(context.Context, models.Event[mocks.Order]){
		func(ctx context.Context, evt models.Event[mocks.Order]) {
			counters[0].Add(1)
		},
		func(ctx context.Context, evt models.Event[mocks.Order]) {
			counters[1].Add(1)
		},
		func(ctx context.Context, evt models.Event[mocks.Order]) {
			counters[2].Add(1)
		},
		func(ctx context.Context, evt models.Event[mocks.Order]) {
			counters[3].Add(1)
		},
	}

	ids := make([]string, 4)
	for i, h := range handlers {
		id, err := instance.Subscribe("OrderCreated", h)
		if err != nil {
			t.Fatalf("Subscribe %d failed: %v", i, err)
		}
		ids[i] = id
	}
	defer func() {
		for _, id := range ids {
			instance.Unsubscribe(id)
		}
	}()

	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	_, err := instance.Send(context.Background(), cmd)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	waitPublish(t, instance)

	for i := 0; i < len(counters); i++ {
		if counters[i].Load() == 0 {
			t.Fatalf("handler %d was not called", i)
		}
	}
}

func TestIntegration_GracefulShutdown(t *testing.T) {
	instance := newInstance(t)

	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	_, err := instance.Send(context.Background(), cmd)
	if err != nil {
		t.Fatalf("Send before shutdown failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = instance.Shutdown(ctx)
	if err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}

	cmd2 := mocks.CreateOrderCmd{ID: "order-2", Total: 200.0}
	_, err = instance.Send(context.Background(), cmd2)
	if err == nil {
		t.Fatal("expected error after shutdown, got nil")
	}
	if err != models.ErrShuttingDown {
		t.Fatalf("expected ErrShuttingDown, got %v", err)
	}
}

func TestSendWait_Success(t *testing.T) {
	instance := newInstance(t)
	cmd := mocks.CreateOrderCmd{ID: "ew-order", Total: 55.0}

	event, err := instance.SendWait(context.Background(), cmd)
	if err != nil {
		t.Fatalf("SendWait failed: %v", err)
	}
	if event.AggregateID != "ew-order" {
		t.Errorf("expected AggregateID ew-order, got %q", event.AggregateID)
	}
	if event.EventName == "" {
		t.Error("expected non-empty EventName")
	}
	if event.Version == 0 {
		t.Error("expected non-zero Version")
	}
}

func TestSendWait_ReturnsNewAndPreviousAggregate(t *testing.T) {
	instance := newInstance(t)

	// Create first
	cmd1 := mocks.CreateOrderCmd{ID: "ew-agg", Total: 100.0}
	_, err := instance.SendWait(context.Background(), cmd1)
	if err != nil {
		t.Fatalf("first SendWait failed: %v", err)
	}

	// Update — event should carry both old and new aggregate
	cmd2 := mocks.UpdateOrderCmd{
		ID:       "ew-agg",
		NewState: mocks.Order{ID: "ew-agg", Total: 200.0, Status: "Updated"},
	}
	event, err := instance.SendWait(context.Background(), cmd2)
	if err != nil {
		t.Fatalf("second SendWait failed: %v", err)
	}

	if event.Aggregate.Total != 200.0 {
		t.Errorf("expected new aggregate Total 200.0, got %f", event.Aggregate.Total)
	}
	if event.PreviousAggregate.Total != 100.0 {
		t.Errorf("expected previous aggregate Total 100.0, got %f", event.PreviousAggregate.Total)
	}
}

func TestSendWait_HandlersCompleteBeforeReturn(t *testing.T) {
	instance := newInstance(t)

	var handlerDone atomic.Int32
	instance.Subscribe("OrderCreated", func(_ context.Context, _ models.Event[mocks.Order]) {
		handlerDone.Add(1)
	})

	cmd := mocks.CreateOrderCmd{ID: "ew-handler", Total: 10.0}
	_, err := instance.SendWait(context.Background(), cmd)
	if err != nil {
		t.Fatalf("SendWait failed: %v", err)
	}

	// No waitPublish needed — handlers must be done when SendWait returns.
	if handlerDone.Load() == 0 {
		t.Error("handler not complete when SendWait returned")
	}
}

func TestSendWait_MultipleHandlersAllComplete(t *testing.T) {
	instance := newInstance(t)

	var count atomic.Int32
	for range 3 {
		instance.Subscribe("OrderCreated", func(_ context.Context, _ models.Event[mocks.Order]) {
			count.Add(1)
		})
	}

	cmd := mocks.CreateOrderCmd{ID: "ew-multi", Total: 10.0}
	_, err := instance.SendWait(context.Background(), cmd)
	if err != nil {
		t.Fatalf("SendWait failed: %v", err)
	}

	if count.Load() != 3 {
		t.Errorf("expected 3 handler calls, got %d", count.Load())
	}
}

func TestSendWait_ValidationFails(t *testing.T) {
	instance := newInstance(t)
	cmd := mocks.CreateOrderCmd{ID: "ew-bad", Total: -1.0}

	_, err := instance.SendWait(context.Background(), cmd)
	if err != models.ErrValidation {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestSendWait_ContextCancelled(t *testing.T) {
	instance := newInstance(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cmd := mocks.CreateOrderCmd{ID: "ew-ctx", Total: 10.0}
	_, err := instance.SendWait(ctx, cmd)
	if err != models.ErrContextCancelled {
		t.Fatalf("expected ErrContextCancelled, got %v", err)
	}
}

func TestSendWait_AfterShutdown(t *testing.T) {
	instance := newInstance(t)
	instance.Shutdown(context.Background())

	cmd := mocks.CreateOrderCmd{ID: "ew-shutdown", Total: 10.0}
	_, err := instance.SendWait(context.Background(), cmd)
	if err != models.ErrShuttingDown {
		t.Fatalf("expected ErrShuttingDown, got %v", err)
	}
}

func TestIntegration_SendWaitThenGet(t *testing.T) {
	instance := newInstance(t)

	cmd := mocks.CreateOrderCmd{ID: "ew-get", Total: 77.0}
	event, err := instance.SendWait(context.Background(), cmd)
	if err != nil {
		t.Fatalf("SendWait failed: %v", err)
	}

	// State is immediately consistent — no WaitPublish needed.
	state, err := instance.Get(context.Background(), "ew-get")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if state.Total != 77.0 {
		t.Errorf("expected Total 77.0, got %f", state.Total)
	}
	if event.Aggregate.Total != state.Total {
		t.Errorf("event aggregate (%f) does not match store (%f)", event.Aggregate.Total, state.Total)
	}
}

// --- Forget ---

func TestForget_AggregateDoesNotExist_ReturnsErrValidation(t *testing.T) {
	instance := newInstance(t)
	err := instance.Forget(context.Background(), "order-missing")
	if !errors.Is(err, models.ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestForget_ErasesAggregate(t *testing.T) {
	instance := newInstance(t)
	ctx := context.Background()

	if _, err := instance.Send(ctx, mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if err := instance.Forget(ctx, "order-1"); err != nil {
		t.Fatalf("Forget: %v", err)
	}

	_, err := instance.Get(ctx, "order-1")
	if !errors.Is(err, models.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after Forget, got %v", err)
	}
}

func TestForget_CallsOnForgetHandler_WithLastState(t *testing.T) {
	instance := newInstance(t)
	ctx := context.Background()

	if _, err := instance.Send(ctx, mocks.CreateOrderCmd{ID: "order-1", Total: 99.0}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	var gotEvent models.Event[mocks.Order]
	if _, err := instance.OnForget(func(_ context.Context, e models.Event[mocks.Order]) {
		gotEvent = e
	}); err != nil {
		t.Fatalf("OnForget: %v", err)
	}

	if err := instance.Forget(ctx, "order-1"); err != nil {
		t.Fatalf("Forget: %v", err)
	}

	if gotEvent.AggregateID != "order-1" {
		t.Errorf("AggregateID = %q, want order-1", gotEvent.AggregateID)
	}
	if gotEvent.EventName != "asynx.aggregate.forget" {
		t.Errorf("EventName = %q, want asynx.aggregate.forget", gotEvent.EventName)
	}
	if gotEvent.Aggregate.Total != 99.0 {
		t.Errorf("Aggregate.Total = %v, want 99.0", gotEvent.Aggregate.Total)
	}
}

func TestForget_AfterShutdown_ReturnsErrShuttingDown(t *testing.T) {
	s := store.New()
	instance, err := asynx.New[mocks.Order]().WithEventStore(s).WithSnapshotStore(store.NewSnapshots()).Build()
	if err != nil {
		t.Fatal(err)
	}
	instance.Shutdown(context.Background())

	if err := instance.Forget(context.Background(), "order-1"); !errors.Is(err, models.ErrShuttingDown) {
		t.Fatalf("expected ErrShuttingDown, got %v", err)
	}
}

func TestForget_SerializedAfterSend(t *testing.T) {
	instance := newInstance(t)
	ctx := context.Background()

	if _, err := instance.Send(ctx, mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}); err != nil {
		t.Fatalf("initial Send: %v", err)
	}

	// Enqueue an update and a Forget — Forget must see the updated state.
	// The Send here is best-effort; it relies on QueueDepth: 1000 in newInstance
	// to avoid ErrQueueFull. If the update were lost, gotTotal would be 100.0, not 200.0.
	updated := mocks.Order{ID: "order-1", Total: 200.0, Status: "Updated"}
	instance.Send(ctx, mocks.UpdateOrderCmd{ID: "order-1", NewState: updated}) //nolint:errcheck

	var gotTotal float64
	instance.OnForget(func(_ context.Context, e models.Event[mocks.Order]) { //nolint:errcheck
		gotTotal = e.Aggregate.Total
	})

	if err := instance.Forget(ctx, "order-1"); err != nil {
		t.Fatalf("Forget: %v", err)
	}

	if gotTotal != 200.0 {
		t.Errorf("Forget saw Total=%v, want 200.0 (Send should have completed first)", gotTotal)
	}
}

// deleteErrStore wraps store.Memory and injects a one-shot error on the next
// DeleteStream call made inside a transaction.
type deleteErrStore struct {
	store.Memory
	deleteErr error
}

func (s *deleteErrStore) Atomically(ctx context.Context, fn func(models.Tx) error) error {
	return s.Memory.Atomically(ctx, func(tx models.Tx) error {
		return fn(&deleteErrTx{Tx: tx, owner: s})
	})
}

type deleteErrTx struct {
	models.Tx
	owner *deleteErrStore
}

func (t *deleteErrTx) DeleteStream(ctx context.Context, streamID string) error {
	if t.owner.deleteErr != nil {
		err := t.owner.deleteErr
		t.owner.deleteErr = nil
		return err
	}
	return t.Tx.DeleteStream(ctx, streamID)
}

func TestForget_DeleteFails_ReturnsErrForgetFailed(t *testing.T) {
	sentinel := errors.New("store unavailable")
	backing := &deleteErrStore{Memory: store.New(), deleteErr: sentinel}

	instance, err := asynx.New[mocks.Order]().
		WithEventStore(backing).
		WithSnapshotStore(store.NewSnapshots()).
		WithShardingOpts(asynx.ShardingOpts{Shards: 8, QueueDepth: 1000}).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { instance.Shutdown(context.Background()) })

	ctx := context.Background()
	if _, err := instance.Send(ctx, mocks.CreateOrderCmd{ID: "order-1", Total: 10.0}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	err = instance.Forget(ctx, "order-1")
	if !errors.Is(err, models.ErrForgetFailed) {
		t.Fatalf("expected ErrForgetFailed, got %v", err)
	}
}

func TestForget_ConcurrentForget_SecondReturnsErrValidation(t *testing.T) {
	instance := newInstance(t)
	ctx := context.Background()

	if _, err := instance.Send(ctx, mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	var (
		wg   sync.WaitGroup
		errs [2]error
	)
	for i := range 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = instance.Forget(ctx, "order-1")
		}(i)
	}
	wg.Wait()

	// At least one call must fail: ErrValidation (aggregate already gone),
	// ErrForgetInProgress (the other Forget is still running) or
	// ErrPipelineFailed (concurrent write conflict). Both callers succeeding
	// would mean two tombstone events were written without a conflict, which is
	// also safe (aggregate is still erased), but we verify the aggregate is gone.
	successCount := 0
	for _, err := range errs {
		if err == nil {
			successCount++
		} else if !errors.Is(err, models.ErrValidation) && !errors.Is(err, models.ErrPipelineFailed) &&
			!errors.Is(err, models.ErrForgetInProgress) {
			t.Errorf("unexpected error from concurrent Forget: %v", err)
		}
	}

	// Regardless of success/failure split, the aggregate must be erased.
	_, getErr := instance.Get(ctx, "order-1")
	if !errors.Is(getErr, models.ErrNotFound) {
		t.Errorf("expected aggregate to be erased after concurrent Forget, got Get err: %v", getErr)
	}
}

func TestOnForget_Unsubscribe_StopsHandler(t *testing.T) {
	instance := newInstance(t)
	ctx := context.Background()

	if _, err := instance.Send(ctx, mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	var called int
	subID, err := instance.OnForget(func(_ context.Context, _ models.Event[mocks.Order]) {
		called++
	})
	if err != nil {
		t.Fatalf("OnForget: %v", err)
	}

	if err := instance.Unsubscribe(subID); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}

	if err := instance.Forget(ctx, "order-1"); err != nil {
		t.Fatalf("Forget: %v", err)
	}

	if called != 0 {
		t.Errorf("handler called %d times after Unsubscribe, want 0", called)
	}
}

// --- Listen ---

func TestListen_EmptyPattern(t *testing.T) {
	instance := newInstance(t)
	_, _, err := instance.Listen("", 1)
	if err != models.ErrEmptyPattern {
		t.Fatalf("expected ErrEmptyPattern, got %v", err)
	}
}

func TestListen_EventArrivesBeforeDeadline(t *testing.T) {
	instance := newInstance(t)

	ch, unsub, err := instance.Listen("OrderCreated", 1)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer unsub()

	cmd := mocks.CreateOrderCmd{ID: "listen-1", Total: 50.0}
	if _, err := instance.SendWait(context.Background(), cmd); err != nil {
		t.Fatalf("SendWait: %v", err)
	}

	select {
	case evt := <-ch:
		if evt.AggregateID != "listen-1" {
			t.Fatalf("expected AggregateID listen-1, got %q", evt.AggregateID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestListen_UnsubscribeCleansUp(t *testing.T) {
	instance := newInstance(t)

	ch, unsub, err := instance.Listen("OrderCreated", 0)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	unsub()

	cmd := mocks.CreateOrderCmd{ID: "listen-unsub", Total: 50.0}
	if _, err := instance.SendWait(context.Background(), cmd); err != nil {
		t.Fatalf("SendWait: %v", err)
	}

	select {
	case evt, ok := <-ch:
		if ok {
			t.Fatalf("expected no event after unsub, got %v", evt)
		}
	case <-time.After(200 * time.Millisecond):
	}
}

func TestListen_UnsubIsIdempotent(t *testing.T) {
	instance := newInstance(t)

	_, unsub, err := instance.Listen("OrderCreated", 0)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	unsub()
	unsub() // should not panic, should not error
}

func TestListen_CountAutoCloses(t *testing.T) {
	instance := newInstance(t)

	ch, unsub, err := instance.Listen("OrderCreated", 1)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer unsub()

	cmd := mocks.CreateOrderCmd{ID: "listen-auto", Total: 50.0}
	if _, err := instance.SendWait(context.Background(), cmd); err != nil {
		t.Fatalf("SendWait: %v", err)
	}

	select {
	case _, ok := <-ch:
		if !ok {
			t.Fatal("channel closed before delivering event")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to be closed after count events")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for channel close")
	}
}

// Regression: unsub() called by the consumer the moment it receives the last
// event must not double-close the channel while the handler is still inside
// its critical section (between the send and its own close).
func TestListen_UnsubRightAfterLastEventDoesNotDoubleClose(t *testing.T) {
	instance := newInstance(t)

	const workers, iterations = 8, 250
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for n := 0; n < iterations; n++ {
				ch, unsub, err := instance.Listen("OrderCreated", 1)
				if err != nil {
					t.Errorf("Listen: %v", err)
					return
				}
				id := fmt.Sprintf("listen-race-%d-%d", w, n)
				if _, err := instance.Send(context.Background(), mocks.CreateOrderCmd{ID: id, Total: 10}); err != nil {
					t.Errorf("Send: %v", err)
					unsub()
					return
				}
				select {
				case <-ch:
					unsub() // consumer returns immediately, like SubscribeWait's defer
				case <-time.After(5 * time.Second):
					t.Error("timed out waiting for event")
					unsub()
					return
				}
			}
		}(w)
	}
	wg.Wait()
}

func TestListen_BufferedChannelPreservesEvent(t *testing.T) {
	instance := newInstance(t)

	ch, unsub, err := instance.Listen("OrderCreated", 1)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer unsub()

	cmd := mocks.CreateOrderCmd{ID: "listen-buf", Total: 75.0}
	if _, err := instance.SendWait(context.Background(), cmd); err != nil {
		t.Fatalf("SendWait: %v", err)
	}

	select {
	case evt := <-ch:
		if evt.Aggregate.Total != 75.0 {
			t.Fatalf("expected Total 75.0, got %f", evt.Aggregate.Total)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("event was lost — not in buffer")
	}
}

func TestListen_Unbounded_ReceivesMultipleEvents(t *testing.T) {
	instance := newInstance(t)

	ch, unsub, err := instance.Listen("OrderCreated", 0)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer unsub()

	for i := range 3 {
		cmd := mocks.CreateOrderCmd{ID: fmt.Sprintf("listen-ub-%d", i), Total: float64(i + 1)}
		if _, err := instance.SendWait(context.Background(), cmd); err != nil {
			t.Fatalf("SendWait: %v", err)
		}
	}

	for i := range 3 {
		select {
		case evt := <-ch:
			expected := fmt.Sprintf("listen-ub-%d", i)
			if evt.AggregateID != expected {
				t.Fatalf("expected %s, got %s", expected, evt.AggregateID)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for event %d", i)
		}
	}
}

func TestListen_UsesTopicPattern(t *testing.T) {
	// "Order.*" uses Topic()-style dot-segment wildcards.
	// Topic("Order.*") → ^Order\..*$ which matches "OrderCreated" (no leading dot needed).
	// A bare Subscribe("Order.*") would use it as a regex directly (matching the same),
	// but what matters is that Listen internally calls Topic() to translate the pattern.
	// We use "*" here: Topic("*") → ^.*$ which matches any event name — verifying
	// that Listen uses Topic() because "*" alone is not valid regex (it would panic).
	instance := newInstance(t)

	ch, unsub, err := instance.Listen("*", 1)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer unsub()

	cmd := mocks.CreateOrderCmd{ID: "listen-topic", Total: 10.0}
	if _, err := instance.SendWait(context.Background(), cmd); err != nil {
		t.Fatalf("SendWait: %v", err)
	}

	select {
	case evt := <-ch:
		if evt.AggregateID != "listen-topic" {
			t.Fatalf("expected listen-topic, got %q", evt.AggregateID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("topic pattern did not match — Listen should use Topic() internally")
	}
}

func TestListen_CountOverflow_DoesNotBlock(t *testing.T) {
	// When count=1 and multiple events are published, only the first is sent.
	// The second event must not block the publisher (handler returns early).
	instance := newInstance(t)

	ch, unsub, err := instance.Listen("OrderCreated", 1)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer unsub()

	// First event — fills the channel and triggers auto-close.
	if _, err := instance.SendWait(context.Background(), mocks.CreateOrderCmd{ID: "ovf-1", Total: 1.0}); err != nil {
		t.Fatalf("SendWait 1: %v", err)
	}
	// Second event — handler must not push to closed channel.
	if _, err := instance.SendWait(context.Background(), mocks.CreateOrderCmd{ID: "ovf-2", Total: 2.0}); err != nil {
		t.Fatalf("SendWait 2: %v", err)
	}

	// Drain
	<-ch
	if _, ok := <-ch; ok {
		t.Fatal("expected channel closed")
	}
}

func TestListen_CountOverflowConcurrent_GuardsEnforced(t *testing.T) {
	// Exercises the concurrent-overflow branches: count=1 with many concurrent
	// publishers. Because handlers for different aggregates run in separate
	// goroutines (the bus spawns one goroutine per subscription per event), two
	// handlers can overlap: both load received=0 (< count=1) and pass the first
	// guard, but only n=1 is ≤ count; goroutines where n>1 hit the second guard.
	// Repeating across iterations makes it highly likely to hit both guards.
	for iter := range 10 {
		func() {
			instance := newInstance(t)

			const count = 1
			ch, unsub, err := instance.Listen("OrderCreated", count)
			if err != nil {
				t.Fatalf("iter %d: Listen: %v", iter, err)
			}
			defer unsub()

			// Fire many events concurrently before auto-unsubscribe can remove the sub.
			const total = 16
			var wg sync.WaitGroup
			for i := range total {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					instance.Send(context.Background(), mocks.CreateOrderCmd{ //nolint:errcheck
						ID:    fmt.Sprintf("ovfc-%d-%d", iter, idx),
						Total: float64(idx + 1),
					})
				}(i)
			}
			wg.Wait()
			waitPublish(t, instance)

			// Channel must yield exactly 1 event, then close.
			got := 0
			deadline := time.After(2 * time.Second)
			for {
				select {
				case _, ok := <-ch:
					if !ok {
						if got != count {
							t.Fatalf("iter %d: expected %d events before close, got %d", iter, count, got)
						}
						return
					}
					got++
				case <-deadline:
					t.Fatalf("iter %d: timed out: got %d events, channel not closed", iter, got)
				}
			}
		}()
	}
}

func TestListen_SubscribeError_ReturnsError(t *testing.T) {
	// If the bus is closed (after Shutdown), Subscribe returns ErrBusClosed.
	// Listen must propagate that error and return nil channel.
	s := store.New()
	instance, err := asynx.New[mocks.Order]().WithEventStore(s).WithSnapshotStore(store.NewSnapshots()).Build()
	if err != nil {
		t.Fatal(err)
	}
	instance.Shutdown(context.Background())

	ch, unsub, err := instance.Listen("OrderCreated", 1)
	if err == nil {
		t.Fatal("expected error when bus is closed, got nil")
	}
	if ch != nil {
		t.Fatalf("expected nil channel on error, got non-nil")
	}
	if unsub != nil {
		t.Fatalf("expected nil unsub on error, got non-nil")
	}
}

func TestListen_NegativeCount_TreatedAsZero(t *testing.T) {
	// Document: negative count behaves like 0 (unbounded)
	instance := newInstance(t)
	ch, unsub, err := instance.Listen("OrderCreated", -1)
	if err != nil {
		t.Fatalf("expected nil error for negative count, got %v", err)
	}
	defer unsub()

	// Channel should not be auto-closing — send 2 events, both must arrive.
	for i := range 2 {
		cmd := mocks.CreateOrderCmd{ID: fmt.Sprintf("neg-%d", i), Total: float64(i + 1)}
		if _, err := instance.SendWait(context.Background(), cmd); err != nil {
			t.Fatalf("SendWait: %v", err)
		}
	}
	for i := range 2 {
		select {
		case evt := <-ch:
			expected := fmt.Sprintf("neg-%d", i)
			if evt.AggregateID != expected {
				t.Fatalf("expected %s, got %s", expected, evt.AggregateID)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for event %d", i)
		}
	}
}

// TestListen_BoundedRace_NOverCount exercises the n > count early-return (branch #1):
// with count=1 and 50 concurrent senders the auto-unsubscribe goroutine races with
// further dispatches, so some handlers observe n=2,3,... and return early.
// The test also covers branch #2 (closed.Load() after sendMu.Lock()) by using count=5
// so that multiple handlers can pass received.Add before any of them takes the lock;
// the last one to acquire the lock after the count-th handler has set closed=true
// returns through that guard.
func TestListen_BoundedRace_NOverCount(t *testing.T) {
	for iter := range 10 {
		func() {
			instance := newInstance(t)

			const count = 1
			ch, unsub, err := instance.Listen("OrderCreated", count)
			if err != nil {
				t.Fatalf("iter %d: Listen: %v", iter, err)
			}
			defer unsub()

			// Drain in background so the buffered channel never blocks a handler.
			go func() {
				for range ch {
				}
			}()

			// Burst many concurrent events — handlers race around received.Add(1).
			var wg sync.WaitGroup
			for i := range 100 {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					instance.Send(context.Background(), mocks.CreateOrderCmd{ //nolint:errcheck
						ID:    fmt.Sprintf("race1-%d-%d", iter, idx),
						Total: float64(idx + 1),
					})
				}(i)
			}
			wg.Wait()
			waitPublish(t, instance)
		}()
	}
}

// TestListen_BoundedRace_PostClosedGuard exercises branch #2: the closed.Load()
// check inside sendMu. With count=5 there is a window where handlers with n<count
// block on sendMu.Lock() while the count-th handler sets closed=true and releases
// the lock; those handlers then enter the body and return early via the closed guard.
func TestListen_BoundedRace_PostClosedGuard(t *testing.T) {
	for iter := range 10 {
		func() {
			instance := newInstance(t)

			// count=5 gives a wide window: 5 handlers can all pass received.Add
			// before any takes the lock. Whichever takes the lock last finds
			// closed==true (set by the count-th handler) and returns via branch #2.
			const count = 5
			ch, unsub, err := instance.Listen("OrderCreated", count)
			if err != nil {
				t.Fatalf("iter %d: Listen: %v", iter, err)
			}
			defer unsub()

			// Drain in background.
			go func() {
				for range ch {
				}
			}()

			// Burst more events than count so the n > count guard (branch #1) and
			// the post-close guard (branch #2) both get exercised.
			var wg sync.WaitGroup
			for i := range 50 {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					instance.Send(context.Background(), mocks.CreateOrderCmd{ //nolint:errcheck
						ID:    fmt.Sprintf("race2-%d-%d", iter, idx),
						Total: float64(idx + 1),
					})
				}(i)
			}
			wg.Wait()
			waitPublish(t, instance)
		}()
	}
}

func TestListen_UnsubDuringHandler_BoundedReturnsCleanly(t *testing.T) {
	// Verifies the defensive `if closed.Load()` check inside sendMu.Lock() in
	// bounded mode. The path triggers when unsub() flips `closed` after a
	// handler has claimed an n via received.Add(1) but before it acquires sendMu.
	// Stress with many iterations to ensure the timing window is hit.
	for iter := 0; iter < 100; iter++ {
		instance := newInstance(t)

		ch, unsub, err := instance.Listen("OrderCreated", 50)
		if err != nil {
			t.Fatalf("iter %d: Listen: %v", iter, err)
		}

		// Drain in background so sends don't block.
		drained := make(chan struct{})
		go func() {
			for range ch {
			}
			close(drained)
		}()

		// Fire several events concurrently, then immediately call unsub().
		var wg sync.WaitGroup
		for j := 0; j < 20; j++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				cmd := mocks.CreateOrderCmd{ID: fmt.Sprintf("uh-%d-%d", iter, idx), Total: 1.0}
				_, _ = instance.Send(context.Background(), cmd)
			}(j)
		}

		// Race unsub against the in-flight events. unsub() doesn't take sendMu,
		// so it can flip `closed` between a handler's Add() and Lock().
		unsub()
		wg.Wait()

		// Cleanup: ensure no goroutine is leaked.
		<-drained
		instance.Shutdown(context.Background())
	}
}

// --- SubscribeWait ---

func TestSubscribeWait_EmptyPattern(t *testing.T) {
	instance := newInstance(t)
	_, err := instance.SubscribeWait(context.Background(), "")
	if err != models.ErrEmptyPattern {
		t.Fatalf("expected ErrEmptyPattern, got %v", err)
	}
}

func TestSubscribeWait_EventArrivesBeforeDeadline(t *testing.T) {
	instance := newInstance(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stop := feedOrders(instance, "sw")
	defer stop()

	evt, err := instance.SubscribeWait(ctx, "OrderCreated")
	if err != nil {
		t.Fatalf("SubscribeWait: %v", err)
	}
	if !strings.HasPrefix(evt.AggregateID, "sw-") {
		t.Fatalf("expected AggregateID sw-*, got %q", evt.AggregateID)
	}
	if evt.Aggregate.Total != 1.0 {
		t.Fatalf("expected Total 1.0, got %f", evt.Aggregate.Total)
	}
}

func TestSubscribeWait_DeadlineExceeded(t *testing.T) {
	instance := newInstance(t)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := instance.SubscribeWait(ctx, "OrderCreated")
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestSubscribeWait_ContextCancel_ReturnsCanceled(t *testing.T) {
	instance := newInstance(t)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := instance.SubscribeWait(ctx, "OrderCreated")
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestSubscribeWait_ConcurrentDifferentPatterns(t *testing.T) {
	instance := newInstance(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	results := make([]models.Event[mocks.Order], 2)
	errs := make([]error, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		results[0], errs[0] = instance.SubscribeWait(ctx, "OrderCreated")
	}()
	go func() {
		defer wg.Done()
		results[1], errs[1] = instance.SubscribeWait(ctx, "OrderCancelled")
	}()

	stop := feedOrders(instance, "conc", func(id string) models.Command[mocks.Order] { return mocks.CancelOrderCmd{ID: id} })
	defer stop()

	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("SubscribeWait[%d]: %v", i, err)
		}
	}
	if results[0].EventName != "OrderCreated" {
		t.Fatalf("expected OrderCreated, got %q", results[0].EventName)
	}
	if results[1].EventName != "OrderCancelled" {
		t.Fatalf("expected OrderCancelled, got %q", results[1].EventName)
	}
}

func TestSubscribeWait_AutoUnsubscribes(t *testing.T) {
	// After SubscribeWait returns (event arrived), the subscription should be cleaned up.
	// We verify this by sending a SECOND matching event and confirming it does not
	// affect anything (no panic, no leak). Indirect — but the proper assertion is
	// via leak-detection which is out of scope here.
	instance := newInstance(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stop := feedOrders(instance, "feed")

	_, err := instance.SubscribeWait(ctx, "OrderCreated")
	stop()
	if err != nil {
		t.Fatalf("SubscribeWait: %v", err)
	}

	// Second event after SubscribeWait returned — must not affect anything.
	if _, err := instance.SendWait(context.Background(), mocks.CreateOrderCmd{ID: "auto-2", Total: 2.0}); err != nil {
		t.Fatalf("second SendWait: %v", err)
	}
}

func TestListen_BoundedSubID_NoLeakOnImmediateFire(t *testing.T) {
	// Stress: race the subID publication against the handler's auto-unsub.
	// If the bug is present, repeated runs will leak subscriptions silently.
	// We can't directly observe bus state from outside, but we can detect
	// the leak via behavior: after Listen returns and we've consumed the one event,
	// publishing more events on the same pattern must not invoke any "leftover" handler.
	for iter := 0; iter < 50; iter++ {
		instance := newInstance(t)

		ch, unsub, err := instance.Listen("OrderCreated", 1)
		if err != nil {
			t.Fatalf("iter %d: Listen: %v", iter, err)
		}

		// Fire immediately to race subID publication.
		if _, err := instance.SendWait(context.Background(), mocks.CreateOrderCmd{ID: "leak-1", Total: 1.0}); err != nil {
			t.Fatalf("iter %d: SendWait: %v", iter, err)
		}

		// Drain the one event.
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatalf("iter %d: did not receive event", iter)
		}

		// Subscribe a *new* counting handler on the same pattern AFTER auto-unsub.
		// If the original Listen subscription leaked, it would still be in the index;
		// however we cannot detect that directly from the public API.
		// Instead: confirm Listen's channel does NOT receive a second event.
		// (The leaked subscription's handler would short-circuit on closed.Load(),
		// so this test mostly stresses the race rather than asserting the leak —
		// useful as a regression guard alongside the fix.)
		if _, err := instance.SendWait(context.Background(), mocks.CreateOrderCmd{ID: "leak-2", Total: 1.0}); err != nil {
			t.Fatalf("iter %d: SendWait 2: %v", iter, err)
		}

		select {
		case evt, ok := <-ch:
			if ok {
				t.Fatalf("iter %d: leaked event %v on closed-channel sub", iter, evt)
			}
		case <-time.After(50 * time.Millisecond):
			// Channel closed (good) or no extra send (also good)
		}

		unsub()
		instance.Shutdown(context.Background())
	}
}

func TestListen_UnboundedMode_UnsubReleasesBlockedHandler(t *testing.T) {
	// Verifies that in unbounded mode (count=0), a handler blocked on a full
	// channel does not deadlock unsub().
	instance := newInstance(t)

	ch, unsub, err := instance.Listen("OrderCreated", 0)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	// Fill the buffer (16) plus a few more events that will block in handlers.
	// Use Send (async) so we don't block the test goroutine.
	for i := range 25 {
		cmd := mocks.CreateOrderCmd{ID: fmt.Sprintf("fill-%d", i), Total: 1.0}
		if _, err := instance.Send(context.Background(), cmd); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}

	// The buffer is full once handlers have nowhere left to put an event: the
	// remaining ones are blocked on ch <- evt.
	eventually(t, "the Listen buffer to fill", func() bool { return len(ch) == cap(ch) })

	// unsub MUST return promptly even with handlers blocked.
	done := make(chan struct{})
	go func() {
		unsub()
		close(done)
	}()

	select {
	case <-done:
		// Good
	case <-time.After(2 * time.Second):
		t.Fatal("unsub deadlocked — handlers blocked on full channel")
	}

	// Drain whatever's in the channel — no panic, no hang.
	drainTimeout := time.After(500 * time.Millisecond)
	for {
		select {
		case <-ch:
		case <-drainTimeout:
			return
		}
	}
}


func TestListen_BoundedConcurrent_DeliversAllCountEvents(t *testing.T) {
	// Regression test for close-race bug: with count > 1 and concurrent events,
	// the n==count handler could acquire sendMu before n<count handlers and close
	// the channel prematurely, dropping earlier events.
	// Run multiple iterations to expose the race; the fix uses a `delivered`
	// counter inside sendMu so close fires only after exactly `count` real sends.
	for iter := 0; iter < 20; iter++ {
		instance := newInstance(t)

		const count = 5
		ch, unsub, err := instance.Listen("OrderCreated", count)
		if err != nil {
			t.Fatalf("iter %d: Listen: %v", iter, err)
		}

		// Fire `count` events concurrently to maximize lock-acquisition reordering.
		var wg sync.WaitGroup
		for j := 0; j < count; j++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				cmd := mocks.CreateOrderCmd{ID: fmt.Sprintf("br-%d-%d", iter, idx), Total: float64(idx + 1)}
				_, _ = instance.Send(context.Background(), cmd)
			}(j)
		}
		wg.Wait()

		// Drain the channel — must receive exactly `count` events before close.
		received := 0
		drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		for {
			select {
			case _, ok := <-ch:
				if !ok {
					goto done
				}
				received++
			case <-drainCtx.Done():
				cancel()
				t.Fatalf("iter %d: timed out waiting for events; received %d/%d", iter, received, count)
			}
		}
	done:
		cancel()

		if received != count {
			t.Fatalf("iter %d: received %d events, want %d (close-race bug?)", iter, received, count)
		}

		unsub()
		instance.Shutdown(context.Background())
	}
}

// TestWaitPublish_NoRaceWithConcurrentDispatch reproduces the race where a pool
// worker is still inside dispatcher.Dispatch() when WaitPublish() returns.
//
// The race: a goroutine enters Dispatch() and accesses d.mu/d.queues before
// calling jobsWg.Add(1). If WaitIdle() checks idleness too early, it can
// return while that goroutine is still inside Dispatch() accessing protected
// fields, causing a data race.
//
// Run with: go test -race -run TestWaitPublish_NoRaceWithConcurrentDispatch ./...
func TestWaitPublish_NoRaceWithConcurrentDispatch(t *testing.T) {
	// Run many iterations to catch the tight race window.
	for iter := 0; iter < 100; iter++ {
		func() {
			instance := newInstance(t)

			// Send many commands concurrently, waiting for all Send() calls to
			// complete (i.e., all commands are enqueued to their pool shards).
			const n = 50
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					cmd := mocks.CreateOrderCmd{
						ID:    fmt.Sprintf("race-wp-%d-%d", iter, idx),
						Total: float64(idx + 1),
					}
					instance.Send(context.Background(), cmd) //nolint:errcheck
				}(i)
			}
			wg.Wait() // All Send() calls have returned; commands are enqueued

			// Now some pool workers may still be inside executor.Execute() →
			// dispatcher.Dispatch() even though Send() returned. WaitPublish()
			// must wait for these workers to finish before returning.
			// The race detector fires if a worker is still in Dispatch()
			// accessing d.mu/d.queues when WaitPublish() returns.
			waitPublish(t, instance)

			// If we reach here without race detector firing, the bug is fixed.
		}()
	}
}

// TestWaitPublish_RaceWithAsyncReactionGoroutines reproduces the race where a
// reaction handler spawns a goroutine that sends commands to asynx while
// WaitPublish() is being called.
//
// Run with: go test -race -run TestWaitPublish_RaceWithAsyncReactionGoroutines ./...
func TestWaitPublish_RaceWithAsyncReactionGoroutines(t *testing.T) {
	for iter := 0; iter < 100; iter++ {
		func() {
			instance := newInstance(t)

			// Register a reaction that spawns a goroutine sending commands
			var reactionStarted atomic.Int32
			var reactionFinished atomic.Int32
			
			handlerID, err := instance.Subscribe("OrderCreated", func(ctx context.Context, evt models.Event[mocks.Order]) {
				reactionStarted.Store(1)
				defer func() { reactionFinished.Store(1) }()
				
				// Spawn a goroutine that sends a command after a small delay
				go func() {
					// Small delay to ensure this happens while WaitPublish is being called
					time.Sleep(1 * time.Millisecond)
					
					// Send a new command from the reaction handler's goroutine
					cmd := mocks.UpdateOrderCmd{
						ID:       evt.AggregateID,
						NewState: mocks.Order{ID: evt.AggregateID, Total: 999.0, Status: "Updated"},
					}
					instance.Send(ctx, cmd) //nolint:errcheck
				}()
			})
			if err != nil {
				t.Fatalf("Subscribe failed: %v", err)
			}
			defer instance.Unsubscribe(handlerID)

			// Send an initial command to trigger the reaction
			cmd := mocks.CreateOrderCmd{ID: fmt.Sprintf("race-reaction-%d", iter), Total: 100.0}
			_, err = instance.Send(context.Background(), cmd)
			if err != nil {
				t.Fatalf("Send failed: %v", err)
			}

			// Wait for publish - this is where the race should occur if unfixed
			waitPublish(t, instance)

			// Verify the reaction handler goroutine completed
			if reactionStarted.Load() == 0 {
				t.Logf("Iteration %d: reaction never started", iter)
			}
			if reactionFinished.Load() == 0 {
				t.Logf("Iteration %d: reaction never finished", iter)
			}
		}()
	}
}

func within[V any](t *testing.T, ch <-chan V, what string) V {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

type versions struct {
	mu  sync.Mutex
	got []int64
}

func (v *versions) add(n int64) {
	v.mu.Lock()
	v.got = append(v.got, n)
	v.mu.Unlock()
}

func (v *versions) list() []int64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]int64(nil), v.got...)
}

func equalVersions(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// A restart after a shutdown that interrupted a retry must still deliver in
// version order: v2 may not be delivered and acknowledged past a pending v1.
func TestFailure_ShutdownDuringRetryKeepsVersionOrderAcrossRestart(t *testing.T) {
	s := store.New()
	ctx := context.Background()

	var seen versions
	attempted := make(chan struct{}, 64)
	first, err := eight(s).
		WithDeliveryRetry(time.Hour, time.Hour).
		WithSubscriber("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) error {
			seen.add(e.Version)
			if e.Version == 1 {
				attempted <- struct{}{}
				return errors.New("poison")
			}
			return nil
		}).Build()
	if err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if _, err := first.Send(ctx, upd("agg", float64(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	within(t, attempted, "first attempt")

	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := first.Shutdown(sctx); err != nil {
		t.Fatalf("Shutdown with a retrying delivery: %v", err)
	}

	if got := seen.list(); !equalVersions(got, []int64{1}) {
		t.Fatalf("handler saw versions %v before restart, want only the failing v1", got)
	}
	pending, _ := s.PendingDeliveries(ctx, 10)
	if len(pending) != 2 || pending[0].Version != 1 || pending[1].Version != 2 {
		t.Fatalf("pending after shutdown = %v, want v1 and v2 both still pending", pending)
	}

	var after versions
	second := build(t, eight(s).WithSubscriber("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) error {
		after.add(e.Version)
		return nil
	}))
	waitPublish(t, second)
	if got := after.list(); !equalVersions(got, []int64{1, 2}) {
		t.Fatalf("after restart delivered %v, want [1 2]", got)
	}
}

// One event whose handler always fails must not hold a shard worker, outlast
// its caller's context, or keep the process from shutting down.
func TestFailure_PoisonEventDoesNotBlockTheShardOrShutdown(t *testing.T) {
	s := store.New()
	ctx := context.Background()

	attempted := make(chan struct{}, 64)
	inst, err := asynx.New[mocks.Order]().
		WithEventStore(s).
		WithSnapshotStore(store.NewSnapshots()).
		WithShardingOpts(asynx.ShardingOpts{Shards: 1, WorkersPerShard: 1, QueueDepth: 100}).
		WithDeliveryRetry(time.Hour, time.Hour).
		WithSubscriber("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) error {
			if strings.HasPrefix(e.AggregateID, "bad") && e.EventName != models.ForgetEventName {
				attempted <- struct{}{}
				return errors.New("poison")
			}
			return nil
		}).
		Build()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := inst.Send(ctx, upd("bad", 1)); err != nil {
		t.Fatal(err)
	}
	within(t, attempted, "poison attempt")

	// SendWait for the poisoned aggregate waits behind the failing delivery but
	// obeys its context.
	wctx, cancelWait := context.WithCancel(ctx)
	waited := make(chan error, 1)
	go func() {
		_, err := inst.SendWait(wctx, upd("bad", 2))
		waited <- err
	}()
	// The command commits (the worker is free), then waits for its handlers.
	eventually(t, "the second command to commit", func() bool {
		select {
		case err := <-waited:
			t.Fatalf("SendWait returned %v before committing", err)
		default:
		}
		blobs, _ := s.ReadFrom(ctx, "events:bad", 1)
		return len(blobs) == 2
	})
	cancelWait()
	if err := within(t, waited, "SendWait to give up"); !errors.Is(err, models.ErrContextCancelled) {
		t.Fatalf("SendWait err = %v, want ErrContextCancelled", err)
	}

	// The single worker of the single shard is free for other aggregates.
	if _, err := inst.SendWait(ctx, upd("good", 1)); err != nil {
		t.Fatalf("unrelated aggregate on the same shard: %v", err)
	}

	// Forget of the poisoned aggregate goes through: its failing deliveries are
	// abandoned, then the tombstone is delivered.
	fctx, cancelForget := context.WithTimeout(ctx, 5*time.Second)
	defer cancelForget()
	if err := inst.Forget(fctx, "bad"); err != nil {
		t.Fatalf("Forget of a poisoned aggregate: %v", err)
	}
	if ok, _ := inst.Exists(ctx, "bad"); ok {
		t.Fatal("poisoned aggregate survived Forget")
	}

	// Shutdown with another poison delivery in flight finishes.
	if _, err := inst.Send(ctx, upd("bad2", 1)); err != nil {
		t.Fatal(err)
	}
	within(t, attempted, "second poison attempt")
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := inst.Shutdown(sctx); err != nil {
		t.Fatalf("Shutdown with a poison delivery: %v", err)
	}
}

func TestFailure_RetriesStopAndTheAggregateIsParkedUntilResumed(t *testing.T) {
	s := store.New()
	ctx := context.Background()

	var healthy bool
	var mu sync.Mutex
	var seen versions
	parked := make(chan error, 8)
	inst := build(t, eight(s).
		WithDeliveryRetry(time.Millisecond, 2*time.Millisecond).
		WithDeliveryMaxAttempts(3).
		WithDeliveryErrorHandler(func(_ context.Context, _ models.Event[mocks.Order], err error, _ int) {
			if errors.Is(err, models.ErrDeliveryParked) {
				parked <- err
			}
		}).
		WithSubscriber("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) error {
			mu.Lock()
			ok := healthy || e.AggregateID != "bad"
			mu.Unlock()
			if !ok {
				return errors.New("read model down")
			}
			if e.AggregateID == "bad" {
				seen.add(e.Version)
			}
			return nil
		}))

	for i := range 2 {
		if _, err := inst.Send(ctx, upd("bad", float64(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	within(t, parked, "park report")

	if _, err := inst.SendWait(ctx, upd("other", 1)); err != nil {
		t.Fatalf("another aggregate: %v", err)
	}
	// A new event of the parked aggregate commits but is not delivered, and
	// SendWait says so instead of reporting success.
	if evt, err := inst.SendWait(ctx, upd("bad", 3)); !errors.Is(err, models.ErrDeliveryParked) || evt.Version != 3 {
		t.Fatalf("SendWait to a parked aggregate = v%d, %v; want the committed v3 with ErrDeliveryParked", evt.Version, err)
	}
	if got := inst.PausedAggregates(); !slices.Equal(got, []string{"bad"}) {
		t.Fatalf("PausedAggregates = %v, want [bad]", got)
	}
	if got := seen.list(); len(got) != 0 {
		t.Fatalf("parked aggregate delivered %v", got)
	}
	pending, _ := s.PendingDeliveries(ctx, 10)
	nBad := 0
	for _, p := range pending {
		if p.AggregateID == "bad" {
			nBad++
		}
	}
	if nBad != 3 {
		t.Fatalf("pending deliveries of the parked aggregate = %d, want 3 (%v)", nBad, pending)
	}

	mu.Lock()
	healthy = true
	mu.Unlock()
	if err := inst.ResumeDeliveries(ctx); err != nil {
		t.Fatalf("ResumeDeliveries: %v", err)
	}
	waitPublish(t, inst)
	if got := seen.list(); !equalVersions(got, []int64{1, 2, 3}) {
		t.Fatalf("after resume delivered %v, want [1 2 3] in order", got)
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 0 {
		t.Fatalf("pending after resume = %v", pending)
	}
}

func TestFailure_DeadLetterSkipsThePoisonEvent(t *testing.T) {
	ctx := context.Background()
	var seen versions
	dead := make(chan int64, 4)
	inst := build(t, eight(store.New()).
		WithDeliveryRetry(time.Millisecond, 2*time.Millisecond).
		WithDeliveryMaxAttempts(2).
		WithDeadLetterHandler(func(_ context.Context, e models.Event[mocks.Order], _ error) error {
			dead <- e.Version
			return nil
		}).
		WithSubscriber("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) error {
			if e.Version == 1 {
				return errors.New("poison")
			}
			seen.add(e.Version)
			return nil
		}))
	for i := range 3 {
		if _, err := inst.Send(ctx, upd("agg", float64(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	if v := within(t, dead, "dead letter"); v != 1 {
		t.Fatalf("dead-lettered v%d, want v1", v)
	}
	waitPublish(t, inst)
	if got := seen.list(); !equalVersions(got, []int64{2, 3}) {
		t.Fatalf("delivered %v after skipping v1, want [2 3]", got)
	}
}

// Between the tombstone and the erase, no command may commit.
func TestFailure_CommandsAreRejectedWhileForgetRuns(t *testing.T) {
	s := store.New()
	ctx := context.Background()
	inst := build(t, eight(s))

	entered := make(chan struct{})
	release := make(chan struct{})
	var reaction error
	if _, err := inst.OnForget(func(c context.Context, e models.Event[mocks.Order]) {
		close(entered)
		// A reaction sending a command to the aggregate it is told was forgotten
		// is refused, not deadlocked.
		_, reaction = inst.Send(c, upd("agg", 99))
		<-release
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := inst.Send(ctx, upd("agg", 1)); err != nil {
		t.Fatal(err)
	}

	forgot := make(chan error, 1)
	go func() { forgot <- inst.Forget(ctx, "agg") }()
	within(t, entered, "forget handler")

	if _, err := inst.Send(ctx, upd("agg", 2)); !errors.Is(err, models.ErrForgetInProgress) {
		t.Fatalf("Send during Forget err = %v, want ErrForgetInProgress", err)
	}
	// Other aggregates are unaffected.
	if _, err := inst.Send(ctx, upd("other", 1)); err != nil {
		t.Fatalf("unrelated aggregate during Forget: %v", err)
	}

	close(release)
	if err := within(t, forgot, "Forget"); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if !errors.Is(reaction, models.ErrForgetInProgress) {
		t.Fatalf("reaction err = %v, want ErrForgetInProgress", reaction)
	}
	if blobs, _ := s.ReadFrom(ctx, "events:agg", 1); len(blobs) != 0 {
		t.Fatalf("stream has %d events after Forget: a command slipped in", len(blobs))
	}

	// After the erase the aggregate can be created again, from version 1.
	evt, err := inst.Send(ctx, upd("agg", 3))
	if err != nil {
		t.Fatalf("Send after Forget: %v", err)
	}
	if evt.Version != 1 {
		t.Fatalf("re-created aggregate version = %d, want 1", evt.Version)
	}
}

// A subscriber that dedupes on (AggregateID, Version) needs the deletion marker
// to know a re-created aggregate starts over.
func TestFailure_DurableSubscribersReceiveTheDeletionMarker(t *testing.T) {
	ctx := context.Background()
	type key struct {
		id string
		v  int64
	}
	var mu sync.Mutex
	seen := map[key]bool{}
	var applied []int64
	var markers int

	inst := build(t, eight(store.New()).
		WithSubscriber("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) error {
			mu.Lock()
			defer mu.Unlock()
			if e.EventName == models.ForgetEventName {
				markers++
				for k := range seen {
					if k.id == e.AggregateID {
						delete(seen, k)
					}
				}
				return nil
			}
			k := key{e.AggregateID, e.Version}
			if seen[k] {
				return nil // duplicate delivery
			}
			seen[k] = true
			applied = append(applied, e.Version)
			return nil
		}))

	if _, err := inst.SendWait(ctx, upd("agg", 1)); err != nil {
		t.Fatal(err)
	}
	if err := inst.Forget(ctx, "agg"); err != nil {
		t.Fatal(err)
	}
	if _, err := inst.SendWait(ctx, upd("agg", 2)); err != nil {
		t.Fatal(err)
	}
	waitPublish(t, inst)

	mu.Lock()
	defer mu.Unlock()
	if markers != 1 {
		t.Fatalf("deletion markers seen = %d, want 1 (pattern OrderUpdated does not match it, it is delivered anyway)", markers)
	}
	if !equalVersions(applied, []int64{1, 1}) {
		t.Fatalf("applied versions = %v, want the re-created aggregate's v1 applied again", applied)
	}
}

// If a durable subscriber cannot process the deletion marker, the aggregate is
// not erased: Forget fails and can be retried.
func TestFailure_ForgetFailsWhenADurableSubscriberCannotTakeTheMarker(t *testing.T) {
	ctx := context.Background()
	var healthy bool
	var mu sync.Mutex
	inst := build(t, eight(store.New()).
		WithDeliveryRetry(time.Millisecond, 2*time.Millisecond).
		WithDeliveryMaxAttempts(2).
		WithSubscriber("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) error {
			mu.Lock()
			defer mu.Unlock()
			if e.EventName == models.ForgetEventName && !healthy {
				return errors.New("cannot drop state")
			}
			return nil
		}))
	if _, err := inst.SendWait(ctx, upd("agg", 1)); err != nil {
		t.Fatal(err)
	}

	if err := inst.Forget(ctx, "agg"); !errors.Is(err, models.ErrForgetFailed) {
		t.Fatalf("Forget err = %v, want ErrForgetFailed", err)
	}
	// Forget aborted before erasing; commands are accepted again, but the
	// aggregate stays paused so their events wait behind the marker.
	if _, err := inst.SendWait(ctx, upd("agg", 2)); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("SendWait after a failed Forget = %v, want ErrDeliveryParked", err)
	}

	mu.Lock()
	healthy = true
	mu.Unlock()
	if err := inst.Forget(ctx, "agg"); err != nil {
		t.Fatalf("retried Forget: %v", err)
	}
	if ok, _ := inst.Exists(ctx, "agg"); ok {
		t.Fatal("aggregate survived the retried Forget")
	}
}

type failingLiveBus struct{ models.Bus[mocks.Order] }

func (failingLiveBus) PublishSync(context.Context, models.Event[mocks.Order]) error {
	return errors.New("live bus down")
}

// A failing live bus must not make durable delivery skip an event, or the
// aggregate's next event would be delivered past it.
func TestFailure_LiveBusErrorDoesNotSkipDurableDelivery(t *testing.T) {
	s := store.New()
	ctx := context.Background()
	var seen versions
	reported := make(chan error, 8)
	inst := build(t, eight(s).
		WithBus(failingLiveBus{bus.NewChannelBus[mocks.Order]()}).
		WithPublishErrorHandler(func(_ context.Context, _ models.Event[mocks.Order], err error) { reported <- err }).
		WithSubscriber("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) error {
			seen.add(e.Version)
			return nil
		}))
	for i := range 3 {
		if _, err := inst.SendWait(ctx, upd("agg", float64(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	waitPublish(t, inst)
	if got := seen.list(); !equalVersions(got, []int64{1, 2, 3}) {
		t.Fatalf("durable deliveries = %v, want [1 2 3]", got)
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 0 {
		t.Fatalf("pending = %v, want none", pending)
	}
	within(t, reported, "bus error report")
}

// A failed Forget supersedes the aggregate's pending deliveries for good and
// keeps later events behind the deletion marker until the aggregate is resumed.
func TestFailure_FailedForgetDoesNotLetLaterEventsOvertakeTheMarker(t *testing.T) {
	s := store.New()
	ctx := context.Background()
	var mu sync.Mutex
	healthy := false
	var delivered []string
	inst := build(t, eight(s).
		WithDeliveryRetry(time.Millisecond, 2*time.Millisecond).
		WithDeliveryMaxAttempts(100000).
		WithSubscriber("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) error {
			mu.Lock()
			defer mu.Unlock()
			if !healthy {
				return errors.New("down")
			}
			delivered = append(delivered, fmt.Sprintf("%s/%d", e.EventName, e.Version))
			return nil
		}))

	if _, err := inst.Send(ctx, upd("agg", 1)); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := inst.Forget(short, "agg"); !errors.Is(err, models.ErrForgetFailed) {
		t.Fatalf("Forget = %v, want ErrForgetFailed", err)
	}
	if got := inst.PausedAggregates(); !slices.Equal(got, []string{"agg"}) {
		t.Fatalf("PausedAggregates = %v, want [agg]", got)
	}
	// The tombstone's transaction dropped the superseded v1 entry, so a restart
	// cannot deliver it after the marker, and left the marker (v2) pending.
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 1 || pending[0].Version != 2 {
		t.Fatalf("pending after the tombstone = %v, want only the marker v2", pending)
	}

	if _, err := inst.SendWait(ctx, upd("agg", 3)); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("SendWait = %v, want ErrDeliveryParked", err)
	}
	mu.Lock()
	healthy = true
	if len(delivered) != 0 {
		t.Fatalf("delivered %v while the marker was undelivered", delivered)
	}
	mu.Unlock()

	if err := inst.ResumeDeliveries(ctx); err != nil {
		t.Fatal(err)
	}
	waitPublish(t, inst)
	mu.Lock()
	defer mu.Unlock()
	if want := []string{models.ForgetEventName + "/2", "OrderUpdated/3"}; !slices.Equal(delivered, want) {
		t.Fatalf("after resume delivered %v, want the marker first, then the event after it: %v", delivered, want)
	}
	if len(inst.PausedAggregates()) != 0 {
		t.Fatalf("still paused: %v", inst.PausedAggregates())
	}
}

// Live-only events of a paused aggregate are not replayed when it resumes.
func TestFailure_ResumeDoesNotReplayLiveSubscribers(t *testing.T) {
	s := store.New()
	ctx := context.Background()
	var mu sync.Mutex
	healthy := false
	var live, cancelled versions
	inst := build(t, eight(s).
		WithDeliveryRetry(time.Millisecond, 2*time.Millisecond).
		WithDeliveryMaxAttempts(2).
		WithSubscriber("OrderUpdated", func(context.Context, models.Event[mocks.Order]) error {
			mu.Lock()
			defer mu.Unlock()
			if !healthy {
				return errors.New("down")
			}
			return nil
		}))
	if _, err := inst.Subscribe("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) { live.add(e.Version) }); err != nil {
		t.Fatal(err)
	}
	if _, err := inst.Subscribe("OrderCancelled", func(_ context.Context, e models.Event[mocks.Order]) { cancelled.add(e.Version) }); err != nil {
		t.Fatal(err)
	}

	if _, err := inst.Send(ctx, upd("agg", 1)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "aggregate paused", func() bool { return len(inst.PausedAggregates()) == 1 })
	if _, err := inst.SendWait(ctx, mocks.CancelOrderCmd{ID: "agg"}); err != nil {
		t.Fatalf("live-only event on a paused aggregate: %v", err)
	}
	if _, err := inst.SendWait(ctx, upd("agg", 3)); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("SendWait = %v, want ErrDeliveryParked", err)
	}

	mu.Lock()
	healthy = true
	mu.Unlock()
	if err := inst.ResumeDeliveries(ctx); err != nil {
		t.Fatal(err)
	}
	waitPublish(t, inst)
	if got := live.list(); !equalVersions(got, []int64{1}) {
		t.Fatalf("live OrderUpdated versions = %v, want [1]: nothing replayed", got)
	}
	if got := cancelled.list(); !equalVersions(got, []int64{2}) {
		t.Fatalf("live OrderCancelled versions = %v, want [2] once", got)
	}
}

// A handler cannot forget the aggregate it is handling, but the failed attempt
// leaves the aggregate usable and shutdown clean.
func TestFailure_HandlerForgettingItsOwnAggregateFailsFast(t *testing.T) {
	ctx := context.Background()
	var inst asynx.Asynx[mocks.Order]
	got := make(chan error, 1)
	inst = build(t, eight(store.New()).
		WithSubscriber("OrderUpdated", func(ctx context.Context, e models.Event[mocks.Order]) error {
			got <- inst.Forget(ctx, e.AggregateID)
			return nil
		}))
	if _, err := inst.SendWait(ctx, upd("agg", 1)); err != nil {
		t.Fatal(err)
	}
	if err := within(t, got, "Forget from the handler"); !errors.Is(err, models.ErrForgetFromHandler) {
		t.Fatalf("Forget from a handler = %v, want ErrForgetFromHandler", err)
	}
	if _, err := inst.SendWait(ctx, upd("agg", 2)); err != nil {
		t.Fatalf("the aggregate is frozen after the refused Forget: %v", err)
	}
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := inst.Shutdown(sctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// A crash after a failed Forget must not deliver anything ahead of the marker:
// the restart delivers the marker first, then what followed it.
func TestFailure_RestartAfterAFailedForgetDeliversTheMarkerFirst(t *testing.T) {
	s := store.New()
	ctx := context.Background()
	down := build(t, eight(s).
		WithDeliveryRetry(time.Millisecond, 2*time.Millisecond).
		WithDeliveryMaxAttempts(2).
		WithSubscriber("OrderUpdated", func(context.Context, models.Event[mocks.Order]) error { return errors.New("down") }))
	if _, err := down.Send(ctx, upd("agg", 1)); err != nil {
		t.Fatal(err)
	}
	if err := down.Forget(ctx, "agg"); !errors.Is(err, models.ErrForgetFailed) {
		t.Fatalf("Forget = %v, want ErrForgetFailed", err)
	}
	if _, err := down.SendWait(ctx, upd("agg", 3)); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("SendWait = %v, want ErrDeliveryParked", err)
	}
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := down.Shutdown(sctx); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var delivered []string
	up := build(t, eight(s).
		WithSubscriber("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) error {
			mu.Lock()
			defer mu.Unlock()
			delivered = append(delivered, fmt.Sprintf("%s/%d", e.EventName, e.Version))
			return nil
		}))
	waitPublish(t, up)
	mu.Lock()
	defer mu.Unlock()
	if want := []string{models.ForgetEventName + "/2", "OrderUpdated/3"}; !slices.Equal(delivered, want) {
		t.Fatalf("after the restart delivered %v, want %v", delivered, want)
	}
}

// feedOrders sends a new order, and a follow-up command when given, every time
// it can, until the returned stop is called. It stands in for traffic that
// starts once a subscription exists, without guessing how long that takes: the
// subscription sees the first order created after it registers.
func feedOrders(
	instance asynx.Asynx[mocks.Order],
	prefix string,
	followUps ...func(id string) models.Command[mocks.Order],
) (stop func()) {
	quit := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for i := 0; ; i++ {
			select {
			case <-quit:
				return
			default:
			}
			id := fmt.Sprintf("%s-%d", prefix, i)
			if _, err := instance.SendWait(context.Background(), mocks.CreateOrderCmd{ID: id, Total: 1}); err != nil {
				return
			}
			for _, followUp := range followUps {
				instance.SendWait(context.Background(), followUp(id)) //nolint:errcheck
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() { close(quit); <-finished })
	}
}

// blockingSnapshots holds the first armed Put for one aggregate until released.
type blockingSnapshots struct {
	models.SnapshotStore
	aggregate string
	armed     atomic.Bool
	entered   chan struct{}
	release   chan struct{}
}

func (s *blockingSnapshots) Put(
	ctx context.Context,
	aggregateID string,
	version int64,
	data []byte,
) error {
	if aggregateID == s.aggregate && s.armed.CompareAndSwap(true, false) {
		close(s.entered)
		<-s.release
	}
	return s.SnapshotStore.Put(ctx, aggregateID, version, data)
}

// A Get that upcasts writes an auto-snapshot without holding the aggregate's
// lock. It must not be able to land after a Forget's erase, which would bring
// the erased state back and make the re-created aggregate start above v1.
func TestGet_AutoSnapshotCannotResurrectAForgottenAggregate(t *testing.T) {
	ctx := context.Background()
	s := store.New()
	old := build(t, eight(s))
	for _, cmd := range []models.Command[mocks.Order]{mocks.CreateOrderCmd{ID: "ws", Total: 10}, upd("ws", 20)} {
		if _, err := old.SendWait(ctx, cmd); err != nil {
			t.Fatal(err)
		}
	}
	if err := old.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	snaps := &blockingSnapshots{SnapshotStore: store.NewSnapshots(), aggregate: "ws", entered: make(chan struct{}), release: make(chan struct{})}
	inst := build(t, asynx.New[mocks.Order]().WithEventStore(s).WithSnapshotStore(snaps).
		WithSchemaVersion(2).
		WithUpcaster(1, func(_ context.Context, _ string, p []byte) ([]byte, error) { return p, nil }))
	snaps.armed.Store(true)
	got := make(chan error, 1)
	go func() { _, err := inst.Get(ctx, "ws"); got <- err }()
	within(t, snaps.entered, "the auto-snapshot write")

	forgot := make(chan error, 1)
	go func() { forgot <- inst.Forget(ctx, "ws") }()
	select {
	case err := <-forgot:
		t.Fatalf("Forget finished (%v) while an auto-snapshot of the aggregate was being written", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(snaps.release)
	if err := within(t, forgot, "Forget"); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	within(t, got, "Get")

	if exists, _ := inst.Exists(ctx, "ws"); exists {
		t.Fatal("the forgotten aggregate exists again")
	}
	if _, err := inst.Get(ctx, "ws"); !errors.Is(err, models.ErrNotFound) {
		t.Fatalf("Get after Forget = %v, want ErrNotFound", err)
	}
	if evt, err := inst.SendWait(ctx, mocks.CreateOrderCmd{ID: "ws", Total: 1}); err != nil || evt.Version != 1 {
		t.Fatalf("re-create = v%d, %v; want v1", evt.Version, err)
	}
}

// A retry of a command whose handlers are still running waits for them, as its
// first attempt would have.
func TestSendWait_RetryOfAnIdempotentCommandWaitsForTheOriginalHandlers(t *testing.T) {
	ctx := context.Background()
	inst := build(t, eight(store.New()))
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var finished atomic.Bool
	if _, err := inst.Subscribe("OrderCreated", func(context.Context, models.Event[mocks.Order]) {
		entered <- struct{}{}
		<-release
		finished.Store(true)
	}); err != nil {
		t.Fatal(err)
	}
	cmd := idemCreate{CreateOrderCmd: mocks.CreateOrderCmd{ID: "agg", Total: 1}, Cmd: "cmd-1"}

	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := inst.SendWait(short, cmd); !errors.Is(err, models.ErrContextCancelled) {
		t.Fatalf("first SendWait = %v, want ErrContextCancelled (committed, handler still running)", err)
	}
	within(t, entered, "the handler")

	retried := make(chan error, 1)
	go func() { _, err := inst.SendWait(ctx, cmd); retried <- err }()
	select {
	case err := <-retried:
		t.Fatalf("the retry returned (%v) before the original handler finished", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := within(t, retried, "the retry"); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !finished.Load() {
		t.Fatal("the retry returned before the handler finished")
	}
}

// A handler that Forgets its own aggregate with a context that is not its own
// cannot be recognised, and the Forget waits for the very handler that called it.
// Shutting down must still release it.
func TestShutdown_ReleasesAHandlerWaitingOnItsOwnAggregatesForget(t *testing.T) {
	ctx := context.Background()
	forgetResult := make(chan error, 1)
	var inst asynx.Asynx[mocks.Order]
	inst = build(t, eight(store.New()).
		WithSubscriber("OrderCreated", func(_ context.Context, e models.Event[mocks.Order]) error {
			forgetResult <- inst.Forget(context.Background(), e.AggregateID)
			return nil
		}))
	if _, err := inst.Send(ctx, mocks.CreateOrderCmd{ID: "agg", Total: 1}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the Forget to raise its fence", func() bool {
		_, err := inst.Send(ctx, upd("agg", 2))
		return errors.Is(err, models.ErrForgetInProgress)
	})

	shutdown, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := inst.Shutdown(shutdown); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	err := within(t, forgetResult, "the Forget")
	if !errors.Is(err, models.ErrForgetFailed) || !errors.Is(err, models.ErrShuttingDown) {
		t.Fatalf("Forget = %v, want ErrForgetFailed wrapping ErrShuttingDown", err)
	}
}

// When several durable subscribers fail, the dead-letter handler learns of each,
// by name.
func TestFailure_DeadLetterLearnsEverySubscriberThatFailed(t *testing.T) {
	ctx := context.Background()
	got := make(chan error, 1)
	billing, search := errors.New("billing down"), errors.New("search down")
	inst := build(t, eight(store.New()).
		WithDeliveryMaxAttempts(2).
		WithDeliveryRetry(time.Millisecond, time.Millisecond).
		WithDeadLetterHandler(func(_ context.Context, _ models.Event[mocks.Order], err error) error {
			got <- err
			return nil
		}).
		WithSubscriber("OrderUpdated", func(context.Context, models.Event[mocks.Order]) error { return billing }).
		WithSubscriber("OrderUpdated", func(context.Context, models.Event[mocks.Order]) error { return search }))
	if _, err := inst.Send(ctx, upd("agg", 1)); err != nil {
		t.Fatal(err)
	}

	err := within(t, got, "the dead letter")
	if !errors.Is(err, billing) || !errors.Is(err, search) {
		t.Fatalf("dead-letter handler received %q, want both failures", err)
	}
	if !strings.Contains(err.Error(), "OrderUpdated#1") || !strings.Contains(err.Error(), "OrderUpdated#2") {
		t.Fatalf("dead-letter handler received %q, want both subscribers named", err)
	}
	var sub *models.SubscriberError
	if !errors.As(err, &sub) {
		t.Fatal("errors.As does not find a SubscriberError")
	}
}
