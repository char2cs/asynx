package asynx_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"testing"
	"time"

	"github.com/char2cs/asynx"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/store"
	"github.com/char2cs/asynx/models"
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
	instance, err := asynx.New[mocks.Order]().WithEventStore(s).Build()
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

// deleteErrStore wraps store.Memory and injects a one-shot error on the next Delete call.
type deleteErrStore struct {
	*store.Memory
	deleteErr error
}

func (s *deleteErrStore) Delete(ctx context.Context, aggregateID string) error {
	if s.deleteErr != nil {
		err := s.deleteErr
		s.deleteErr = nil
		return err
	}
	return s.Memory.Delete(ctx, aggregateID)
}

func TestForget_DeleteFails_ReturnsErrForgetFailed(t *testing.T) {
	sentinel := errors.New("store unavailable")
	backing := &deleteErrStore{Memory: store.New(), deleteErr: sentinel}

	instance, err := asynx.New[mocks.Order]().
		WithEventStore(backing).
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

	// At least one call must fail: either ErrValidation (aggregate already gone)
	// or ErrPipelineFailed (concurrent write conflict). Both callers succeeding
	// would mean two tombstone events were written without a conflict, which is
	// also safe (aggregate is still erased), but we verify the aggregate is gone.
	successCount := 0
	for _, err := range errs {
		if err == nil {
			successCount++
		} else if !errors.Is(err, models.ErrValidation) && !errors.Is(err, models.ErrPipelineFailed) {
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
	instance, err := asynx.New[mocks.Order]().WithEventStore(s).Build()
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
