package asynx_test

import (
	"context"
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
	err := instance.Send(context.Background(), cmd)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}
}

func TestSend_ValidationFails(t *testing.T) {
	instance := newInstance(t)
	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: -5.0} // Invalid total
	err := instance.Send(context.Background(), cmd)
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
	err := instance.Send(ctx, cmd)
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
	err := instance.Send(context.Background(), cmd)
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
	err = instance.Send(context.Background(), cmd)
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
	err = instance.Send(context.Background(), cmd)
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
		err := instance.Send(context.Background(), cmd)
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
			errChan <- instance.Send(context.Background(), cmd)
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
	err := instance.Send(context.Background(), cmd)
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
	err := instance.Send(context.Background(), cmd)
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
	err = instance.Send(context.Background(), cmd2)
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
