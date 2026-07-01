package asynx_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/char2cs/asynx"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/store"
	"github.com/char2cs/asynx/models"
)

func newBenchInstance(b *testing.B) asynx.Asynx[mocks.Order] {
	s := store.New()
	instance, err := asynx.New[mocks.Order]().
		WithEventStore(s).
		WithShardingOpts(asynx.ShardingOpts{Shards: 8, QueueDepth: 10000}).
		Build()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { instance.Shutdown(context.Background()) })
	return instance
}

// Throughput benchmarks

func BenchmarkSend_SingleSerial(b *testing.B) {
	instance := newBenchInstance(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		orderID := fmt.Sprintf("order-%d", i)
		cmd := mocks.CreateOrderCmd{ID: orderID, Total: 100.0}
		instance.Send(context.Background(), cmd)
	}
}

func BenchmarkSend_Parallel_DifferentAggregates(b *testing.B) {
	instance := newBenchInstance(b)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			orderID := fmt.Sprintf("order-%d-%d", i, b.N)
			cmd := mocks.CreateOrderCmd{ID: orderID, Total: 100.0}
			instance.Send(context.Background(), cmd)
			i++
		}
	})
}

func BenchmarkSend_Parallel_SameAggregate(b *testing.B) {
	instance := newBenchInstance(b)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cmd := mocks.UpdateOrderCmd{
				ID:       "order-1",
				NewState: mocks.Order{ID: "order-1", Total: 100.0},
			}
			instance.Send(context.Background(), cmd)
		}
	})
}

// Latency benchmarks

func BenchmarkGet_WarmPath(b *testing.B) {
	instance := newBenchInstance(b)
	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	instance.Send(context.Background(), cmd)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		instance.Get(context.Background(), "order-1")
	}
}

func BenchmarkGet_ColdPath(b *testing.B) {
	instance := newBenchInstance(b)
	orderID := "order-1"
	cmd := mocks.CreateOrderCmd{ID: orderID, Total: 100.0}
	instance.Send(context.Background(), cmd)

	for i := 0; i < 50; i++ {
		upCmd := mocks.UpdateOrderCmd{
			ID:       orderID,
			NewState: mocks.Order{ID: orderID, Total: float64((i+2)*100.0)},
		}
		instance.Send(context.Background(), upCmd)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		instance.Get(context.Background(), orderID)
	}
}

func BenchmarkGet_WithSnapshot(b *testing.B) {
	instance := newBenchInstance(b)
	orderID := "order-1"
	cmd := mocks.CreateOrderCmd{ID: orderID, Total: 100.0}
	instance.Send(context.Background(), cmd)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		instance.Get(context.Background(), orderID)
	}
}

// Handler performance benchmarks

func BenchmarkPublish_SingleHandler(b *testing.B) {
	instance := newBenchInstance(b)
	handler := func(ctx context.Context, evt models.Event[mocks.Order]) {
	}
	subID, _ := instance.Subscribe("OrderCreated", handler)
	defer instance.Unsubscribe(subID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		orderID := fmt.Sprintf("order-%d", i)
		cmd := mocks.CreateOrderCmd{ID: orderID, Total: 100.0}
		instance.Send(context.Background(), cmd)
	}
}

func BenchmarkPublish_FiveHandlers(b *testing.B) {
	instance := newBenchInstance(b)
	handler := func(ctx context.Context, evt models.Event[mocks.Order]) {
	}
	ids := make([]string, 5)
	for i := 0; i < 5; i++ {
		id, _ := instance.Subscribe("OrderCreated", handler)
		ids[i] = id
	}
	defer func() {
		for _, id := range ids {
			instance.Unsubscribe(id)
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		orderID := fmt.Sprintf("order-%d", i)
		cmd := mocks.CreateOrderCmd{ID: orderID, Total: 100.0}
		instance.Send(context.Background(), cmd)
	}
}

func BenchmarkPublish_HandlerWithTimeout(b *testing.B) {
	instance := newBenchInstance(b)
	handler := func(ctx context.Context, evt models.Event[mocks.Order]) {
	}
	subID, _ := instance.Subscribe("OrderCreated", handler)
	defer instance.Unsubscribe(subID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		orderID := fmt.Sprintf("order-%d", i)
		cmd := mocks.CreateOrderCmd{ID: orderID, Total: 100.0}
		instance.Send(context.Background(), cmd)
	}
}

// Shutdown benchmarks

func BenchmarkShutdown_NoInFlight(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		instance := newBenchInstance(b)
		cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
		instance.Send(context.Background(), cmd)
		b.StartTimer()

		instance.Shutdown(context.Background())
	}
}

func BenchmarkShutdown_WithInFlight(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		instance := newBenchInstance(b)
		for j := 0; j < 100; j++ {
			orderID := fmt.Sprintf("order-%d", j)
			cmd := mocks.CreateOrderCmd{ID: orderID, Total: 100.0}
			go instance.Send(context.Background(), cmd)
		}
		b.StartTimer()

		instance.Shutdown(context.Background())
	}
}

// Allocations benchmarks

func BenchmarkAllocations_Send(b *testing.B) {
	instance := newBenchInstance(b)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		orderID := fmt.Sprintf("order-%d", i)
		cmd := mocks.CreateOrderCmd{ID: orderID, Total: 100.0}
		instance.Send(context.Background(), cmd)
	}
}

func BenchmarkAllocations_Subscribe(b *testing.B) {
	instance := newBenchInstance(b)
	handler := func(ctx context.Context, evt models.Event[mocks.Order]) {
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id, _ := instance.Subscribe("OrderCreated", handler)
		instance.Unsubscribe(id)
	}
}

func BenchmarkAllocations_Get(b *testing.B) {
	instance := newBenchInstance(b)
	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	instance.Send(context.Background(), cmd)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		instance.Get(context.Background(), "order-1")
	}
}
