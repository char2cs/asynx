package bus_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/internal/bus"
)

// noop is a zero-cost handler used to isolate publish-path overhead.
var noop = func(_ context.Context, _ models.Event[string]) {}

// BenchmarkPublish_ExactMatch measures Publish throughput when the event name
// exactly matches the subscription pattern, varying the total number of
// subscriptions. This directly benchmarks the subscription index lookup.
func BenchmarkPublish_ExactMatch(b *testing.B) {
	for _, n := range []int{1, 10, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("%d_subs", n), func(b *testing.B) {
			cb := bus.NewChannelBus[string]()
			b.Cleanup(func() { cb.Close(context.Background()) })

			for i := range n {
				cb.Subscribe(fmt.Sprintf("Event%d", i), noop)
			}

			event := models.Event[string]{EventName: "Event0"}
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()

			for range b.N {
				cb.Publish(ctx, event)
			}
			b.StopTimer()
		})
	}
}

// BenchmarkPublish_NoMatch measures the cost of publishing an event that
// matches no subscription — worst case for a full linear scan, best case
// for an indexed lookup.
func BenchmarkPublish_NoMatch(b *testing.B) {
	for _, n := range []int{1, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("%d_subs", n), func(b *testing.B) {
			cb := bus.NewChannelBus[string]()
			b.Cleanup(func() { cb.Close(context.Background()) })

			for i := range n {
				cb.Subscribe(fmt.Sprintf("Event%d", i), noop)
			}

			event := models.Event[string]{EventName: "NoMatch"}
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()

			for range b.N {
				cb.Publish(ctx, event)
			}
			b.StopTimer()
		})
	}
}

// BenchmarkPublish_Regex measures Publish when patterns are regex, requiring
// compilation (first publish) and matching (subsequent publishes).
func BenchmarkPublish_Regex(b *testing.B) {
	for _, n := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("%d_patterns", n), func(b *testing.B) {
			cb := bus.NewChannelBus[string]()
			b.Cleanup(func() { cb.Close(context.Background()) })

			for i := range n {
				cb.Subscribe(fmt.Sprintf("^Event%d.*", i), noop)
			}

			event := models.Event[string]{EventName: "Event0Created"}
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()

			for range b.N {
				cb.Publish(ctx, event)
			}
			b.StopTimer()
		})
	}
}

// BenchmarkPublish_Fanout measures the cost of dispatching to many matching
// handlers for a single event (goroutine spawn overhead dominates here).
func BenchmarkPublish_Fanout(b *testing.B) {
	for _, n := range []int{1, 10, 50, 100} {
		b.Run(fmt.Sprintf("%d_handlers", n), func(b *testing.B) {
			cb := bus.NewChannelBus[string]()
			b.Cleanup(func() { cb.Close(context.Background()) })

			for range n {
				cb.Subscribe("OrderPlaced", noop)
			}

			event := models.Event[string]{EventName: "OrderPlaced"}
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()

			for range b.N {
				cb.Publish(ctx, event)
			}
			b.StopTimer()
		})
	}
}

// BenchmarkPublish_Parallel measures sustained throughput with multiple
// concurrent publishers (simulates real-world multi-goroutine producers).
func BenchmarkPublish_Parallel(b *testing.B) {
	cb := bus.NewChannelBus[string]()
	b.Cleanup(func() { cb.Close(context.Background()) })

	cb.Subscribe("OrderPlaced", noop)

	event := models.Event[string]{EventName: "OrderPlaced"}
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cb.Publish(ctx, event)
		}
	})
	b.StopTimer()
}

// BenchmarkPublish_MixedSubs benchmarks the realistic case: many exact-match
// subscriptions plus a few regex patterns.
func BenchmarkPublish_MixedSubs(b *testing.B) {
	for _, exact := range []int{100, 1_000} {
		for _, regex := range []int{5, 20} {
			b.Run(fmt.Sprintf("%d_exact_%d_regex", exact, regex), func(b *testing.B) {
				cb := bus.NewChannelBus[string]()
				b.Cleanup(func() { cb.Close(context.Background()) })

				for i := range exact {
					cb.Subscribe(fmt.Sprintf("Exact%d", i), noop)
				}
				for i := range regex {
					cb.Subscribe(fmt.Sprintf("^Regex%d.*", i), noop)
				}

				event := models.Event[string]{EventName: "Exact0"}
				ctx := context.Background()

				b.ResetTimer()
				b.ReportAllocs()

				for range b.N {
					cb.Publish(ctx, event)
				}
				b.StopTimer()
			})
		}
	}
}

// BenchmarkSubscribe measures Subscribe throughput.
func BenchmarkSubscribe(b *testing.B) {
	cb := bus.NewChannelBus[string]()
	b.Cleanup(func() { cb.Close(context.Background()) })

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		cb.Subscribe("OrderPlaced", noop)
	}
}

// BenchmarkSubscribe_Regex measures Subscribe throughput for regex patterns,
// which take the slice-append COW path instead of the map-copy path.
func BenchmarkSubscribe_Regex(b *testing.B) {
	cb := bus.NewChannelBus[string]()
	b.Cleanup(func() { cb.Close(context.Background()) })

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		cb.Subscribe("^Order.*", noop)
	}
}

// BenchmarkUnsubscribe measures Unsubscribe throughput.
func BenchmarkUnsubscribe(b *testing.B) {
	cb := bus.NewChannelBus[string]()
	b.Cleanup(func() { cb.Close(context.Background()) })

	ids := make([]string, b.N)
	for i := range b.N {
		ids[i], _ = cb.Subscribe("OrderPlaced", noop)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		cb.Unsubscribe(ids[i])
	}
}

// BenchmarkPublish_Timeout measures the overhead of handler timeout machinery
// (time.NewTimer vs time.After).
func BenchmarkPublish_Timeout(b *testing.B) {
	cb := bus.NewChannelBus[string]()
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cb.Close(ctx)
	})

	cb.Subscribe("OrderPlaced", noop, models.WithHandlerTimeout[string](time.Second))

	event := models.Event[string]{EventName: "OrderPlaced"}
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		cb.Publish(ctx, event)
	}
	b.StopTimer()
}
