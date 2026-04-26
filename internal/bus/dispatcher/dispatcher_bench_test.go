package dispatcher

import (
	"context"
	"fmt"
	"testing"

	asynxmd "github.com/char2cs/asynx/models"
)

// noopBus is a zero-cost Bus[T] implementation for benchmarking.
type noopBus[T any] struct{}

func (b *noopBus[T]) Publish(_ context.Context, _ asynxmd.Event[T]) error { return nil }
func (b *noopBus[T]) PublishSync(_ context.Context, _ asynxmd.Event[T]) error { return nil }
func (b *noopBus[T]) Subscribe(_ string, _ asynxmd.ProjectionHandler[T], _ ...asynxmd.SubscriptionOpt[T]) (string, error) {
	return "", nil
}
func (b *noopBus[T]) Unsubscribe(_ string) error        { return nil }
func (b *noopBus[T]) Close(_ context.Context) error      { return nil }
func (b *noopBus[T]) WaitForHandlers()                   {}

// BenchmarkDispatch_SingleAggregate benchmarks async dispatch where all events
// target a single aggregate "agg1".
func BenchmarkDispatch_SingleAggregate(b *testing.B) {
	d := New[string](&noopBus[string]{})
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = d.Dispatch(ctx, asynxmd.Event[string]{
			AggregateID: "agg1",
			EventName:   "BenchEvent",
			Version:     int64(i),
		}, false)
	}

	b.StopTimer()
	_ = d.Close(context.Background())
}

// BenchmarkDispatch_MultiAggregate benchmarks async dispatch spread across
// varying numbers of aggregates (10, 100, 1000).
func BenchmarkDispatch_MultiAggregate(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		n := n
		b.Run(fmt.Sprintf("aggs=%d", n), func(b *testing.B) {
			d := New[string](&noopBus[string]{})
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				aggID := fmt.Sprintf("agg%d", i%n)
				_ = d.Dispatch(ctx, asynxmd.Event[string]{
					AggregateID: aggID,
					EventName:   "BenchEvent",
					Version:     int64(i),
				}, false)
			}

			b.StopTimer()
			_ = d.Close(context.Background())
		})
	}
}

// BenchmarkDispatch_Parallel benchmarks concurrent async dispatch to "agg1"
// using RunParallel.
func BenchmarkDispatch_Parallel(b *testing.B) {
	d := New[string](&noopBus[string]{})
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			_ = d.Dispatch(ctx, asynxmd.Event[string]{
				AggregateID: "agg1",
				EventName:   "BenchEvent",
				Version:     i,
			}, false)
			i++
		}
	})

	b.StopTimer()
	_ = d.Close(context.Background())
}

// BenchmarkDispatch_Sync benchmarks synchronous dispatch (waitHandlers=true)
// where all events target "agg1".
func BenchmarkDispatch_Sync(b *testing.B) {
	d := New[string](&noopBus[string]{})
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = d.Dispatch(ctx, asynxmd.Event[string]{
			AggregateID: "agg1",
			EventName:   "BenchEvent",
			Version:     int64(i),
		}, true)
	}

	b.StopTimer()
	_ = d.Close(context.Background())
}
