package exec

import (
	"context"
	"testing"

	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/store"
)

func BenchmarkExecute_CreateNew(b *testing.B) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()

	for _, numEvents := range []int{1, 100, 1000} {
		b.Run("events="+string(rune('0'+numEvents/100))+"_000", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				cmd := mocks.CreateOrderCmd{ID: "order" + string(rune(b.N)), Total: 100.0}
				_ = executor.Execute(ctx, cmd, 1)
			}
		})
	}
}

func BenchmarkExecute_UpdateExisting(b *testing.B) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()

	// Pre-populate with an aggregate
	createCmd := mocks.CreateOrderCmd{ID: "order_base", Total: 100.0}
	executor.Execute(ctx, createCmd, 1)

	for _, numEvents := range []int{1, 100, 1000} {
		b.Run("events="+string(rune('0'+numEvents/100))+"_000", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				cmd := mocks.UpdateOrderCmd{
					ID:       "order_base",
					NewState: order{ID: "order_base", Total: 150.0, Status: "Confirmed"},
				}
				_ = executor.Execute(ctx, cmd, int64(b.N))
			}
		})
	}
}
