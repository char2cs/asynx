package processor_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor"
	"github.com/char2cs/asynx/store"
	asynxmd "github.com/char2cs/asynx/models"
)

func BenchmarkSend_MultiShard(b *testing.B) {
	for _, shards := range []int{1, 4, 8, 16} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			memStore := store.New()
			channelBus := bus.NewChannelBus[order]()
			es := eventstore.New[order](memStore, memStore, nil, 1, nil)

			p := processor.New(es, channelBus, processor.WithShards[order](shards))
			defer p.Shutdown(context.Background())

			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				cmd := mocks.CreateOrderCmd{ID: "order", Total: 100.0}
				p.Send(ctx, cmd)
			}
		})
	}
}

func BenchmarkSend_Parallel(b *testing.B) {
	memStore := store.New()
	channelBus := bus.NewChannelBus[order]()
	es := eventstore.New[order](memStore, memStore, nil, 1, nil)

	p := processor.New(es, channelBus, processor.WithShards[order](8))
	defer p.Shutdown(context.Background())

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cmd := mocks.CreateOrderCmd{ID: "order", Total: 100.0}
			p.Send(ctx, cmd)
		}
	})
}

func BenchmarkSend_WithBusFanout(b *testing.B) {
	memStore := store.New()
	channelBus := bus.NewChannelBus[order]()
	es := eventstore.New[order](memStore, memStore, nil, 1, nil)

	// Subscribe 10 handlers to the bus
	for i := 0; i < 10; i++ {
		channelBus.Subscribe("OrderCreated", func(ctx context.Context, e asynxmd.Event[order]) {
			// Handler does nothing
		})
	}

	p := processor.New(es, channelBus)
	defer p.Shutdown(context.Background())

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		cmd := mocks.CreateOrderCmd{ID: "order", Total: 100.0}
		p.Send(ctx, cmd)
	}
}
