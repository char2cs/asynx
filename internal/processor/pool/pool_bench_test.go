package pool_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/bus/dispatcher"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor/exec"
	"github.com/char2cs/asynx/internal/processor/models"
	"github.com/char2cs/asynx/internal/processor/pool"
	"github.com/char2cs/asynx/store"
)

func BenchmarkPool_Send_SingleShard(b *testing.B) {
	s := store.New()
	bu := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	d := dispatcher.New[order](bu)
	defer d.Close(context.Background())
	executor := exec.New(es, d)

	p := pool.New(executor, 1, 0, 1)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		envelope := &models.CommandEnvelope[order]{
			Cmd:        mocks.CreateOrderCmd{ID: "order", Total: 100.0},
			Ctx:        ctx,
			ResultChan: make(chan models.CommandResult[order], 1),
		}

		shard.CommandChan() <- envelope
		<-envelope.ResultChan
	}
}

func BenchmarkPool_Send_MultiShard(b *testing.B) {
	for _, shards := range []int{1, 4, 8, 16} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			s := store.New()
			bu := bus.NewChannelBus[order]()
			es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
			d := dispatcher.New[order](bu)
			defer d.Close(context.Background())
			executor := exec.New(es, d)

			p := pool.New(executor, shards, 0, 1)
			defer p.Drain(context.Background())

			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				idx := b.N % shards
				shard := p.Shards()[idx]

				envelope := &models.CommandEnvelope[order]{
					Cmd:        mocks.CreateOrderCmd{ID: "order", Total: 100.0},
					Ctx:        ctx,
					ResultChan: make(chan models.CommandResult[order], 1),
				}

				shard.CommandChan() <- envelope
				<-envelope.ResultChan
			}
		})
	}
}

func BenchmarkPool_Send_Parallel(b *testing.B) {
	s := store.New()
	bu := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	d := dispatcher.New[order](bu)
	defer d.Close(context.Background())
	executor := exec.New(es, d)

	p := pool.New(executor, 8, 0, 1)
	defer p.Drain(context.Background())

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			shard := p.Shards()[idx%8]
			idx++

			envelope := &models.CommandEnvelope[order]{
				Cmd:        mocks.CreateOrderCmd{ID: "order", Total: 100.0},
				Ctx:        ctx,
				ResultChan: make(chan models.CommandResult[order], 1),
			}

			shard.CommandChan() <- envelope
			<-envelope.ResultChan
		}
	})
}
