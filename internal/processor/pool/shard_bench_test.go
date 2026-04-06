package pool_test

import (
	"context"
	"testing"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor/exec"
	"github.com/char2cs/asynx/internal/processor/models"
	"github.com/char2cs/asynx/internal/processor/pool"
	"github.com/char2cs/asynx/internal/store"
)

func BenchmarkShard_SingleWorker(b *testing.B) {
	s := store.New()
	bu := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := exec.New(es, bu)

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

func BenchmarkShard_MultipleWorkers(b *testing.B) {
	for _, workers := range []int{1, 4, 8} {
		b.Run("workers="+string(rune('0'+workers)), func(b *testing.B) {
			s := store.New()
			bu := bus.NewChannelBus[order]()
			es := eventstore.New[order](s, s, nil, 1, nil)
			executor := exec.New(es, bu)

			p := pool.New(executor, 1, 0, workers)
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
		})
	}
}
