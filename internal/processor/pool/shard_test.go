package pool_test

import (
	"context"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor/exec"
	"github.com/char2cs/asynx/internal/processor/models"
	"github.com/char2cs/asynx/internal/processor/pool"
	"github.com/char2cs/asynx/internal/store"
	asynxmd "github.com/char2cs/asynx/models"
)

func TestShard_SingleCommandSuccess(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := exec.New(es, b)

	p := pool.New(executor, 1, 0, 1)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	envelope := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
		Ctx:        ctx,
		ResultChan: make(chan error, 1),
	}

	shard.CommandChan() <- envelope

	err := <-envelope.ResultChan
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestShard_ValidationErrorDecrementVersionSingleWorker(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := exec.New(es, b)

	p := pool.New(executor, 1, 0, 1)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	envelope1 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
		Ctx:        ctx,
		ResultChan: make(chan error, 1),
	}
	shard.CommandChan() <- envelope1
	<-envelope1.ResultChan

	envelope2 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CancelOrderCmd{ID: "order1"},
		Ctx:        ctx,
		ResultChan: make(chan error, 1),
	}
	shard.CommandChan() <- envelope2
	<-envelope2.ResultChan

	envelope3 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CancelOrderCmd{ID: "order2"},
		Ctx:        ctx,
		ResultChan: make(chan error, 1),
	}
	shard.CommandChan() <- envelope3
	err := <-envelope3.ResultChan
	if err != asynxmd.ErrValidation {
		t.Fatalf("expected ErrValidation, got %v", err)
	}

	envelope4 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order3", Total: 200.0},
		Ctx:        ctx,
		ResultChan: make(chan error, 1),
	}
	shard.CommandChan() <- envelope4
	err = <-envelope4.ResultChan
	if err != nil {
		t.Fatalf("create after validation error failed: %v", err)
	}
}

func TestShard_ContextCancelledDuringExecution(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := exec.New(es, b)

	p := pool.New(executor, 1, 0, 1)
	defer p.Drain(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	shard := p.Shards()[0]

	envelope := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
		Ctx:        ctx,
		ResultChan: make(chan error, 1),
	}

	shard.CommandChan() <- envelope
	cancel()

	select {
	case <-envelope.ResultChan:
	case <-time.After(2 * time.Second):
		t.Fatalf("result not received within timeout")
	}
}

func TestShard_MultipleWorkersNoCorrection(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := exec.New(es, b)

	p := pool.New(executor, 1, 0, 2)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	envelope1 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
		Ctx:        ctx,
		ResultChan: make(chan error, 1),
	}
	shard.CommandChan() <- envelope1
	<-envelope1.ResultChan

	envelope2 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CancelOrderCmd{ID: "order2"},
		Ctx:        ctx,
		ResultChan: make(chan error, 1),
	}
	shard.CommandChan() <- envelope2
	err := <-envelope2.ResultChan
	if err != asynxmd.ErrValidation {
		t.Fatalf("expected ErrValidation, got %v", err)
	}

	envelope3 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order3", Total: 100.0},
		Ctx:        ctx,
		ResultChan: make(chan error, 1),
	}
	shard.CommandChan() <- envelope3
	err = <-envelope3.ResultChan
	if err != nil {
		t.Fatalf("create after failed validation failed: %v", err)
	}
}
