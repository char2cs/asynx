package pool_test

import (
	"context"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/bus/dispatcher"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor/exec"
	"github.com/char2cs/asynx/internal/processor/models"
	"github.com/char2cs/asynx/internal/processor/pool"
	"github.com/char2cs/asynx/store"
	asynxmd "github.com/char2cs/asynx/models"
)

func TestShard_SingleCommandSuccess(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	d := dispatcher.New[order](b)
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(es, d)

	p := pool.New(executor, 1, 0, 1)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	envelope := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[order], 1),
	}

	shard.CommandChan() <- envelope

	result := <-envelope.ResultChan
	if result.Err != nil {
		t.Fatalf("expected nil, got %v", result.Err)
	}
}

func TestShard_ValidationErrorDecrementVersionSingleWorker(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	d := dispatcher.New[order](b)
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(es, d)

	p := pool.New(executor, 1, 0, 1)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	envelope1 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[order], 1),
	}
	shard.CommandChan() <- envelope1
	<-envelope1.ResultChan

	envelope2 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CancelOrderCmd{ID: "order1"},
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[order], 1),
	}
	shard.CommandChan() <- envelope2
	<-envelope2.ResultChan

	envelope3 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CancelOrderCmd{ID: "order2"},
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[order], 1),
	}
	shard.CommandChan() <- envelope3
	result3 := <-envelope3.ResultChan
	if result3.Err != asynxmd.ErrValidation {
		t.Fatalf("expected ErrValidation, got %v", result3.Err)
	}

	envelope4 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order3", Total: 200.0},
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[order], 1),
	}
	shard.CommandChan() <- envelope4
	result4 := <-envelope4.ResultChan
	if result4.Err != nil {
		t.Fatalf("create after validation error failed: %v", result4.Err)
	}
}

func TestShard_ContextCancelledDuringExecution(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	d := dispatcher.New[order](b)
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(es, d)

	p := pool.New(executor, 1, 0, 1)
	defer p.Drain(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	shard := p.Shards()[0]

	envelope := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[order], 1),
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
	d := dispatcher.New[order](b)
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(es, d)

	p := pool.New(executor, 1, 0, 2)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	envelope1 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[order], 1),
	}
	shard.CommandChan() <- envelope1
	<-envelope1.ResultChan

	envelope2 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CancelOrderCmd{ID: "order2"},
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[order], 1),
	}
	shard.CommandChan() <- envelope2
	result2 := <-envelope2.ResultChan
	if result2.Err != asynxmd.ErrValidation {
		t.Fatalf("expected ErrValidation, got %v", result2.Err)
	}

	envelope3 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order3", Total: 100.0},
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[order], 1),
	}
	shard.CommandChan() <- envelope3
	result3 := <-envelope3.ResultChan
	if result3.Err != nil {
		t.Fatalf("create after failed validation failed: %v", result3.Err)
	}
}

func TestShard_ResultCarriesEvent(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	d := dispatcher.New[order](b)
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(es, d)

	p := pool.New(executor, 1, 0, 1)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	envelope := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order-evt", Total: 50.0},
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[order], 1),
	}
	shard.CommandChan() <- envelope

	result := <-envelope.ResultChan
	if result.Err != nil {
		t.Fatalf("expected nil error, got %v", result.Err)
	}
	if result.Event.AggregateID != "order-evt" {
		t.Errorf("expected AggregateID order-evt, got %q", result.Event.AggregateID)
	}
	if result.Event.EventName == "" {
		t.Error("expected non-empty EventName in result")
	}
}

func TestShard_ResultUsesCommandResult(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	d := dispatcher.New[order](b)
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(es, d)

	p := pool.New(executor, 1, 0, 1)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	envelope := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order-result", Total: 50.0},
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[order], 1),
	}
	shard.CommandChan() <- envelope

	result := <-envelope.ResultChan
	if result.Err != nil {
		t.Fatalf("expected nil error, got %v", result.Err)
	}
}

// TestShard_NilEnvelopeClosesJobQueue verifies that sending nil on commandChan
// triggers the defensive nil-guard in handleDispatch, which closes the jobQueue
// and exits the dispatcher loop gracefully.
func TestShard_NilEnvelopeClosesJobQueue(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	d := dispatcher.New[order](b)
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(es, d)

	p := pool.New(executor, 1, 0, 1)

	shard := p.Shards()[0]

	// Send nil envelope — triggers handleDispatch's nil guard, closes jobQueue,
	// and causes the dispatcher and workers to exit.
	shard.CommandChan() <- nil

	// Drain should complete quickly since the dispatcher and workers have exited.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := p.Drain(ctx); err != nil {
		t.Fatalf("Drain after nil envelope failed: %v", err)
	}
}
