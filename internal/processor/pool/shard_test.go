package pool_test

import (
	"context"
	"errors"
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
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
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
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
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
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
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
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
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
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
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
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
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
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
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

func TestShard_WaitingForHandlersDoesNotHoldTheWorker(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	release := make(chan struct{})
	if _, err := b.Subscribe("OrderCreated", func(context.Context, asynxmd.Event[order]) {
		<-release
	}); err != nil {
		t.Fatal(err)
	}
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	d := dispatcher.New[order](b)
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(es, d)

	p := pool.New(executor, 1, 0, 1) // one shard, one worker
	defer p.Drain(context.Background())

	send := func(id string) *models.CommandEnvelope[order] {
		env := &models.CommandEnvelope[order]{
			Cmd:          mocks.CreateOrderCmd{ID: id, Total: 1},
			Ctx:          context.Background(),
			ResultChan:   make(chan models.CommandResult[order], 1),
			WaitHandlers: true,
		}
		p.Shards()[0].CommandChan() <- env
		return env
	}

	first := send("order1")
	select {
	case res := <-first.ResultChan:
		if res.Err != nil || res.Wait == nil {
			t.Fatalf("result = %+v, want committed with a Wait", res)
		}
		short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		if err := res.Wait(short); !errors.Is(err, asynxmd.ErrContextCancelled) {
			t.Fatalf("Wait returned %v while the handler is still blocked", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker is held by a blocked handler: first result never arrived")
	}

	// The single worker is free: another aggregate commits while the handler blocks.
	second := send("order2")
	select {
	case res := <-second.ResultChan:
		if res.Err != nil {
			t.Fatal(res.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second aggregate stalled behind the first one's handler")
	}
	close(release)
}

// Envelopes still queued when the pool stops are answered, not abandoned.
func TestPool_Drain_AnswersEnvelopesLeftInTheQueue(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	executor := exec.New[order](es, nil)
	p := pool.New(executor, 1, 8, 1)

	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	send := func(cmd asynxmd.Command[order]) *models.CommandEnvelope[order] {
		env := &models.CommandEnvelope[order]{Cmd: cmd, Ctx: context.Background(), ResultChan: make(chan models.CommandResult[order], 1)}
		p.Shards()[0].CommandChan() <- env
		return env
	}
	var envs []*models.CommandEnvelope[order]
	envs = append(envs, send(gatedUpdate{UpdateOrderCmd: mocks.UpdateOrderCmd{ID: "a", NewState: order{ID: "a"}}, entered: entered, gate: gate}))
	<-entered
	for i := range 5 {
		id := string(rune('b' + i))
		envs = append(envs, send(mocks.UpdateOrderCmd{ID: id, NewState: order{ID: id}}))
	}

	drained := make(chan error, 1)
	go func() { drained <- p.Drain(context.Background()) }()
	time.Sleep(10 * time.Millisecond) // let the stop signal land
	close(gate)

	for i, env := range envs {
		select {
		case <-env.ResultChan:
		case <-time.After(5 * time.Second):
			t.Fatalf("envelope %d never got a result", i)
		}
	}
	if err := <-drained; err != nil {
		t.Fatalf("Drain: %v", err)
	}
}

type gatedUpdate struct {
	mocks.UpdateOrderCmd
	entered chan<- struct{}
	gate    <-chan struct{}
}

func (c gatedUpdate) Validate(*order) error {
	c.entered <- struct{}{}
	<-c.gate
	return nil
}
