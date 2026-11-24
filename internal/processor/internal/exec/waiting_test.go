package exec

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/v2/internal/eventstore"
	"github.com/char2cs/asynx/v2/internal/mocks"
	"github.com/char2cs/asynx/v2/internal/processor/internal/dispatcher"
	dispatchermodel "github.com/char2cs/asynx/v2/models/dispatcher"
	"github.com/char2cs/asynx/v2/models/event"
	"github.com/char2cs/asynx/v2/models/failure"
	"github.com/char2cs/asynx/v2/store"
	"sync/atomic"
	"testing"
)

type handlerFlag struct {
	done atomic.Bool
}

func (f *handlerFlag) markDone() {
	f.done.Store(true)
}

type waitedResult struct {
	evt event.Event[order]
	err error
}

type waitingCaller struct {
	executor *CommandExecutor[order]
	got      chan waitedResult
}

func (c *waitingCaller) run(
	ctx context.Context,
) {
	evt, err := c.executor.ExecuteWait(
		ctx,
		update(
			"agg",
			1,
		),
	)
	c.got <- waitedResult{evt, err}
}

func TestExecute_WaitHandlersTrue_HandlersDoneBeforeReturn(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)

	handler := &handlerFlag{}
	callbackBus := &recordingBus[order]{
		onPublishSync: handler.markDone,
	}
	d := dispatcher.Dispatcher(dispatchermodel.Config[order]{Bus: callbackBus})
	t.Cleanup(func() { d.Close(context.Background()) })

	executor := New(
		es,
		d,
	)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "wait-order", Total: 10.0}

	_, err := executor.ExecuteWait(
		ctx,
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"expected nil, got %v",
			err,
		)
	}

	if !handler.done.Load() {
		t.Error("Execute with waitHandlers=true returned before PublishSync completed")
	}
}

func TestExecute_WaitHandlersFalse_DispatchCalled(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	trackBus := &recordingBus[order]{}
	d := dispatcher.Dispatcher(dispatchermodel.Config[order]{Bus: trackBus})
	t.Cleanup(func() { d.Close(context.Background()) })

	executor := New(
		es,
		d,
	)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "async-order", Total: 20.0}

	_, err := executor.Execute(
		ctx,
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"expected nil, got %v",
			err,
		)
	}

	d.WaitIdle()

	trackBus.mu.Lock()
	defer trackBus.mu.Unlock()
	if trackBus.count == 0 {
		t.Fatal("dispatch was not called")
	}
}

func TestExecute_WaitHandlers_NilDispatcherNoPanic(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	executor := New(
		es,
		nil,
	)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "nil-bus-order", Total: 5.0}

	event, err := executor.ExecuteWait(
		ctx,
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"expected nil, got %v",
			err,
		)
	}

	if event.AggregateID != "nil-bus-order" {
		t.Errorf(
			"expected AggregateID nil-bus-order, got %q",
			event.AggregateID,
		)
	}
}

func TestExecute_WaitingForHandlersGivesUpWithTheCommittedEvent(
	t *testing.T,
) {
	ex, _, _, _, b := newEnv(t)
	gate := newHandlerGate()
	t.Cleanup(func() { close(gate.release) })
	b.Subscribe(
		"OrderUpdated",
		gate.hold,
	)

	ctx, cancel := context.WithCancel(context.Background())
	caller := &waitingCaller{
		executor: ex,
		got: make(
			chan waitedResult,
			1,
		),
	}
	go caller.run(ctx)
	<-gate.entered
	cancel()
	r := <-caller.got
	if !errors.Is(
		r.err,
		failure.ErrContextCancelled,
	) ||
		r.evt.Version != 1 {
		t.Fatalf(
			"got (%+v, %v), want the committed v1 with ErrContextCancelled",
			r.evt,
			r.err,
		)
	}
}

func TestExecute_WaitingOnAParkedAggregateReportsNoHandlerRan(
	t *testing.T,
) {
	ex := durableEnv(
		t,
		acceptsEveryEvent,
	)
	ex.dispatcher.Park(
		"agg",
		errors.New("parked by the test"),
	)

	evt, err := ex.ExecuteWait(
		context.Background(),
		update(
			"agg",
			1,
		),
	)
	if !errors.Is(
		err,
		failure.ErrDeliveryParked,
	) ||
		evt.Version != 1 {
		t.Fatalf(
			"Execute = v%d, %v; want the committed v1 with ErrDeliveryParked",
			evt.Version,
			err,
		)
	}

	if _, err := ex.Execute(
		context.Background(),
		update(
			"agg",
			2,
		),
	); err != nil {
		t.Fatalf(
			"Execute without waiting: %v",
			err,
		)
	}
}
