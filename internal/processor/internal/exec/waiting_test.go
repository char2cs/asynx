package exec

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
	"sync/atomic"
	"testing"
)

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

	var handlerDone atomic.Bool
	markDone := func() {
		handlerDone.Store(true)
	}
	callbackBus := &recordingBus[order]{
		onPublishSync: markDone,
	}
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: callbackBus})
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

	if !handlerDone.Load() {
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
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: trackBus})
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
	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	block := func(
		context.Context,
		asynxmd.Event[order],
	) {
		close(entered)
		<-release
	}
	b.Subscribe(
		"OrderUpdated",
		block,
	)

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		evt asynxmd.Event[order]
		err error
	}
	got := make(
		chan result,
		1,
	)
	run := func() {
		evt, err := ex.ExecuteWait(
			ctx,
			update(
				"agg",
				1,
			),
		)
		got <- result{evt, err}
	}
	go run()
	<-entered
	cancel()
	r := <-got
	if !errors.Is(
		r.err,
		asynxmd.ErrContextCancelled,
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
	succeed := func(
		context.Context,
		asynxmd.Event[order],
	) error {
		return nil
	}
	ex := durableEnv(
		t,
		succeed,
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
		asynxmd.ErrDeliveryParked,
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
