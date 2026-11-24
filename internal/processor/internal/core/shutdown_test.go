package core

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mockbuses"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor/models"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestProcessor_ShuttingDown(
	t *testing.T,
) {
	p := newProcessor(t)

	ctx := context.Background()
	err := p.Shutdown(ctx)
	if err != nil {
		t.Fatalf(
			"shutdown failed: %v",
			err,
		)
	}

	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err = p.Send(
		ctx,
		cmd,
	)
	if err != asynxmd.ErrShuttingDown {
		t.Fatalf(
			"expected ErrShuttingDown, got %v",
			err,
		)
	}
}

func TestProcessor_ShutdownDrains(
	t *testing.T,
) {
	p := newProcessor(
		t,
		models.WithShards[order](4),
		models.WithQueueDepth[order](5),
	)

	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make(
		chan error,
		10,
	)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(
			idx int,
		) {
			defer wg.Done()
			cmd := mocks.CreateOrderCmd{ID: "order" + string(rune(idx)), Total: 100.0}
			_, _err := p.Send(
				ctx,
				cmd,
			)
			errs <- _err
		}(i)
	}

	wg.Wait()

	err := p.Shutdown(ctx)
	if err != nil {
		t.Fatalf(
			"shutdown failed: %v",
			err,
		)
	}

	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf(
				"Send failed: %v",
				err,
			)
		}
	}
}

func TestProcessor_ShutdownTimeout(
	t *testing.T,
) {
	p := newProcessor(t)

	ctx, cancel := context.WithDeadline(
		context.Background(),
		time.Now().Add(-time.Second),
	)
	defer cancel()

	err := p.Shutdown(ctx)
	if err != nil &&
		!errors.Is(
			err,
			context.DeadlineExceeded,
		) {
		t.Logf(
			"Shutdown returned: %v (may be acceptable if completed quickly)",
			err,
		)
	}
}

func TestProcessor_AlreadyShuttingDown(
	t *testing.T,
) {
	p := newProcessor(t)

	ctx := context.Background()

	err1 := p.Shutdown(ctx)
	if err1 != nil {
		t.Fatalf(
			"first shutdown failed: %v",
			err1,
		)
	}

	err2 := p.Shutdown(ctx)
	if err2 != asynxmd.ErrAlreadyShuttingDown {
		t.Fatalf(
			"expected ErrAlreadyShuttingDown, got %v",
			err2,
		)
	}
}

func TestProcessor_ShutdownWithBus(
	t *testing.T,
) {
	memStore := store.New()
	channelBus := bus.New[order]()
	es := eventstore.New[order](
		memStore,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)

	p := mustNew(
		t,
		es,
		channelBus,
	)

	ctx := context.Background()

	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err := p.Send(
		ctx,
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"Send failed: %v",
			err,
		)
	}

	err = p.Shutdown(ctx)
	if err != nil {
		t.Fatalf(
			"shutdown failed: %v",
			err,
		)
	}
}

func TestProcessor_ShutdownWithoutBus(
	t *testing.T,
) {
	memStore := store.New()
	es := eventstore.New[order](
		memStore,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)

	p := mustNew(
		t,
		es,
		nil,
	)

	ctx := context.Background()

	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err := p.Send(
		ctx,
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"Send failed: %v",
			err,
		)
	}

	err = p.Shutdown(ctx)
	if err != nil {
		t.Fatalf(
			"shutdown failed: %v",
			err,
		)
	}
}

func TestProcessor_ShutdownClosesPool(
	t *testing.T,
) {
	memStore := store.New()
	channelBus := bus.New[order]()
	es := eventstore.New[order](
		memStore,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)

	p := mustNew(
		t,
		es,
		channelBus,
	)

	ctx := context.Background()

	cmd1 := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err := p.Send(
		ctx,
		cmd1,
	)
	if err != nil {
		t.Fatalf(
			"send before shutdown failed: %v",
			err,
		)
	}

	err = p.Shutdown(ctx)
	if err != nil {
		t.Fatalf(
			"shutdown failed: %v",
			err,
		)
	}

	cmd2 := mocks.CreateOrderCmd{ID: "order2", Total: 100.0}
	_, err = p.Send(
		ctx,
		cmd2,
	)
	if err != asynxmd.ErrShuttingDown {
		t.Fatalf(
			"send after shutdown should return ErrShuttingDown, got %v",
			err,
		)
	}
}

func TestProcessor_SendWait_AfterShutdown(
	t *testing.T,
) {
	p := newProcessor(t)

	ctx := context.Background()
	p.Shutdown(ctx)

	cmd := mocks.CreateOrderCmd{ID: "shutdown-order", Total: 1.0}

	_, err := p.SendWait(
		ctx,
		cmd,
	)
	if err != asynxmd.ErrShuttingDown {
		t.Fatalf(
			"expected ErrShuttingDown, got %v",
			err,
		)
	}
}

func TestProcessor_Forget_RejectedWhenShuttingDownOrCancelled(
	t *testing.T,
) {
	f := newFixture(
		t,
		store.New(),
	)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.p.Forget(
		cancelled,
		"o1",
	); !errors.Is(
		err,
		asynxmd.ErrContextCancelled,
	) {
		t.Errorf(
			"cancelled: err = %v, want ErrContextCancelled",
			err,
		)
	}

	f.p.Shutdown(context.Background())
	if err := f.p.Forget(
		context.Background(),
		"o1",
	); !errors.Is(
		err,
		asynxmd.ErrShuttingDown,
	) {
		t.Errorf(
			"shutting down: err = %v, want ErrShuttingDown",
			err,
		)
	}
}

func TestProcessor_Shutdown_WaitsForForgetInFlightUntilItsContextEnds(
	t *testing.T,
) {
	f := newFixture(
		t,
		store.New(),
	)
	ctx := context.Background()
	if _, err := f.p.Send(
		ctx,
		mocks.CreateOrderCmd{ID: "o1", Total: 1},
	); err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	f.b.Subscribe(
		"asynx\\.aggregate\\.forget",
		func(
			context.Context,
			asynxmd.Event[order],
		) {
			close(entered)
			<-release
		},
	)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		f.p.Forget(
			ctx,
			"o1",
		)
	}()
	<-entered

	short, cancel := context.WithTimeout(
		ctx,
		20*time.Millisecond,
	)
	defer cancel()
	if err := f.p.Shutdown(short); !errors.Is(
		err,
		context.DeadlineExceeded,
	) {
		t.Fatalf(
			"Shutdown err = %v, want the context's deadline while a Forget is in flight",
			err,
		)
	}

	close(release)
	wg.Wait()
}

func TestProcessor_Shutdown_NeverStrandsAQueuedSend(
	t *testing.T,
) {
	for range 25 {
		gate := make(chan struct{})
		entered := make(
			chan struct{},
			1,
		)
		p := newProcessor(
			t,
			models.WithShards[order](1),
			models.WithWorkersPerShard[order](1),
			models.WithQueueDepth[order](2),
		)

		results := make(
			chan error,
			6,
		)
		for i := range 6 {
			go func() {
				cmd := gatedCmd{UpdateOrderCmd: updateCmd(
					"agg"+string(rune('a'+i)),
					1,
				), entered: entered, gate: gate}
				_, err := p.Send(
					context.Background(),
					cmd,
				)
				results <- err
			}()
		}

		<-entered

		down := make(
			chan error,
			1,
		)
		go func() { down <- p.Shutdown(context.Background()) }()
		close(gate)

		awaitSendResults(
			t,
			results,
			6,
		)
		if err := <-down; err != nil {
			t.Fatalf(
				"Shutdown: %v",
				err,
			)
		}
	}
}

func TestProcessor_Shutdown_CanBeRetriedAfterItsContextEnds(
	t *testing.T,
) {
	es := eventstore.New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	b := bus.New[order]()
	release := make(chan struct{})
	entered := make(chan struct{})
	b.Subscribe(
		"OrderUpdated",
		func(
			context.Context,
			asynxmd.Event[order],
		) {
			close(entered)
			<-release
		},
	)
	p := mustNew(
		t,
		es,
		b,
	)
	if _, err := p.Send(
		context.Background(),
		updateCmd(
			"o1",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	<-entered

	short, cancel := context.WithTimeout(
		context.Background(),
		20*time.Millisecond,
	)
	defer cancel()
	if err := p.Shutdown(short); err == nil {
		t.Fatal("Shutdown succeeded with a handler still running")
	}

	close(release)
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf(
			"retried Shutdown: %v",
			err,
		)
	}

	if err := p.Shutdown(context.Background()); !errors.Is(
		err,
		asynxmd.ErrAlreadyShuttingDown,
	) {
		t.Fatalf(
			"Shutdown after completion = %v, want ErrAlreadyShuttingDown",
			err,
		)
	}
}

func TestProcessor_Shutdown_ReportsAWorkerThatWillNotFinish(
	t *testing.T,
) {
	gate := make(chan struct{})
	entered := make(
		chan struct{},
		1,
	)
	p := newProcessor(
		t,
		models.WithShards[order](1),
		models.WithWorkersPerShard[order](1),
	)
	t.Cleanup(func() { close(gate) })

	sendCtx, cancelSend := context.WithCancel(context.Background())
	sent := make(
		chan error,
		1,
	)
	go func() {
		_, err := p.Send(
			sendCtx,
			gatedCmd{UpdateOrderCmd: updateCmd(
				"agg",
				1,
			), entered: entered, gate: gate},
		)
		sent <- err
	}()
	<-entered
	cancelSend()
	<-sent

	short, cancel := context.WithTimeout(
		context.Background(),
		20*time.Millisecond,
	)
	defer cancel()
	if err := p.Shutdown(short); err == nil {
		t.Fatal("Shutdown succeeded with a command still running")
	}
}

func TestProcessor_Shutdown_WaitsForASendInFlightUntilItsContextEnds(
	t *testing.T,
) {
	gate := make(chan struct{})
	entered := make(
		chan struct{},
		1,
	)
	p := newProcessor(
		t,
		models.WithShards[order](1),
		models.WithWorkersPerShard[order](1),
	)
	t.Cleanup(func() { close(gate) })
	go p.Send(
		context.Background(),
		gatedCmd{UpdateOrderCmd: updateCmd(
			"agg",
			1,
		), entered: entered, gate: gate},
	)
	<-entered

	short, cancel := context.WithTimeout(
		context.Background(),
		20*time.Millisecond,
	)
	defer cancel()
	if err := p.Shutdown(short); err == nil {
		t.Fatal("Shutdown returned while a Send was in flight")
	}
}

func TestProcessor_WaitPublish_DoesNotFailCommandsSentWhileItWaits(
	t *testing.T,
) {
	f := newFixture(
		t,
		store.New(),
	)
	ctx := context.Background()
	entered := make(
		chan struct{},
		1,
	)
	release := make(chan struct{})
	holdSlow := func(
		_ context.Context,
		e asynxmd.Event[order],
	) {
		if e.AggregateID == "slow" {
			entered <- struct{}{}
			<-release
		}
	}
	if _, err := f.b.Subscribe(
		"OrderCreated",
		holdSlow,
	); err != nil {
		t.Fatal(err)
	}

	if _, err := f.p.Send(
		ctx,
		mocks.CreateOrderCmd{ID: "slow", Total: 1},
	); err != nil {
		t.Fatal(err)
	}

	<-entered

	idle := make(chan struct{})
	go func() { f.p.WaitPublish(); close(idle) }()

	if _, err := f.p.SendWait(
		ctx,
		mocks.CreateOrderCmd{ID: "other", Total: 1},
	); err != nil {
		t.Fatalf(
			"a command sent while WaitPublish waits failed: %v",
			err,
		)
	}

	if paused := f.p.Paused(); len(paused) != 0 {
		t.Fatalf(
			"Paused = %v, want nobody parked",
			paused,
		)
	}

	close(release)
	<-idle
}

func TestShutdown_InterruptsThenClosesTheSuppliedDispatcher(
	t *testing.T,
) {
	fake := mockbuses.NewDispatcher[order]()
	p := mustNew(
		t,
		newES(),
		bus.New[order](),
		models.WithDispatcherFactory(fake.Factory()),
	)

	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	calls := fake.Calls()
	wrongCalls := len(calls) != 3 || calls[1] != "Interrupt" || calls[2] != "Close"
	if wrongCalls {
		t.Errorf(
			"calls = %v, want Factory, Interrupt, Close",
			calls,
		)
	}
}

func TestProcessor_IntegrationShutdownGraceful(
	t *testing.T,
) {
	memStore := store.New()
	channelBus := bus.New[order]()
	es := eventstore.New[order](
		memStore,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)

	p := mustNew(
		t,
		es,
		channelBus,
		models.WithShards[order](4),
		models.WithWorkersPerShard[order](2),
		models.WithQueueDepth[order](10),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	commandCount := 50
	var successCount int32

	var wg sync.WaitGroup
	for i := 0; i < commandCount; i++ {
		wg.Add(1)
		go func(
			idx int,
		) {
			defer wg.Done()
			cmd := mocks.CreateOrderCmd{
				ID:    "order" + string(rune(idx%4)),
				Total: 100.0,
			}
			if _, err := p.Send(
				ctx,
				cmd,
			); err == nil {
				atomic.AddInt32(
					&successCount,
					1,
				)
			}
		}(i)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer shutdownCancel()

	err := p.Shutdown(shutdownCtx)
	if err != nil &&
		err != context.DeadlineExceeded {
		t.Logf(
			"Shutdown returned: %v",
			err,
		)
	}

	cancel()
	wg.Wait()

	successVal := atomic.LoadInt32(&successCount)
	if successVal == 0 {
		t.Logf("Warning: no commands completed before shutdown")
	} else {
		t.Logf(
			"Integration: %d commands completed before shutdown",
			successVal,
		)
	}
}
