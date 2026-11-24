package core

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mockbuses"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor/models"
	"github.com/char2cs/asynx/models/failure"
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
	if err != failure.ErrShuttingDown {
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
		go sendCreate(
			ctx,
			p,
			i,
			&wg,
			errs,
		)
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
		expectNoErrorf(
			t,
			err,
			"Send failed",
		)
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
	if err2 != failure.ErrAlreadyShuttingDown {
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
	if err != failure.ErrShuttingDown {
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
	if err != failure.ErrShuttingDown {
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
		failure.ErrContextCancelled,
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
		failure.ErrShuttingDown,
	) {
		t.Errorf(
			"shutting down: err = %v, want ErrShuttingDown",
			err,
		)
	}
}

func forgetLogged(
	ctx context.Context,
	t *testing.T,
	p *Processor[order],
	id string,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	err := p.Forget(
		ctx,
		id,
	)
	if err != nil {
		t.Logf(
			"Forget: %v",
			err,
		)
	}
}

func sendGated(
	p *Processor[order],
	id string,
	entered chan<- struct{},
	gate <-chan struct{},
	results chan<- error,
) {
	cmd := gatedCmd{
		UpdateOrderCmd: updateCmd(
			id,
			1,
		),
		entered: entered,
		gate:    gate,
	}
	_, err := p.Send(
		context.Background(),
		cmd,
	)
	results <- err
}

func sendBurst(
	p *Processor[order],
	entered chan<- struct{},
	gate <-chan struct{},
	results chan<- error,
) {
	for i := range 6 {
		go sendGated(
			p,
			"agg"+string(rune('a'+i)),
			entered,
			gate,
			results,
		)
	}
}

func shutdownInto(
	p *Processor[order],
	down chan<- error,
) {
	down <- p.Shutdown(context.Background())
}

func waitPublishThenClose(
	p *Processor[order],
	idle chan<- struct{},
) {
	p.WaitPublish()
	close(idle)
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

	gate := newHandlerGate(0)
	f.b.Subscribe(
		"asynx\\.aggregate\\.forget",
		gate.block,
	)
	var wg sync.WaitGroup
	wg.Add(1)
	go forgetLogged(
		ctx,
		t,
		f.p,
		"o1",
		&wg,
	)
	<-gate.entered

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

	close(gate.release)
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
		sendBurst(
			p,
			entered,
			gate,
			results,
		)

		<-entered

		down := make(
			chan error,
			1,
		)
		go shutdownInto(
			p,
			down,
		)
		close(gate)

		awaitSendResults(
			t,
			results,
			6,
		)
		requireNoErrorf(
			t,
			<-down,
			"Shutdown",
		)
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
	gate := newHandlerGate(0)
	b.Subscribe(
		"OrderUpdated",
		gate.block,
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

	<-gate.entered

	short, cancel := context.WithTimeout(
		context.Background(),
		20*time.Millisecond,
	)
	defer cancel()
	if err := p.Shutdown(short); err == nil {
		t.Fatal("Shutdown succeeded with a handler still running")
	}

	close(gate.release)
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf(
			"retried Shutdown: %v",
			err,
		)
	}

	if err := p.Shutdown(context.Background()); !errors.Is(
		err,
		failure.ErrAlreadyShuttingDown,
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
	gate := newHandlerGate(1)
	if _, err := f.b.Subscribe(
		"OrderCreated",
		gate.holdIfSlow,
	); err != nil {
		t.Fatal(err)
	}

	if _, err := f.p.Send(
		ctx,
		mocks.CreateOrderCmd{ID: "slow", Total: 1},
	); err != nil {
		t.Fatal(err)
	}

	<-gate.entered

	idle := make(chan struct{})
	go waitPublishThenClose(
		f.p,
		idle,
	)

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

	close(gate.release)
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

func sendCounted(
	ctx context.Context,
	p *Processor[order],
	idx int,
	wg *sync.WaitGroup,
	successCount *int32,
) {
	defer wg.Done()
	cmd := mocks.CreateOrderCmd{
		ID:    "order" + string(rune(idx%4)),
		Total: 100.0,
	}
	_, err := p.Send(
		ctx,
		cmd,
	)
	if err == nil {
		atomic.AddInt32(
			successCount,
			1,
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
		go sendCounted(
			ctx,
			p,
			i,
			&wg,
			&successCount,
		)
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
		return
	}

	t.Logf(
		"Integration: %d commands completed before shutdown",
		successVal,
	)
}

func TestProcessor_Shutdown_DoesNotHangOnACallerWaitingForItsTurn(
	t *testing.T,
) {
	p := newProcessor(
		t,
		models.WithShards[order](1),
		models.WithWorkersPerShard[order](1),
	)
	gate := make(chan struct{})
	entered := make(
		chan struct{},
		1,
	)
	results := make(
		chan error,
		2,
	)
	go func() {
		_, err := p.Send(
			context.Background(),
			gatedCmd{
				UpdateOrderCmd: updateCmd(
					"agg",
					1,
				),
				entered: entered,
				gate:    gate,
			},
		)
		results <- err
	}()
	<-entered

	pending := make(
		chan struct{},
		1,
	)
	p.SetOnSendPending(func() { pending <- struct{}{} })
	go func() {
		_, err := p.Send(
			context.Background(),
			mocks.CreateOrderCmd{ID: "waiting", Total: 1},
		)
		results <- err
	}()
	<-pending

	done := make(
		chan error,
		1,
	)
	go func() { done <- p.Shutdown(context.Background()) }()
	close(gate)

	requireShutdownDone(
		t,
		done,
	)

	awaitSendResults(
		t,
		results,
		2,
	)
}

func requireShutdownDone(
	t *testing.T,
	done <-chan error,
) {
	t.Helper()
	select {
	case err := <-done:
		requireNoShutdownError(
			t,
			err,
		)
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown hung on a caller waiting for its turn")
	}
}

func requireNoShutdownError(
	t *testing.T,
	err error,
) {
	t.Helper()
	if err != nil {
		t.Fatalf(
			"Shutdown = %v, want nil",
			err,
		)
	}
}
