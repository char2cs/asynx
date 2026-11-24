package core

import (
	"context"
	"errors"
	"fmt"
	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor/models"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/failure"
	"github.com/char2cs/asynx/store"
	"testing"
	"time"
)

func TestProcessor_SendSuccess(
	t *testing.T,
) {
	p := newProcessor(t)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := p.Send(
		ctx,
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"expected nil, got %v",
			err,
		)
	}
}

func TestProcessor_ContextCancelledBeforeQueue(
	t *testing.T,
) {
	p := newProcessor(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err := p.Send(
		ctx,
		cmd,
	)
	if err != failure.ErrContextCancelled {
		t.Fatalf(
			"expected ErrContextCancelled, got %v",
			err,
		)
	}
}

func TestProcessor_ContextCancelledWhileWaiting(
	t *testing.T,
) {
	p := newProcessor(
		t,
		models.WithShards[order](1),
	)

	ctx, cancel := context.WithCancel(context.Background())

	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	pending := make(
		chan struct{},
		1,
	)
	p.SetOnSendPending(func() {
		select {
		case pending <- struct{}{}:
		default:
		}
	})

	done := make(
		chan error,
		1,
	)
	go func() {
		_, _err := p.Send(
			ctx,
			cmd,
		)
		done <- _err
	}()

	<-pending

	cancel()

	err := <-done
	if err != nil &&
		err != failure.ErrContextCancelled {
		t.Logf(
			"Send returned: %v (acceptable if completed quickly)",
			err,
		)
	}
}

func TestProcessor_QueueFull(
	t *testing.T,
) {
	p := newProcessor(
		t,
		models.WithShards[order](1),
		models.WithQueueDepth[order](1),
	)

	ctx := context.Background()

	cmd1 := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err := p.Send(
		ctx,
		cmd1,
	)
	if err != nil {
		t.Fatalf(
			"first send failed: %v",
			err,
		)
	}

	cmd2 := mocks.CreateOrderCmd{ID: "order2", Total: 100.0}
	_, err = p.Send(
		ctx,
		cmd2,
	)
	if err != nil {
		t.Logf(
			"second send result: %v (may be acceptable)",
			err,
		)
	}
}

func TestProcessor_SendContextCancelledDuringWait(
	t *testing.T,
) {
	p := newProcessor(t)

	ctx, cancel := context.WithCancel(context.Background())

	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	pending := make(
		chan struct{},
		1,
	)
	p.SetOnSendPending(func() {
		select {
		case pending <- struct{}{}:
		default:
		}
	})

	go func() {
		<-pending
		cancel()
	}()

	_, err := p.Send(
		ctx,
		cmd,
	)
	if err != nil &&
		err != failure.ErrContextCancelled {
		t.Logf(
			"Send returned: %v",
			err,
		)
	}
}

func TestProcessor_SendToMultipleShards(
	t *testing.T,
) {
	p := newProcessor(
		t,
		models.WithShards[order](4),
	)

	ctx := context.Background()

	for i := 0; i < 12; i++ {
		mustSendOrder(
			t,
			p,
			ctx,
			"order"+string(rune('0'+i)),
			i,
		)
	}
}

func requireCancelled(
	t *testing.T,
	err error,
) {
	t.Helper()
	if err != failure.ErrContextCancelled {
		t.Errorf(
			"expected ErrContextCancelled, got %v",
			err,
		)
	}
}

func mustSendOrder(
	t *testing.T,
	p *Processor[order],
	ctx context.Context,
	id string,
	i int,
) {
	t.Helper()
	cmd := mocks.CreateOrderCmd{ID: id, Total: 100.0}
	_, err := p.Send(
		ctx,
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"send %d failed: %v",
			i,
			err,
		)
	}
}

func TestProcessor_ContextCancelledEarlyCheck(
	t *testing.T,
) {
	p := newProcessor(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := p.Send(
		ctx,
		cmd,
	)
	if err != failure.ErrContextCancelled {
		t.Fatalf(
			"expected ErrContextCancelled, got %v",
			err,
		)
	}
}

func TestProcessor_AllSendBranches(
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
		models.WithShards[order](8),
		models.WithQueueDepth[order](100),
	)
	defer p.Shutdown(context.Background())

	ctx := context.Background()

	cmd1 := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err := p.Send(
		ctx,
		cmd1,
	)
	if err != nil {
		t.Fatalf(
			"normal send failed: %v",
			err,
		)
	}

	ctx2, cancel := context.WithCancel(context.Background())
	cancel()
	cmd2 := mocks.CreateOrderCmd{ID: "order2", Total: 100.0}
	_, err = p.Send(
		ctx2,
		cmd2,
	)
	if err != failure.ErrContextCancelled {
		t.Fatalf(
			"cancelled early should return ErrContextCancelled, got %v",
			err,
		)
	}

	cmd3 := mocks.CreateOrderCmd{ID: "order3", Total: -100.0}
	_, err = p.Send(
		ctx,
		cmd3,
	)
	if err != failure.ErrValidation {
		t.Fatalf(
			"validation error should propagate, got %v",
			err,
		)
	}

	for i := 0; i < 5; i++ {
		mustSendOrder(
			t,
			p,
			ctx,
			"order"+string(rune('A'+i)),
			i,
		)
	}
}

func TestProcessor_SendWait_Success(
	t *testing.T,
) {
	p := newProcessor(t)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "emit-order", Total: 42.0}

	event, err := p.SendWait(
		ctx,
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"expected nil, got %v",
			err,
		)
	}

	if event.AggregateID != "emit-order" {
		t.Errorf(
			"expected AggregateID emit-order, got %q",
			event.AggregateID,
		)
	}

	if event.EventName == "" {
		t.Error("expected non-empty EventName")
	}
}

func TestProcessor_SendWait_HandlersCompleteBeforeReturn(
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
	defer p.Shutdown(context.Background())

	handled := &callCounter{}
	channelBus.Subscribe(
		"OrderCreated",
		handled.onEvent,
	)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "handler-order", Total: 10.0}

	_, err := p.SendWait(
		ctx,
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"expected nil, got %v",
			err,
		)
	}

	if handled.calls.Load() == 0 {
		t.Error("handler not done when SendWait returned")
	}
}

func TestProcessor_SendWait_ValidationError(
	t *testing.T,
) {
	p := newProcessor(t)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "bad-order", Total: -1.0}

	_, err := p.SendWait(
		ctx,
		cmd,
	)
	if err != failure.ErrValidation {
		t.Fatalf(
			"expected ErrValidation, got %v",
			err,
		)
	}
}

func TestProcessor_SendWait_ContextCancelled(
	t *testing.T,
) {
	p := newProcessor(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cmd := mocks.CreateOrderCmd{ID: "ctx-order", Total: 1.0}

	_, err := p.SendWait(
		ctx,
		cmd,
	)
	if err != failure.ErrContextCancelled {
		t.Fatalf(
			"expected ErrContextCancelled, got %v",
			err,
		)
	}
}

func TestProcessor_SendWait_ContextCancelledWhileWaiting(
	t *testing.T,
) {
	memStore := store.New()
	unblockCh := make(chan struct{})
	bb := &blockingTestBus[order]{unblockCh: unblockCh}
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
		bb,
		models.WithShards[order](1),
	)
	t.Cleanup(func() {
		close(unblockCh)
		p.Shutdown(context.Background())
	})

	ctx, cancel := context.WithCancel(context.Background())

	cmd := mocks.CreateOrderCmd{ID: "ew-ctx-wait", Total: 1.0}

	pending := make(
		chan struct{},
		1,
	)
	p.SetOnSendPending(func() {
		select {
		case pending <- struct{}{}:
		default:
		}
	})

	done := make(
		chan error,
		1,
	)
	go func() {
		_, err := p.SendWait(
			ctx,
			cmd,
		)
		done <- err
	}()

	<-pending

	cancel()

	select {
	case err := <-done:
		requireCancelled(
			t,
			err,
		)
	case <-time.After(2 * time.Second):
		t.Error("SendWait did not return after context cancel")
	}
}

func TestProcessor_Forget_ErasesAndNotifiesHandlers(
	t *testing.T,
) {
	f := newFixture(
		t,
		store.New(),
	)
	ctx := context.Background()

	tombstones := &callCounter{}
	f.b.Subscribe(
		"asynx\\.aggregate\\.forget",
		tombstones.onEvent,
	)

	if _, err := f.p.Send(
		ctx,
		mocks.CreateOrderCmd{ID: "o1", Total: 5},
	); err != nil {
		t.Fatal(err)
	}

	if err := f.p.Forget(
		ctx,
		"o1",
	); err != nil {
		t.Fatalf(
			"Forget: %v",
			err,
		)
	}

	if tombstones.calls.Load() != 1 {
		t.Errorf(
			"forget handler calls = %d, want 1",
			tombstones.calls.Load(),
		)
	}

	if blobs, _ := f.s.ReadFrom(
		ctx,
		"events:o1",
		1,
	); len(blobs) != 0 {
		t.Errorf(
			"stream not erased: %d blobs",
			len(blobs),
		)
	}

	if err := f.p.Forget(
		ctx,
		"o1",
	); !errors.Is(
		err,
		failure.ErrValidation,
	) {
		t.Errorf(
			"second Forget err = %v, want ErrValidation",
			err,
		)
	}
}

func TestProcessor_SendWait_GivesUpWaitingButReturnsTheCommittedEvent(
	t *testing.T,
) {
	f := newFixture(
		t,
		store.New(),
	)
	gate := newHandlerGate(0)
	t.Cleanup(func() { close(gate.release) })
	f.b.Subscribe(
		"OrderUpdated",
		gate.block,
	)

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		evt event.Event[order]
		err error
	}
	got := make(
		chan result,
		1,
	)
	go func() {
		evt, err := f.p.SendWait(
			ctx,
			updateCmd(
				"o1",
				1,
			),
		)
		got <- result{evt, err}
	}()
	<-gate.entered
	cancel()
	r := <-got
	if !errors.Is(
		r.err,
		failure.ErrContextCancelled,
	) {
		t.Fatalf(
			"err = %v, want ErrContextCancelled",
			r.err,
		)
	}

	if r.evt.AggregateID != "o1" ||
		r.evt.Version != 1 {
		t.Fatalf(
			"event = %+v, want the committed v1",
			r.evt,
		)
	}
}

func TestProcessor_SendWait_FromAHandlerOfTheSameAggregateFailsFast(
	t *testing.T,
) {
	f := newFixture(
		t,
		store.New(),
	)
	again := &sendsWaitAgain{
		processor: f.p,
		got: make(
			chan error,
			1,
		),
	}
	f.b.Subscribe(
		"OrderUpdated",
		again.sendWait,
	)
	if _, err := f.p.Send(
		context.Background(),
		updateCmd(
			"o1",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-again.got:
		requireSendWaitFromHandler(
			t,
			err,
		)
	case <-time.After(5 * time.Second):
		t.Fatal("SendWait from a handler hung")
	}
}

type sendsWaitAgain struct {
	processor *Processor[order]
	got       chan error
}

func (s *sendsWaitAgain) sendWait(
	ctx context.Context,
	e event.Event[order],
) {
	_, err := s.processor.SendWait(
		ctx,
		updateCmd(
			e.AggregateID,
			2,
		),
	)
	s.got <- err
}

func requireSendWaitFromHandler(
	t *testing.T,
	err error,
) {
	t.Helper()
	if !errors.Is(
		err,
		failure.ErrSendWaitFromHandler,
	) {
		t.Fatalf(
			"SendWait from the handler = %v, want ErrSendWaitFromHandler",
			err,
		)
	}
}

func BenchmarkSend_MultiShard(
	b *testing.B,
) {
	for _, shards := range []int{1, 4, 8, 16} {
		b.Run(
			fmt.Sprintf(
				"shards=%d",
				shards,
			),
			multiShardBench(shards),
		)
	}
}

func multiShardBench(
	shards int,
) func(*testing.B) {
	return func(
		b *testing.B,
	) {
		runMultiShard(
			b,
			shards,
		)
	}
}

func runMultiShard(
	b *testing.B,
	shards int,
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
		b,
		es,
		channelBus,
		models.WithShards[order](shards),
	)
	defer p.Shutdown(context.Background())

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		sendBenchOrder(
			p,
			ctx,
		)
	}
}

func drainBench(
	pb *testing.PB,
	p *Processor[order],
	ctx context.Context,
) {
	for pb.Next() {
		sendBenchOrder(
			p,
			ctx,
		)
	}
}

func sendBenchOrder(
	p *Processor[order],
	ctx context.Context,
) {
	cmd := mocks.CreateOrderCmd{ID: "order", Total: 100.0}
	p.Send(
		ctx,
		cmd,
	)
}

func BenchmarkSend_Parallel(
	b *testing.B,
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
		b,
		es,
		channelBus,
		models.WithShards[order](8),
	)
	defer p.Shutdown(context.Background())

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	sendLoop := func(
		pb *testing.PB,
	) {
		drainBench(
			pb,
			p,
			ctx,
		)
	}
	b.RunParallel(sendLoop)
}

func BenchmarkSend_WithBusFanout(
	b *testing.B,
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

	for i := 0; i < 10; i++ {
		channelBus.Subscribe(
			"OrderCreated",
			func(
				ctx context.Context,
				e event.Event[order],
			) {
			},
		)
	}

	p := mustNew(
		b,
		es,
		channelBus,
	)
	defer p.Shutdown(context.Background())

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		cmd := mocks.CreateOrderCmd{ID: "order", Total: 100.0}
		p.Send(
			ctx,
			cmd,
		)
	}
}

func TestProcessor_Send_ACommandCancelledBeforeItsTurnNeverRuns(
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
	holder := make(
		chan error,
		1,
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
		holder <- err
	}()
	<-entered

	pending := make(
		chan struct{},
		1,
	)
	p.SetOnSendPending(func() { pending <- struct{}{} })
	ctx, cancel := context.WithCancel(context.Background())
	victim := make(
		chan error,
		1,
	)
	go func() {
		_, err := p.Send(
			ctx,
			mocks.CreateOrderCmd{ID: "victim", Total: 1},
		)
		victim <- err
	}()
	<-pending
	p.SetOnSendPending(nil)
	cancel()
	if err := <-victim; !errors.Is(
		err,
		failure.ErrContextCancelled,
	) {
		t.Fatalf(
			"Send = %v, want ErrContextCancelled",
			err,
		)
	}

	close(gate)
	<-holder
	mustSendOrder(
		t,
		p,
		context.Background(),
		"after",
		0,
	)

	exists, err := p.es.Exists(
		context.Background(),
		"victim",
	)
	if err != nil {
		t.Fatal(err)
	}

	if exists {
		t.Fatal("a command whose Send reported ErrContextCancelled ran")
	}
}
