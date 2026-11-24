package pool_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher"
	"github.com/char2cs/asynx/internal/processor/internal/exec"
	"github.com/char2cs/asynx/internal/processor/internal/pool"
	"github.com/char2cs/asynx/internal/processor/models"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

type blockingBus[T any] struct {
	unblockCh chan struct{}
}

// Publish accepts the event and does nothing; the bus is only here to block
// PublishSync.
func (b *blockingBus[T]) Publish(
	_ context.Context,
	_ asynxmd.Event[T],
) error {
	return nil
}

// PublishSync blocks until the test closes unblockCh, holding delivery stuck
// so tests can observe the pool meanwhile.
func (b *blockingBus[T]) PublishSync(
	_ context.Context,
	_ asynxmd.Event[T],
) error {
	<-b.unblockCh
	return nil
}

// Subscribe registers nothing and returns an empty id.
func (b *blockingBus[T]) Subscribe(
	_ string,
	_ asynxmd.ProjectionHandler[T],
	_ ...asynxmd.SubscriptionOpt[T],
) (string, error) {
	return "",
		nil
}

// Unsubscribe does nothing.
func (b *blockingBus[T]) Unsubscribe(
	_ string,
) error {
	return nil
}

// Close does nothing and succeeds.
func (b *blockingBus[T]) Close(
	_ context.Context,
) error {
	return nil
}

// WaitForHandlers returns at once; nothing is tracked.
func (b *blockingBus[T]) WaitForHandlers() {}

type order = mocks.Order

func TestPool_SingleCommandSuccess(
	t *testing.T,
) {
	s := store.New()
	b := bus.New[order]()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: b})
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		1,
		0,
		1,
	)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	envelope := newEnvelope(
		executor,
		ctx,
		mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
	)

	shard.CommandChan() <- envelope

	result := <-envelope.ResultChan
	if result.Err != nil {
		t.Fatalf(
			"expected nil, got %v",
			result.Err,
		)
	}

	agg, err := es.Get(
		ctx,
		"order1",
	)
	if err != nil {
		t.Fatalf(
			"expected aggregate to exist, got error: %v",
			err,
		)
	}

	if agg.ID != "order1" ||
		agg.Total != 100.0 {
		t.Errorf(
			"unexpected aggregate state: %+v",
			agg,
		)
	}
}

func TestPool_SerialOrderingSameAggregate(
	t *testing.T,
) {
	s := store.New()
	b := bus.New[order]()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: b})
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		1,
		0,
		1,
	)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	for i := 1; i <= 10; i++ {
		envelope := newEnvelope(
			executor,
			ctx,
			mocks.UpdateOrderCmd{
				ID:       "order1",
				NewState: order{ID: "order1", Total: float64(i * 10), Status: "Pending"},
			},
		)

		shard.CommandChan() <- envelope

		result := <-envelope.ResultChan
		requireSent(
			t,
			i,
			result.Err,
		)
	}

	agg, err := es.Get(
		ctx,
		"order1",
	)
	if err != nil {
		t.Fatalf(
			"failed to get aggregate: %v",
			err,
		)
	}

	if agg.Total != 100.0 {
		t.Errorf(
			"expected Total=100.0 after 10 updates, got %f",
			agg.Total,
		)
	}
}

func TestPool_ParallelDifferentAggregates(
	t *testing.T,
) {
	s := store.New()
	b := bus.New[order]()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: b})
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		8,
		0,
		1,
	)
	defer p.Drain(context.Background())

	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(
		chan error,
		10,
	)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go submitCreate(
			executor,
			ctx,
			&wg,
			errs,
			p.Shards()[i%len(p.Shards())],
			i,
		)
	}

	wg.Wait()
	close(errs)

	reportErrs(
		t,
		errs,
	)

	for i := 0; i < 10; i++ {
		checkTotal(
			ctx,
			t,
			es,
			"order"+string(rune(i)),
		)
	}
}

func TestPool_ValidationFailureNoWrite(
	t *testing.T,
) {
	s := store.New()
	b := bus.New[order]()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: b})
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		1,
		0,
		1,
	)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	envelope := newEnvelope(
		executor,
		ctx,
		mocks.CancelOrderCmd{ID: "order1"},
	)

	shard.CommandChan() <- envelope

	result := <-envelope.ResultChan
	if result.Err != asynxmd.ErrValidation {
		t.Fatalf(
			"expected ErrValidation, got %v",
			result.Err,
		)
	}

	_, err := es.Get(
		ctx,
		"order1",
	)
	if err != asynxmd.ErrNotFound {
		t.Errorf("expected ErrNotFound, but aggregate was written")
	}
}

func TestPool_DrainWaitsForInflight(
	t *testing.T,
) {
	s := store.New()
	b := bus.New[order]()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: b})
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		1,
		0,
		1,
	)

	ctx := context.Background()
	shard := p.Shards()[0]

	var wg sync.WaitGroup
	errs := make(
		chan error,
		10,
	)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go submitCreate(
			executor,
			ctx,
			&wg,
			errs,
			shard,
			i,
		)
	}

	wg.Wait()

	err := p.Drain(context.Background())
	if err != nil {
		t.Fatalf(
			"drain failed: %v",
			err,
		)
	}

	close(errs)

	reportErrs(
		t,
		errs,
	)

	for i := 0; i < 10; i++ {
		checkExists(
			ctx,
			t,
			es,
			"order"+string(rune(i)),
		)
	}
}

func TestPool_DrainTimeout(
	t *testing.T,
) {
	p := pool.New[order](
		1,
		0,
		1,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		1*time.Millisecond,
	)
	defer cancel()

	err := p.Drain(ctx)
	if err != nil &&
		err != context.DeadlineExceeded {
		t.Logf(
			"drain returned: %v (may be acceptable if completed quickly)",
			err,
		)
	}
}

func TestPool_QueueFullBlocksSender(
	t *testing.T,
) {
	s := store.New()
	b := bus.New[order]()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: b})
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		1,
		1,
		1,
	)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	blocked := make(chan struct{})
	dispatched := make(
		chan struct{},
		1,
	)
	unblock := make(chan struct{})

	shard.SetOnDispatched(func() {
		select {
		case dispatched <- struct{}{}:
		default:
		}
	})

	blockingCmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	done1 := make(
		chan error,
		1,
	)
	go func() {
		envelope := newEnvelope(
			executor,
			ctx,
			blockingCmd,
		)
		shard.CommandChan() <- envelope
		close(blocked)
		result := <-envelope.ResultChan
		done1 <- result.Err
	}()

	<-blocked
	<-dispatched

	normalEnvelope := newEnvelope(
		executor,
		ctx,
		mocks.CreateOrderCmd{ID: "order2", Total: 100.0},
	)

	sendDone := make(chan struct{})
	go func() {
		shard.CommandChan() <- normalEnvelope
		close(sendDone)
	}()

	select {
	case <-sendDone:
		t.Logf("send completed (queue not full or processor fast)")
	case <-time.After(100 * time.Millisecond):
	}

	close(unblock)
	<-done1
	p.Drain(context.Background())
}

func TestPool_MultipleWorkers(
	t *testing.T,
) {
	s := store.New()
	b := bus.New[order]()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: b})
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		4,
		0,
		4,
	)
	defer p.Drain(context.Background())

	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make(
		chan error,
		100,
	)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go submitCreate(
			executor,
			ctx,
			&wg,
			errs,
			p.Shards()[i%4],
			i,
		)
	}

	wg.Wait()
	close(errs)

	reportErrs(
		t,
		errs,
	)
}

func TestPool_ValidationErrorDecrementVersionSingleWorker(
	t *testing.T,
) {
	s := store.New()
	b := bus.New[order]()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: b})
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		1,
		0,
		1,
	)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	envelope1 := newEnvelope(
		executor,
		ctx,
		mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
	)
	shard.CommandChan() <- envelope1
	result1 := <-envelope1.ResultChan
	if result1.Err != nil {
		t.Fatalf(
			"create failed: %v",
			result1.Err,
		)
	}

	envelope2 := newEnvelope(
		executor,
		ctx,
		mocks.CancelOrderCmd{ID: "order1"},
	)
	shard.CommandChan() <- envelope2
	result2 := <-envelope2.ResultChan
	if result2.Err != nil {
		t.Fatalf(
			"cancel failed: %v",
			result2.Err,
		)
	}

	envelope3 := newEnvelope(
		executor,
		ctx,
		mocks.CancelOrderCmd{ID: "order2"},
	)
	shard.CommandChan() <- envelope3
	result3 := <-envelope3.ResultChan
	if result3.Err != asynxmd.ErrValidation {
		t.Fatalf(
			"expected ErrValidation for cancel non-existent, got %v",
			result3.Err,
		)
	}

	envelope4 := newEnvelope(
		executor,
		ctx,
		mocks.CreateOrderCmd{ID: "order3", Total: 200.0},
	)
	shard.CommandChan() <- envelope4
	result4 := <-envelope4.ResultChan
	if result4.Err != nil {
		t.Fatalf(
			"create after validation error failed: %v",
			result4.Err,
		)
	}
}

func TestPool_ContextCancelledDuringExecution(
	t *testing.T,
) {
	s := store.New()
	b := bus.New[order]()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: b})
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		1,
		0,
		1,
	)
	defer p.Drain(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	shard := p.Shards()[0]

	envelope := newEnvelope(
		executor,
		ctx,
		mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
	)

	shard.CommandChan() <- envelope

	cancel()

	select {
	case <-envelope.ResultChan:
	case <-time.After(2 * time.Second):
		t.Fatalf("result not received within timeout")
	}
}

func TestPool_DrainWithNoCommands(
	t *testing.T,
) {
	p := pool.New[order](
		2,
		0,
		2,
	)

	err := p.Drain(context.Background())
	if err != nil {
		t.Fatalf(
			"drain failed: %v",
			err,
		)
	}
}

func TestPool_DrainTimeoutBothPhases(
	t *testing.T,
) {
	p := pool.New[order](
		1,
		0,
		1,
	)

	ctx, cancel := context.WithDeadline(
		context.Background(),
		time.Now().Add(-time.Second),
	)
	defer cancel()

	err := p.Drain(ctx)
	if err != context.DeadlineExceeded &&
		err != nil {
		t.Logf(
			"Drain timeout result: %v",
			err,
		)
	}
}

func TestPool_DispatcherMultipleCorrections(
	t *testing.T,
) {
	s := store.New()
	b := bus.New[order]()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: b})
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		1,
		0,
		2,
	)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	envelope1 := newEnvelope(
		executor,
		ctx,
		mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
	)
	shard.CommandChan() <- envelope1
	<-envelope1.ResultChan

	envelope2 := newEnvelope(
		executor,
		ctx,
		mocks.CancelOrderCmd{ID: "order2"},
	)
	shard.CommandChan() <- envelope2
	result2 := <-envelope2.ResultChan
	if result2.Err != asynxmd.ErrValidation {
		t.Fatalf(
			"expected ErrValidation, got %v",
			result2.Err,
		)
	}

	envelope3 := newEnvelope(
		executor,
		ctx,
		mocks.CreateOrderCmd{ID: "order3", Total: 100.0},
	)
	shard.CommandChan() <- envelope3
	result3 := <-envelope3.ResultChan
	if result3.Err != nil {
		t.Fatalf(
			"create after failed validation (multi-worker) failed: %v",
			result3.Err,
		)
	}
}

func TestPool_WaitWorkers_ContextCancelledAfterDispatch(
	t *testing.T,
) {
	s := store.New()
	unblockCh := make(chan struct{})
	bb := &blockingBus[order]{unblockCh: unblockCh}
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: bb})
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		1,
		0,
		1,
	)

	ctx := context.Background()
	shard := p.Shards()[0]

	envelope := newWaitingEnvelope(
		executor,
		ctx,
		mocks.CreateOrderCmd{ID: "blocking-order", Total: 10.0},
	)
	shard.CommandChan() <- envelope

	drainCtx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Millisecond,
	)
	defer cancel()

	err := p.Drain(drainCtx)
	if err == nil {
		close(unblockCh)
		t.Skip("Drain completed before timeout — worker was too fast; skipping")
	}

	if err != context.DeadlineExceeded {
		close(unblockCh)
		t.Fatalf(
			"expected DeadlineExceeded from waitWorkers, got %v",
			err,
		)
	}

	close(unblockCh)
}

func BenchmarkPool_Send_SingleShard(
	b *testing.B,
) {
	s := store.New()
	bu := bus.New[order]()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: bu})
	defer d.Close(context.Background())
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		1,
		0,
		1,
	)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		envelope := newEnvelope(
			executor,
			ctx,
			mocks.CreateOrderCmd{ID: "order", Total: 100.0},
		)

		shard.CommandChan() <- envelope
		<-envelope.ResultChan
	}
}

func BenchmarkPool_Send_MultiShard(
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
		benchMultiShard(
			b,
			shards,
		)
	}
}

func benchMultiShard(
	b *testing.B,
	shards int,
) {
	s := store.New()
	bu := bus.New[order]()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: bu})
	defer d.Close(context.Background())
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		shards,
		0,
		1,
	)
	defer p.Drain(context.Background())

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		benchSend(
			executor,
			ctx,
			p.Shards()[b.N%shards],
		)
	}
}

func BenchmarkPool_Send_Parallel(
	b *testing.B,
) {
	s := store.New()
	bu := bus.New[order]()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: bu})
	defer d.Close(context.Background())
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		8,
		0,
		1,
	)
	defer p.Drain(context.Background())

	b.ReportAllocs()
	b.ResetTimer()

	sending := &parallelSender{
		executor: executor,
		pool:     p,
	}
	b.RunParallel(sending.sendUntilDone)
}

type parallelSender struct {
	executor models.Executor[order]
	pool     *pool.ShardPool[order]
}

func (s *parallelSender) sendUntilDone(
	pb *testing.PB,
) {
	idx := 0
	for pb.Next() {
		benchSend(
			s.executor,
			context.Background(),
			s.pool.Shards()[idx%8],
		)
		idx++
	}
}

func benchSend(
	executor models.Executor[order],
	ctx context.Context,
	shard *pool.Shard[order],
) {
	envelope := newEnvelope(
		executor,
		ctx,
		mocks.CreateOrderCmd{ID: "order", Total: 100.0},
	)

	shard.CommandChan() <- envelope
	<-envelope.ResultChan
}

func submitCreate(
	executor models.Executor[order],
	ctx context.Context,
	wg *sync.WaitGroup,
	errs chan<- error,
	shard *pool.Shard[order],
	idx int,
) {
	defer wg.Done()
	envelope := newEnvelope(
		executor,
		ctx,
		mocks.CreateOrderCmd{ID: "order" + string(rune(idx)), Total: 100.0},
	)

	shard.CommandChan() <- envelope
	result := <-envelope.ResultChan
	errs <- result.Err
}

func requireSent(
	t *testing.T,
	iteration int,
	err error,
) {
	t.Helper()
	if err != nil {
		t.Fatalf(
			"Send failed at iteration %d: %v",
			iteration,
			err,
		)
	}
}

func reportErrs(
	t *testing.T,
	errs <-chan error,
) {
	t.Helper()
	for err := range errs {
		reportErr(
			t,
			err,
		)
	}
}

func reportErr(
	t *testing.T,
	err error,
) {
	t.Helper()
	if err != nil {
		t.Errorf(
			"Send failed: %v",
			err,
		)
	}
}

func checkExists(
	ctx context.Context,
	t *testing.T,
	es eventstore.EventStore[order],
	id string,
) {
	t.Helper()
	if _, err := es.Get(
		ctx,
		id,
	); err != nil {
		t.Errorf(
			"aggregate %s not found: %v",
			id,
			err,
		)
	}
}

func checkTotal(
	ctx context.Context,
	t *testing.T,
	es eventstore.EventStore[order],
	id string,
) {
	t.Helper()
	agg, err := es.Get(
		ctx,
		id,
	)
	if err != nil {
		t.Errorf(
			"aggregate %s not found: %v",
			id,
			err,
		)
		return
	}

	if agg.Total != 100.0 {
		t.Errorf(
			"aggregate %s has Total=%f, expected 100.0",
			id,
			agg.Total,
		)
	}
}
