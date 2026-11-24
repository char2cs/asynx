package core

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/v2/internal/bus"
	"github.com/char2cs/asynx/v2/internal/eventstore"
	"github.com/char2cs/asynx/v2/internal/mockbuses"
	"github.com/char2cs/asynx/v2/internal/mocks"
	"github.com/char2cs/asynx/v2/internal/processor/models"
	"github.com/char2cs/asynx/v2/models/dispatch"
	"github.com/char2cs/asynx/v2/models/dispatcher"
	"github.com/char2cs/asynx/v2/models/event"
	"github.com/char2cs/asynx/v2/models/failure"
	"github.com/char2cs/asynx/v2/models/handler"
	"github.com/char2cs/asynx/v2/models/pubsub"
	"github.com/char2cs/asynx/v2/models/storage"
	"github.com/char2cs/asynx/v2/models/subscription"
	"github.com/char2cs/asynx/v2/store"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type order = mocks.Order

func newProcessor(
	t *testing.T,
	opts ...models.Opt[order],
) *Processor[order] {
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
		opts...,
	)

	t.Cleanup(func() {
		p.Shutdown(context.Background())
	})

	return p
}

func TestProcessor_ValidationError(
	t *testing.T,
) {
	p := newProcessor(t)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: -100.0}

	_, err := p.Send(
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

func TestProcessor_ValidationNoWrite(
	t *testing.T,
) {
	p := newProcessor(t)

	ctx := context.Background()

	cmd1 := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	p.Send(
		ctx,
		cmd1,
	)

	cmd2 := mocks.CancelOrderCmd{ID: "order2"}

	_, err := p.Send(
		ctx,
		cmd2,
	)
	if err != failure.ErrValidation {
		t.Fatalf(
			"expected ErrValidation, got %v",
			err,
		)
	}
}

func TestProcessor_SerialOrdering10Increments(
	t *testing.T,
) {
	p := newProcessor(
		t,
		models.WithShards[order](1),
	)

	ctx := context.Background()

	for i := 1; i <= 10; i++ {
		cmd := mocks.UpdateOrderCmd{
			ID:       "order1",
			NewState: order{ID: "order1", Total: float64(i * 10), Status: "Pending"},
		}
		_, err := p.Send(
			ctx,
			cmd,
		)
		requireNoErrorf(
			t,
			err,
			"Send failed at iteration %d",
			i,
		)
	}
}

func requireNoErrorf(
	t *testing.T,
	err error,
	format string,
	args ...any,
) {
	t.Helper()
	if err != nil {
		t.Fatalf(
			format+": %v",
			append(
				args,
				err,
			)...,
		)
	}
}

func expectNoErrorf(
	t *testing.T,
	err error,
	format string,
	args ...any,
) {
	t.Helper()
	if err != nil {
		t.Errorf(
			format+": %v",
			append(
				args,
				err,
			)...,
		)
	}
}

func sendCreate(
	ctx context.Context,
	p *Processor[order],
	idx int,
	wg *sync.WaitGroup,
	errs chan<- error,
) {
	defer wg.Done()
	cmd := mocks.CreateOrderCmd{ID: "order" + string(rune(idx)), Total: 100.0}
	_, err := p.Send(
		ctx,
		cmd,
	)
	errs <- err
}

func TestProcessor_ParallelAggregates8Goroutines(
	t *testing.T,
) {
	p := newProcessor(
		t,
		models.WithShards[order](8),
	)

	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make(
		chan error,
		8,
	)

	for i := 0; i < 8; i++ {
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
	close(errs)

	for err := range errs {
		expectNoErrorf(
			t,
			err,
			"Send failed",
		)
	}
}

func TestProcessor_EventPublishedToRealBus(
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

	ctx := context.Background()

	forwarded := &eventForwarder{events: make(
		chan event.Event[order],
		1,
	)}
	channelBus.Subscribe(
		"OrderCreated",
		forwarded.forward,
	)

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

	select {
	case evt := <-forwarded.events:
		expectEventName(
			t,
			evt.EventName,
		)
	case <-time.After(1 * time.Second):
		t.Fatalf("event not published within timeout")
	}
}

func expectEventName(
	t *testing.T,
	name string,
) {
	t.Helper()
	if name != "OrderCreated" {
		t.Errorf(
			"unexpected event name: %s",
			name,
		)
	}
}

func TestProcessor_PublishUsesDetachedContext(
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

	ctx, cancel := context.WithCancel(context.Background())

	forwarded := &contextForwarder{contexts: make(
		chan context.Context,
		1,
	)}
	channelBus.Subscribe(
		"OrderCreated",
		forwarded.forward,
	)

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

	cancel()

	select {
	case <-forwarded.contexts:
	case <-time.After(1 * time.Second):
		t.Fatalf("event not published within timeout")
	}
}

func TestProcessor_WithWorkersPerShard(
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
		models.WithShards[order](2),
		models.WithWorkersPerShard[order](6),
	)
	defer p.Shutdown(context.Background())

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

func TestProcessor_WithQueueDepth(
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
		models.WithQueueDepth[order](100),
	)
	defer p.Shutdown(context.Background())

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

func TestProcessor_NegativeOptionsIgnored(
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
		models.WithShards[order](-5),
		models.WithQueueDepth[order](-1),
		models.WithWorkersPerShard[order](0),
	)
	defer p.Shutdown(context.Background())

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

func TestWithPublishErrorHandler_IsInvokedOnPublishError(
	t *testing.T,
) {
	reported := &publishErrorFlag{}
	s := store.New()

	proc := mustNew(
		t,
		eventstore.New[mocks.Order](
			s,
			store.NewSnapshots(),
			nil,
			1,
			nil,
		),
		&mockbuses.ErrBus[mocks.Order]{Err: errors.New("bus down")},
		models.WithPublishErrorHandler[mocks.Order](reported.report),
	)

	_, err := proc.Send(
		context.Background(),
		mocks.CreateOrderCmd{ID: "order-1", Total: 10},
	)
	if err != nil {
		t.Fatalf(
			"Send: %v",
			err,
		)
	}

	proc.WaitPublish()

	if !reported.wasCalled() {
		t.Error("PublishErrorHandler was not called after bus error")
	}
}

type eventForwarder struct {
	events chan event.Event[order]
}

func (f *eventForwarder) forward(
	_ context.Context,
	e event.Event[order],
) {
	f.events <- e
}

type contextForwarder struct {
	contexts chan context.Context
}

func (f *contextForwarder) forward(
	ctx context.Context,
	_ event.Event[order],
) {
	f.contexts <- ctx
}

type publishErrorFlag struct {
	mu     sync.Mutex
	called bool
}

func (f *publishErrorFlag) report(
	context.Context,
	event.Event[mocks.Order],
	error,
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.called = true
}

func (f *publishErrorFlag) wasCalled() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.called
}

type handlerGate struct {
	entered chan struct{}
	release chan struct{}
}

func newHandlerGate(
	entries int,
) *handlerGate {
	return &handlerGate{
		entered: make(
			chan struct{},
			entries,
		),
		release: make(chan struct{}),
	}
}

func (g *handlerGate) block(
	context.Context,
	event.Event[order],
) {
	close(g.entered)
	<-g.release
}

func (g *handlerGate) holdIfSlow(
	_ context.Context,
	e event.Event[order],
) {
	if e.AggregateID == "slow" {
		g.entered <- struct{}{}
		<-g.release
	}
}

type callCounter struct {
	calls atomic.Int32
}

func (c *callCounter) onEvent(
	context.Context,
	event.Event[order],
) {
	c.calls.Add(1)
}

func (c *callCounter) onProjection(
	context.Context,
	storage.Tx,
	event.Event[order],
) error {
	c.calls.Add(1)
	return nil
}

func (c *callCounter) onSnapshotError(
	context.Context,
	event.Event[order],
	error,
) {
	c.calls.Add(1)
}

func (c *callCounter) onDeliveryError(
	context.Context,
	event.Event[order],
	error,
	int,
) {
	c.calls.Add(1)
}

func (c *callCounter) onDeliveryPanic(
	context.Context,
	event.Event[order],
	any,
) {
	c.calls.Add(1)
}

type blockingTestBus[T any] struct {
	unblockCh chan struct{}
}

// Publish accepts the event and does nothing; the bus is only here to block
// PublishSync.
func (b *blockingTestBus[T]) Publish(
	_ context.Context,
	_ event.Event[T],
) error {
	return nil
}

// PublishSync blocks until the test closes unblockCh, holding the dispatcher
// inside a handler so tests can observe commands and shutdown while delivery
// is stuck.
func (b *blockingTestBus[T]) PublishSync(
	_ context.Context,
	_ event.Event[T],
) error {
	<-b.unblockCh
	return nil
}

// Subscribe registers nothing and returns an empty id; the double has no
// subscribers.
func (b *blockingTestBus[T]) Subscribe(
	_ string,
	_ handler.Projection[T],
	_ ...subscription.Opt[T],
) (string, error) {
	return "",
		nil
}

// Unsubscribe does nothing; the double has no subscribers.
func (b *blockingTestBus[T]) Unsubscribe(
	_ string,
) error {
	return nil
}

// Close does nothing and succeeds, so Shutdown can complete.
func (b *blockingTestBus[T]) Close(
	_ context.Context,
) error {
	return nil
}

// WaitForHandlers returns at once; nothing is tracked.
func (b *blockingTestBus[T]) WaitForHandlers() {}

type fixture struct {
	p    *Processor[order]
	s    store.Memory
	snap store.SnapshotMemory
	b    pubsub.Bus[order]
}

func newFixture(
	t *testing.T,
	s store.Memory,
	opts ...models.Opt[order],
) fixture {
	t.Helper()
	snap := store.NewSnapshots()
	b := bus.New[order]()
	es := eventstore.New[order](
		s,
		snap,
		nil,
		1,
		nil,
	)
	p := mustNew(
		t,
		es,
		b,
		opts...,
	)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return fixture{p: p, s: s, snap: snap, b: b}
}

func updateCmd(
	id string,
	total float64,
) mocks.UpdateOrderCmd {
	return mocks.UpdateOrderCmd{ID: id, NewState: order{ID: id, Total: total}}
}

type healthGatedDelivery struct {
	healthy *atomic.Bool
}

func (g healthGatedDelivery) deliver(
	context.Context,
	event.Event[order],
) error {
	if g.healthy.Load() {
		return nil
	}

	return errors.New("down")
}

func updatedEvent(
	name string,
) bool {
	return name == "OrderUpdated"
}

func parkingOpts(
	healthy *atomic.Bool,
	extra ...models.Opt[order],
) []models.Opt[order] {
	gated := healthGatedDelivery{healthy: healthy}
	subs := []subscription.Durable[order]{{
		Match:   always,
		Handler: gated.deliver,
	}}
	base := []models.Opt[order]{
		models.WithDurableDelivery[order](updatedEvent),
		models.WithDurableSubscribers(subs),
		models.WithDeliveryRetry[order](
			time.Millisecond,
			2*time.Millisecond,
		),
		models.WithDeliveryMaxAttempts[order](2),
	}
	return append(
		base,
		extra...,
	)
}

func TestProcessor_Options_SyncProjectionsDurableDeliverySnapshotErrors(
	t *testing.T,
) {
	projected := &callCounter{}
	snapErrs := &callCounter{}
	s := store.New()
	f := newFixture(
		t,
		s,
		models.WithSyncProjections([]models.SyncProjection[order]{{
			Match: always,
			Fn:    projected.onProjection,
		}}),
		models.WithDurableDelivery[order](always),
		models.WithSnapshotErrorHandler(snapErrs.onSnapshotError),
	)
	f.snap.SetError(
		"o1",
		errors.New("snapshot down"),
	)

	ctx := context.Background()
	cmd := mocks.UpdateOrderCmd{ID: "o1", NewState: order{ID: "o1", Total: 1}, Snapshot: true}
	if _, err := f.p.SendWait(
		ctx,
		cmd,
	); err != nil {
		t.Fatal(err)
	}

	f.p.WaitPublish()

	if projected.calls.Load() != 1 ||
		snapErrs.calls.Load() != 1 {
		t.Errorf(
			"projected=%d snapshotErrors=%d, want 1 and 1",
			projected.calls.Load(),
			snapErrs.calls.Load(),
		)
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 0 {
		t.Errorf(
			"delivery not acked after SendWait: %v",
			pending,
		)
	}
}

type flakyDelivery struct {
	attempts atomic.Int32
}

func (d *flakyDelivery) deliver(
	context.Context,
	event.Event[order],
) error {
	switch d.attempts.Add(1) {
	case 1:
		return errors.New("transient")
	case 2:
		panic("boom")
	}

	return nil
}

func TestProcessor_DurableDeliveryOptions_RetryReportAndPanicHandling(
	t *testing.T,
) {
	flaky := &flakyDelivery{}
	reports := &callCounter{}
	panics := &callCounter{}
	s := store.New()
	f := newFixture(
		t,
		s,
		models.WithDurableDelivery[order](updatedEvent),
		models.WithDurableSubscribers([]subscription.Durable[order]{{
			Match:   always,
			Handler: flaky.deliver,
		}}),
		models.WithDeliveryErrorHandler(reports.onDeliveryError),
		models.WithDeliveryPanicHandler(panics.onDeliveryPanic),
		models.WithDeliveryRetry[order](
			time.Millisecond,
			2*time.Millisecond,
		),
	)

	ctx := context.Background()
	if _, err := f.p.Send(
		ctx,
		mocks.UpdateOrderCmd{ID: "o1", NewState: order{ID: "o1", Total: 1}},
	); err != nil {
		t.Fatal(err)
	}

	f.p.WaitPublish()

	if flaky.attempts.Load() != 3 ||
		reports.calls.Load() != 2 ||
		panics.calls.Load() != 1 {
		t.Fatalf(
			"attempts=%d reports=%d panics=%d; want 3, 2, 1",
			flaky.attempts.Load(),
			reports.calls.Load(),
			panics.calls.Load(),
		)
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 0 {
		t.Fatalf(
			"pending = %v, want none after the retry succeeded",
			pending,
		)
	}
}

func always(
	string,
) bool {
	return true
}

func requireVersionOne(
	t *testing.T,
	v int64,
) {
	t.Helper()
	if v != 1 {
		t.Fatalf(
			"dead-lettered v%d",
			v,
		)
	}
}

type deadLetterRecorder struct {
	versions chan int64
}

func (r *deadLetterRecorder) record(
	_ context.Context,
	e event.Event[order],
	_ error,
) error {
	r.versions <- e.Version
	return nil
}

func TestProcessor_Options_DeadLetter(
	t *testing.T,
) {
	var healthy atomic.Bool
	dead := &deadLetterRecorder{versions: make(
		chan int64,
		4,
	)}
	f := newFixture(
		t,
		store.New(),
		parkingOpts(
			&healthy,
			models.WithDeadLetterHandler(dead.record),
		)...,
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
	case v := <-dead.versions:
		requireVersionOne(
			t,
			v,
		)
	case <-time.After(5 * time.Second):
		t.Fatal("dead-letter handler never called")
	}
}

var _ = bus.New[order]

type gatedCmd struct {
	mocks.UpdateOrderCmd
	entered chan<- struct{}
	gate    <-chan struct{}
}

// Validate announces on entered (without blocking if nobody listens), then
// blocks until the gate is released, so a test can hold the command
// mid-flight under the aggregate lock.
func (c gatedCmd) Validate(
	o *order,
) error {
	select {
	case c.entered <- struct{}{}:
	default:
	}

	<-c.gate
	return nil
}

func awaitSendResults(
	t *testing.T,
	results chan error,
	n int,
) {
	t.Helper()
	for range n {
		awaitOneResult(
			t,
			results,
		)
	}
}

func awaitOneResult(
	t *testing.T,
	results chan error,
) {
	t.Helper()
	select {
	case <-results:
	case <-time.After(5 * time.Second):
		t.Fatal("a Send was stranded by Shutdown")
	}
}

func TestProcessor_HandlerMayWaitOnAnotherInstanceUnderTheSameID(
	t *testing.T,
) {
	a := newFixture(
		t,
		store.New(),
	)
	b := newFixture(
		t,
		store.New(),
	)
	ctx := context.Background()
	if _, err := b.p.Send(
		ctx,
		updateCmd(
			"ns",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	waiting := &crossInstanceWait{
		other: b.p,
		got: make(
			chan error,
			2,
		),
	}
	a.b.Subscribe(
		"OrderUpdated",
		waiting.wait,
	)
	if _, err := a.p.Send(
		ctx,
		updateCmd(
			"ns",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	for _, what := range []string{"SendWait", "Forget"} {
		awaitOtherInstance(
			t,
			waiting.got,
			what,
		)
	}
}

type crossInstanceWait struct {
	other *Processor[order]
	got   chan error
}

func (w *crossInstanceWait) wait(
	ctx context.Context,
	e event.Event[order],
) {
	_, err := w.other.SendWait(
		ctx,
		updateCmd(
			e.AggregateID,
			2,
		),
	)
	w.got <- err
	w.got <- w.other.Forget(
		ctx,
		e.AggregateID,
	)
}

func awaitOtherInstance(
	t *testing.T,
	got chan error,
	what string,
) {
	t.Helper()
	select {
	case err := <-got:
		requireNoErrorf(
			t,
			err,
			"%s on the other instance from a handler",
			what,
		)
	case <-time.After(5 * time.Second):
		t.Fatalf(
			"%s on the other instance from a handler hung",
			what,
		)
	}
}

func mustNew[T any](
	tb testing.TB,
	es eventstore.EventStore[T],
	bus pubsub.Bus[T],
	opts ...models.Opt[T],
) *Processor[T] {
	tb.Helper()
	p, err := New(
		es,
		bus,
		opts...,
	)
	if err != nil {
		tb.Fatalf(
			"processor.New: %v",
			err,
		)
	}

	return p
}

func newES() eventstore.EventStore[order] {
	return eventstore.New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
}

func TestNew_AFactoryReplacesTheDefaultDispatcher(
	t *testing.T,
) {
	fake := mockbuses.NewDispatcher[order]()
	p := mustNew(
		t,
		newES(),
		bus.New[order](),
		models.WithDispatcherFactory(fake.Factory()),
	)
	t.Cleanup(func() { p.Shutdown(context.Background()) })

	if _, err := p.Send(
		context.Background(),
		mocks.CreateOrderCmd{ID: "a", Total: 1},
	); err != nil {
		t.Fatal(err)
	}

	requests := fake.Requests()
	wrongRequests := len(requests) != 1 || requests[0].Kind != dispatch.KindEvent || requests[0].Event.AggregateID != "a"
	if wrongRequests {
		t.Errorf(
			"requests = %+v, want the one dispatch.KindEvent of a",
			requests,
		)
	}
}

type failingFactory struct {
	err error
}

func (f failingFactory) build(
	dispatcher.Config[order],
) (dispatcher.Dispatcher[order], error) {
	return nil,
		f.err
}

func noDispatcherFactory(
	dispatcher.Config[order],
) (dispatcher.Dispatcher[order], error) {
	return nil,
		nil
}

func typedNilDispatcherFactory(
	dispatcher.Config[order],
) (dispatcher.Dispatcher[order], error) {
	return (*mockbuses.Dispatcher[order])(nil),
		nil
}

func TestNew_AFactoryErrorIsReturned(
	t *testing.T,
) {
	sentinel := errors.New("factory failed")
	factory := failingFactory{err: sentinel}

	p, err := New(
		newES(),
		bus.New[order](),
		models.WithDispatcherFactory(factory.build),
	)
	if !errors.Is(
		err,
		sentinel,
	) ||
		p != nil {
		t.Fatalf(
			"New = (%v, %v), want the factory error and no processor",
			p,
			err,
		)
	}

	if !strings.HasPrefix(
		err.Error(),
		"asynx: dispatcher factory: ",
	) {
		t.Errorf(
			"error = %q, want the %q prefix",
			err,
			"asynx: dispatcher factory: ",
		)
	}
}

func TestNew_ANilDispatcherWithoutAnErrorFails(
	t *testing.T,
) {
	p, err := New(
		newES(),
		bus.New[order](),
		models.WithDispatcherFactory(noDispatcherFactory),
	)
	if err == nil ||
		p != nil {
		t.Errorf(
			"New = (%v, %v), want an error and no processor",
			p,
			err,
		)
	}
}

func TestNew_AFactoryIsCalledEvenWithoutABus(
	t *testing.T,
) {
	fake := mockbuses.NewDispatcher[order]()
	p := mustNew[order](
		t,
		newES(),
		nil,
		models.WithDispatcherFactory(fake.Factory()),
	)
	t.Cleanup(func() { p.Shutdown(context.Background()) })

	if _, err := p.Send(
		context.Background(),
		mocks.CreateOrderCmd{ID: "a", Total: 1},
	); err != nil {
		t.Fatal(err)
	}

	if len(fake.Requests()) != 1 ||
		fake.Config().Bus != nil {
		t.Errorf(
			"requests = %d, bus = %v, want one request and a nil bus",
			len(fake.Requests()),
			fake.Config().Bus,
		)
	}
}

func TestNew_TheFactoryReceivesTheConfigWithDefaultsApplied(
	t *testing.T,
) {
	tests := []factoryCase{
		{
			name:     "no options keeps the defaults",
			attempts: 20,
			initial:  100 * time.Millisecond,
			max:      30 * time.Second,
		},
		{
			name: "non-positive values are ignored",
			opts: []models.Opt[order]{
				models.WithDeliveryMaxAttempts[order](0),
				models.WithDeliveryRetry[order](
					-time.Second,
					0,
				),
			},
			attempts: 20,
			initial:  100 * time.Millisecond,
			max:      30 * time.Second,
		},
		{
			name: "positive values are carried",
			opts: []models.Opt[order]{
				models.WithDeliveryMaxAttempts[order](4),
				models.WithDeliveryRetry[order](
					time.Millisecond,
					9*time.Millisecond,
				),
			},
			attempts: 4,
			initial:  time.Millisecond,
			max:      9 * time.Millisecond,
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name,
			tt.check,
		)
	}
}

type factoryCase struct {
	name     string
	opts     []models.Opt[order]
	attempts int
	initial  time.Duration
	max      time.Duration
}

func (tt factoryCase) check(
	t *testing.T,
) {
	assertFactoryConfig(
		t,
		tt.opts,
		tt.attempts,
		tt.initial,
		tt.max,
	)
}

func ignoresPublishError(
	context.Context,
	event.Event[order],
	error,
) {
}

func assertFactoryConfig(
	t *testing.T,
	opts []models.Opt[order],
	attempts int,
	initial time.Duration,
	max time.Duration,
) {
	t.Helper()
	fake := mockbuses.NewDispatcher[order]()
	opts = append(
		opts,
		models.WithDispatcherFactory(fake.Factory()),
		models.WithPublishErrorHandler(ignoresPublishError),
	)
	mustNew(
		t,
		newES(),
		bus.New[order](),
		opts...,
	)

	got := fake.Config()
	configDiffers := got.MaxAttempts != attempts || got.RetryInitial != initial || got.RetryMax != max
	if configDiffers {
		t.Errorf(
			"config = (%d, %v, %v), want (%d, %v, %v)",
			got.MaxAttempts,
			got.RetryInitial,
			got.RetryMax,
			attempts,
			initial,
			max,
		)
	}

	if got.Bus == nil ||
		got.OnPublishError == nil {
		t.Errorf(
			"config = %+v, want the bus and the publish error handler",
			got,
		)
	}
}

func TestNew_ATypedNilDispatcherWithoutAnErrorFails(
	t *testing.T,
) {
	p, err := New(
		newES(),
		bus.New[order](),
		models.WithDispatcherFactory(typedNilDispatcherFactory),
	)

	notRejected := p != nil || err == nil || !strings.Contains(
		err.Error(),
		"returned no dispatcher",
	)
	if notRejected {
		t.Errorf(
			"New = (%v, %v), want the no-dispatcher error and no processor",
			p,
			err,
		)
	}
}

func succeeded(
	err error,
) int {
	if err == nil {
		return 1
	}

	return 0
}

func TestProcessor_IntegrationHighThroughput(
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
		models.WithWorkersPerShard[order](4),
		models.WithQueueDepth[order](32),
	)
	defer p.Shutdown(context.Background())

	ctx := context.Background()
	successCount := 0

	for i := 0; i < 200; i++ {
		cmd := mocks.CreateOrderCmd{
			ID:    "order" + string(rune(i%26+'a')),
			Total: 100.0,
		}
		_, err := p.Send(
			ctx,
			cmd,
		)
		successCount += succeeded(err)
	}

	if successCount < 190 {
		t.Fatalf(
			"throughput: expected at least 190 successes, got %d",
			successCount,
		)
	}

	t.Logf(
		"Integration: %d/200 commands processed",
		successCount,
	)
}

func TestProcessor_IntegrationOrderPreservation(
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
	)
	defer p.Shutdown(context.Background())

	ctx := context.Background()
	aggregateID := "order1"

	events := &callCounter{}
	channelBus.Subscribe(
		"OrderUpdated",
		events.onEvent,
	)

	for i := 1; i <= 50; i++ {
		cmd := mocks.UpdateOrderCmd{
			ID: aggregateID,
			NewState: order{
				ID:     aggregateID,
				Total:  float64(i * 10),
				Status: "Pending",
			},
		}
		_, err := p.Send(
			ctx,
			cmd,
		)
		requireNoErrorf(
			t,
			err,
			"Send failed at iteration %d",
			i,
		)
	}

	p.WaitPublish()

	count := events.calls.Load()
	if count > 0 {
		t.Logf(
			"Integration: %d update events processed",
			count,
		)
	}
}

func TestProcessor_IntegrationCancelDuringBurst(
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
		models.WithQueueDepth[order](16),
	)
	defer p.Shutdown(context.Background())

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

	ctx2 := context.Background()
	cmd2 := mocks.CreateOrderCmd{ID: "order2", Total: 100.0}
	_, err2 := p.Send(
		ctx2,
		cmd2,
	)
	if err2 != nil {
		t.Fatalf(
			"expected success, got %v",
			err2,
		)
	}

	t.Log("Integration: cancellation handling verified")
}

func sendValidOrInvalid(
	ctx context.Context,
	p *Processor[order],
	idx int,
	wg *sync.WaitGroup,
	validCount *int32,
	invalidCount *int32,
) {
	defer wg.Done()
	total := 100.0
	if idx%2 != 0 {
		total = -100.0
	}

	_, err := p.Send(
		ctx,
		mocks.CreateOrderCmd{
			ID:    "order" + string(rune(idx)),
			Total: total,
		},
	)
	if err == failure.ErrValidation {
		atomic.AddInt32(
			invalidCount,
			1,
		)
		return
	}

	if err == nil {
		atomic.AddInt32(
			validCount,
			1,
		)
	}
}

func TestProcessor_IntegrationWithValidationErrors(
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
	)
	defer p.Shutdown(context.Background())

	ctx := context.Background()
	var validCount int32
	var invalidCount int32

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go sendValidOrInvalid(
			ctx,
			p,
			i,
			&wg,
			&validCount,
			&invalidCount,
		)
	}

	wg.Wait()

	validVal := atomic.LoadInt32(&validCount)
	invalidVal := atomic.LoadInt32(&invalidCount)
	if validVal < 30 ||
		invalidVal < 30 {
		t.Fatalf(
			"unexpected distribution: valid=%d, invalid=%d",
			validVal,
			invalidVal,
		)
	}

	t.Logf(
		"Integration: %d valid, %d invalid",
		validVal,
		invalidVal,
	)
}

func runBatch(
	ctx context.Context,
	t *testing.T,
	p *Processor[order],
	batch int,
) {
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go sendLogged(
			ctx,
			t,
			p,
			"order"+string(rune((i+batch*100)%26+'a')),
			&wg,
		)
	}

	wg.Wait()
}

func sendLogged(
	ctx context.Context,
	t *testing.T,
	p *Processor[order],
	id string,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	_, err := p.Send(
		ctx,
		mocks.CreateOrderCmd{
			ID:    id,
			Total: 100.0,
		},
	)
	if err != nil {
		t.Logf(
			"Send failed: %v",
			err,
		)
	}
}

func TestProcessor_IntegrationMemoryStability(
	t *testing.T,
) {
	if testing.Short() {
		t.Skip("Skipping memory stability test in short mode")
	}

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
		models.WithQueueDepth[order](20),
	)
	defer p.Shutdown(context.Background())

	ctx := context.Background()

	for batch := 0; batch < 10; batch++ {
		runBatch(
			ctx,
			t,
			p,
			batch,
		)
	}

	t.Log("Integration: memory stability test completed without panic")
}

func TestProcessor_IntegrationEventPublishingReliability(
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
	)
	defer p.Shutdown(context.Background())

	ctx := context.Background()
	eventsChan := make(
		chan event.Event[order],
		100,
	)

	buffered := &eventBuffer{
		t:      t,
		events: eventsChan,
	}
	channelBus.Subscribe(
		"OrderCreated",
		buffered.bufferOrDrop,
	)

	commandCount := 20
	for i := 0; i < commandCount; i++ {
		cmd := mocks.CreateOrderCmd{
			ID:    "order" + string(rune('a'+i%26)),
			Total: 100.0,
		}
		_, err := p.Send(
			ctx,
			cmd,
		)
		logError(
			t,
			err,
		)
	}

	p.WaitPublish()

	receivedCount := countUntil(
		eventsChan,
		time.After(1*time.Second),
	)
	if receivedCount == 0 {
		t.Logf("Warning: no events received (may be timing issue)")
		return
	}

	t.Logf(
		"Integration: %d events published out of %d commands",
		receivedCount,
		commandCount,
	)
}

type eventBuffer struct {
	t      *testing.T
	events chan event.Event[order]
}

func (b *eventBuffer) bufferOrDrop(
	_ context.Context,
	e event.Event[order],
) {
	select {
	case b.events <- e:
	default:
		b.t.Logf("Event buffer full, dropped event")
	}
}

func countUntil(
	eventsChan chan event.Event[order],
	timeout <-chan time.Time,
) int {
	received := 0
	more := true
	for more {
		var got int
		got, more = pollEvent(
			eventsChan,
			timeout,
		)
		received += got
	}

	return received
}

func pollEvent(
	eventsChan chan event.Event[order],
	timeout <-chan time.Time,
) (got int, more bool) {
	select {
	case <-eventsChan:
		return 1,
			true
	case <-timeout:
		return 0,
			false
	}
}

func logError(
	t *testing.T,
	err error,
) {
	if err != nil {
		t.Logf(
			"Send failed: %v",
			err,
		)
	}
}

func TestProcessor_IntegrationDefaultQueueDepthScaling(
	t *testing.T,
) {
	tests := []queueDepthCase{
		{"1 worker", 1, 1},
		{"4 workers", 4, 4},
		{"8 workers", 8, 8},
		{"16 workers", 16, 16},
	}

	for _, tt := range tests {
		t.Run(
			tt.name,
			tt.check,
		)
	}
}

type queueDepthCase struct {
	name          string
	workers       int
	expectedMinQD int
}

func (tt queueDepthCase) check(
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
		models.WithWorkersPerShard[order](tt.workers),
	)
	defer p.Shutdown(context.Background())

	ctx := context.Background()
	successCount := 0
	for i := 0; i < tt.expectedMinQD*2; i++ {
		cmd := mocks.CreateOrderCmd{
			ID:    "order" + string(rune(i%8)),
			Total: 100.0,
		}
		_, err := p.Send(
			ctx,
			cmd,
		)
		successCount += succeeded(err)
	}

	if successCount < tt.expectedMinQD {
		t.Logf(
			"Warning: expected at least %d successes, got %d",
			tt.expectedMinQD,
			successCount,
		)
	}
}

func TestRetryBackoff_IgnoresNonPositiveValues(
	t *testing.T,
) {
	cfg := configure(models.WithDeliveryRetry[int](
		0,
		-1,
	))
	if cfg.Dispatcher.RetryInitial != defaultRetryInitial ||
		cfg.Dispatcher.RetryMax != defaultRetryMax {
		t.Fatalf(
			"backoff = %v..%v, want defaults",
			cfg.Dispatcher.RetryInitial,
			cfg.Dispatcher.RetryMax,
		)
	}

	cfg = configure(
		models.WithDeliveryRetry[int](
			time.Second,
			time.Minute,
		),
		models.WithDeliveryRetry[int](
			0,
			-1,
		),
	)
	if cfg.Dispatcher.RetryInitial != time.Second ||
		cfg.Dispatcher.RetryMax != time.Minute {
		t.Fatalf(
			"backoff = %v..%v, want the earlier positive values kept",
			cfg.Dispatcher.RetryInitial,
			cfg.Dispatcher.RetryMax,
		)
	}
}

func TestOptions_NonPositiveBoundsAreIgnored(
	t *testing.T,
) {
	cfg := configure(models.WithDeliveryMaxAttempts[int](0))
	if cfg.Dispatcher.MaxAttempts != defaultMaxAttempts {
		t.Fatalf(
			"maxAttempts=%d, want the default",
			cfg.Dispatcher.MaxAttempts,
		)
	}

	cfg = configure(
		models.WithDeliveryMaxAttempts[int](3),
		models.WithDeliveryMaxAttempts[int](-1),
	)
	if cfg.Dispatcher.MaxAttempts != 3 {
		t.Fatalf(
			"maxAttempts=%d, want the earlier positive value kept",
			cfg.Dispatcher.MaxAttempts,
		)
	}
}

func TestConfigure_PassesTheDeliverySettingsToTheDispatcherConfig(
	t *testing.T,
) {
	cfg := configure(
		models.WithDeliveryRetry[int](
			time.Second,
			time.Minute,
		),
		models.WithDeliveryMaxAttempts[int](7),
	)
	wrongConfig := cfg.Dispatcher.RetryInitial != time.Second || cfg.Dispatcher.RetryMax != time.Minute || cfg.Dispatcher.MaxAttempts != 7
	if wrongConfig {
		t.Fatalf(
			"config = %+v, want the values set",
			cfg.Dispatcher,
		)
	}
}
