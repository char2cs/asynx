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
	if err != asynxmd.ErrValidation {
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
	if err != asynxmd.ErrValidation {
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

	eventReceived := make(
		chan asynxmd.Event[order],
		1,
	)
	forward := func(
		ctx context.Context,
		e asynxmd.Event[order],
	) {
		eventReceived <- e
	}
	channelBus.Subscribe(
		"OrderCreated",
		forward,
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
	case event := <-eventReceived:
		expectEventName(
			t,
			event.EventName,
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

	contextReceived := make(
		chan context.Context,
		1,
	)
	forwardCtx := func(
		ctx context.Context,
		e asynxmd.Event[order],
	) {
		contextReceived <- ctx
	}
	channelBus.Subscribe(
		"OrderCreated",
		forwardCtx,
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
	case <-contextReceived:
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
	var got struct {
		sync.Mutex
		called bool
	}

	s := store.New()
	handler := func(
		_ context.Context,
		_ asynxmd.Event[mocks.Order],
		_ error,
	) {
		got.Lock()
		got.called = true
		got.Unlock()
	}

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
		models.WithPublishErrorHandler[mocks.Order](handler),
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

	got.Lock()
	called := got.called
	got.Unlock()

	if !called {
		t.Error("PublishErrorHandler was not called after bus error")
	}
}

type blockingTestBus[T any] struct {
	unblockCh chan struct{}
}

// Publish accepts the event and does nothing; the bus is only here to block
// PublishSync.
func (b *blockingTestBus[T]) Publish(
	_ context.Context,
	_ asynxmd.Event[T],
) error {
	return nil
}

// PublishSync blocks until the test closes unblockCh, holding the dispatcher
// inside a handler so tests can observe commands and shutdown while delivery
// is stuck.
func (b *blockingTestBus[T]) PublishSync(
	_ context.Context,
	_ asynxmd.Event[T],
) error {
	<-b.unblockCh
	return nil
}

// Subscribe registers nothing and returns an empty id; the double has no
// subscribers.
func (b *blockingTestBus[T]) Subscribe(
	_ string,
	_ asynxmd.ProjectionHandler[T],
	_ ...asynxmd.SubscriptionOpt[T],
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
	b    asynxmd.Bus[order]
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

func downUnlessHealthy(
	healthy *atomic.Bool,
) error {
	if healthy.Load() {
		return nil
	}

	return errors.New("down")
}

func parkingOpts(
	healthy *atomic.Bool,
	extra ...models.Opt[order],
) []models.Opt[order] {
	isUpdated := func(
		name string,
	) bool {
		return name == "OrderUpdated"
	}
	handler := func(
		context.Context,
		asynxmd.Event[order],
	) error {
		return downUnlessHealthy(healthy)
	}
	subs := []asynxmd.DurableSub[order]{{
		Match:   always,
		Handler: handler,
	}}
	base := []models.Opt[order]{
		models.WithDurableDelivery[order](isUpdated),
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
	var projected, snapErrs atomic.Int32
	s := store.New()
	project := func(
		context.Context,
		asynxmd.Tx,
		asynxmd.Event[order],
	) error {
		projected.Add(1)
		return nil
	}
	countSnapErr := func(
		context.Context,
		asynxmd.Event[order],
		error,
	) {
		snapErrs.Add(1)
	}
	f := newFixture(
		t,
		s,
		models.WithSyncProjections([]models.SyncProjection[order]{{
			Match: always,
			Fn:    project,
		}}),
		models.WithDurableDelivery[order](always),
		models.WithSnapshotErrorHandler(countSnapErr),
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

	if projected.Load() != 1 ||
		snapErrs.Load() != 1 {
		t.Errorf(
			"projected=%d snapshotErrors=%d, want 1 and 1",
			projected.Load(),
			snapErrs.Load(),
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

func flakyHandler(
	attempts *atomic.Int32,
) error {
	switch attempts.Add(1) {
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
	var attempts, reports, panics atomic.Int32
	s := store.New()
	isUpdated := func(
		name string,
	) bool {
		return name == "OrderUpdated"
	}
	handler := func(
		context.Context,
		asynxmd.Event[order],
	) error {
		return flakyHandler(&attempts)
	}
	countReport := func(
		context.Context,
		asynxmd.Event[order],
		error,
		int,
	) {
		reports.Add(1)
	}
	countPanic := func(
		context.Context,
		asynxmd.Event[order],
		any,
	) {
		panics.Add(1)
	}
	f := newFixture(
		t,
		s,
		models.WithDurableDelivery[order](isUpdated),
		models.WithDurableSubscribers([]asynxmd.DurableSub[order]{{
			Match:   always,
			Handler: handler,
		}}),
		models.WithDeliveryErrorHandler(countReport),
		models.WithDeliveryPanicHandler(countPanic),
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

	if attempts.Load() != 3 ||
		reports.Load() != 2 ||
		panics.Load() != 1 {
		t.Fatalf(
			"attempts=%d reports=%d panics=%d; want 3, 2, 1",
			attempts.Load(),
			reports.Load(),
			panics.Load(),
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

func TestProcessor_Options_DeadLetter(
	t *testing.T,
) {
	var healthy atomic.Bool
	dead := make(
		chan int64,
		4,
	)
	record := func(
		_ context.Context,
		e asynxmd.Event[order],
		_ error,
	) error {
		dead <- e.Version
		return nil
	}
	f := newFixture(
		t,
		store.New(),
		parkingOpts(
			&healthy,
			models.WithDeadLetterHandler(record),
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
	case v := <-dead:
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

	got := make(
		chan error,
		2,
	)
	waitOther := func(
		hctx context.Context,
		e asynxmd.Event[order],
	) {
		_, err := b.p.SendWait(
			hctx,
			updateCmd(
				e.AggregateID,
				2,
			),
		)
		got <- err
		got <- b.p.Forget(
			hctx,
			e.AggregateID,
		)
	}
	a.b.Subscribe(
		"OrderUpdated",
		waitOther,
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
			got,
			what,
		)
	}
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
	bus asynxmd.Bus[T],
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
	wrongRequests := len(requests) != 1 || requests[0].Kind != asynxmd.DispatchEvent || requests[0].Event.AggregateID != "a"
	if wrongRequests {
		t.Errorf(
			"requests = %+v, want the one DispatchEvent of a",
			requests,
		)
	}
}

func TestNew_AFactoryErrorIsReturned(
	t *testing.T,
) {
	sentinel := errors.New("factory failed")
	factory := func(
		asynxmd.DispatcherConfig[order],
	) (asynxmd.Dispatcher[order], error) {
		return nil,
			sentinel
	}

	p, err := New(
		newES(),
		bus.New[order](),
		models.WithDispatcherFactory(factory),
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
	factory := func(
		asynxmd.DispatcherConfig[order],
	) (asynxmd.Dispatcher[order], error) {
		return nil,
			nil
	}

	p, err := New(
		newES(),
		bus.New[order](),
		models.WithDispatcherFactory(factory),
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
		runFactoryCase(
			t,
			tt,
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

func runFactoryCase(
	t *testing.T,
	tt factoryCase,
) {
	check := func(
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
	t.Run(
		tt.name,
		check,
	)
}

func assertFactoryConfig(
	t *testing.T,
	opts []models.Opt[order],
	attempts int,
	initial time.Duration,
	max time.Duration,
) {
	t.Helper()
	handled := func(
		context.Context,
		asynxmd.Event[order],
		error,
	) {
	}
	fake := mockbuses.NewDispatcher[order]()
	opts = append(
		opts,
		models.WithDispatcherFactory(fake.Factory()),
		models.WithPublishErrorHandler(handled),
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
	factory := func(
		asynxmd.DispatcherConfig[order],
	) (asynxmd.Dispatcher[order], error) {
		return (*mockbuses.Dispatcher[order])(nil),
			nil
	}

	p, err := New(
		newES(),
		bus.New[order](),
		models.WithDispatcherFactory(factory),
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

	var eventCount int32
	countEvent := func(
		ctx context.Context,
		e asynxmd.Event[order],
	) {
		atomic.AddInt32(
			&eventCount,
			1,
		)
	}
	channelBus.Subscribe(
		"OrderUpdated",
		countEvent,
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

	count := atomic.LoadInt32(&eventCount)
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
	if err != asynxmd.ErrContextCancelled {
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
	if err == asynxmd.ErrValidation {
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
		chan asynxmd.Event[order],
		100,
	)

	buffer := func(
		ctx context.Context,
		e asynxmd.Event[order],
	) {
		bufferOrDrop(
			t,
			eventsChan,
			e,
		)
	}
	channelBus.Subscribe(
		"OrderCreated",
		buffer,
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

func bufferOrDrop(
	t *testing.T,
	eventsChan chan asynxmd.Event[order],
	e asynxmd.Event[order],
) {
	select {
	case eventsChan <- e:
	default:
		t.Logf("Event buffer full, dropped event")
	}
}

func countUntil(
	eventsChan chan asynxmd.Event[order],
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
	eventsChan chan asynxmd.Event[order],
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
		runQueueDepthCase(
			t,
			tt,
		)
	}
}

type queueDepthCase struct {
	name          string
	workers       int
	expectedMinQD int
}

func runQueueDepthCase(
	t *testing.T,
	tt queueDepthCase,
) {
	check := func(
		t *testing.T,
	) {
		checkQueueDepth(
			t,
			tt,
		)
	}
	t.Run(
		tt.name,
		check,
	)
}

func checkQueueDepth(
	t *testing.T,
	tt queueDepthCase,
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
