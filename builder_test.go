package asynx_test

import (
	"context"
	"errors"
	"github.com/char2cs/asynx"
	"github.com/char2cs/asynx/internal/mockbuses"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/models/command"
	"github.com/char2cs/asynx/models/dispatch"
	"github.com/char2cs/asynx/models/dispatcher"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/failure"
	"github.com/char2cs/asynx/store"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestNewBuilderDefaults(
	t *testing.T,
) {
	b := asynx.New[mocks.Order]()
	if b == nil {
		t.Fatal("New returned nil")
	}
}

func TestShardingOptsZeroValue(
	t *testing.T,
) {
	opts := asynx.ShardingOpts{}
	if opts.Shards != 0 ||
		opts.QueueDepth != 0 ||
		opts.WorkersPerShard != 0 {
		t.Errorf(
			"unexpected zero ShardingOpts: %+v",
			opts,
		)
	}
}

func TestShardingOpts_CustomShards(
	t *testing.T,
) {
	s := store.New()
	instance, err := asynx.New[mocks.Order]().
		WithEventStore(s).
		WithSnapshotStore(store.NewSnapshots()).
		WithShardingOpts(asynx.ShardingOpts{Shards: 16, QueueDepth: 100}).
		Build()
	if err != nil {
		t.Fatal(err)
	}

	defer instance.Shutdown(context.Background())

	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	_, err = instance.Send(
		context.Background(),
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"Send with custom shards failed: %v",
			err,
		)
	}
}

type forgetTotal struct {
	mu    sync.Mutex
	total float64
}

func (f *forgetTotal) handle(
	_ context.Context,
	e event.Event[mocks.Order],
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.total = e.Aggregate.Total
}

func (f *forgetTotal) get() float64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.total
}

func TestWithForgetHandler_IsCalledOnForget(
	t *testing.T,
) {
	got := &forgetTotal{}

	instance, err := asynx.New[mocks.Order]().
		WithEventStore(store.New()).
		WithSnapshotStore(store.NewSnapshots()).
		WithShardingOpts(asynx.ShardingOpts{Shards: 8, QueueDepth: 1000}).
		WithForgetHandler(got.handle).
		Build()
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { instance.Shutdown(context.Background()) })

	ctx := context.Background()
	if _, err := instance.Send(
		ctx,
		mocks.CreateOrderCmd{ID: "order-1", Total: 77.0},
	); err != nil {
		t.Fatalf(
			"Send: %v",
			err,
		)
	}

	if err := instance.Forget(
		ctx,
		"order-1",
	); err != nil {
		t.Fatalf(
			"Forget: %v",
			err,
		)
	}

	if gotTotal := got.get(); gotTotal != 77.0 {
		t.Errorf(
			"handler.Forget got Total=%v, want 77.0",
			gotTotal,
		)
	}
}

type idemCmd struct {
	mocks.UpdateOrderCmd
	Cmd string
}

// CommandID returns the idempotency key of the command.
func (c idemCmd) CommandID() string { return c.Cmd }

type idemCreate struct {
	mocks.CreateOrderCmd
	Cmd string
}

// CommandID returns the idempotency key of the command.
func (c idemCreate) CommandID() string { return c.Cmd }

func upd(
	id string,
	total float64,
) mocks.UpdateOrderCmd {
	return mocks.UpdateOrderCmd{ID: id, NewState: mocks.Order{ID: id, Total: total}}
}

func build(
	t *testing.T,
	b *asynx.Builder[mocks.Order],
) asynx.Asynx[mocks.Order] {
	t.Helper()
	inst, err := b.Build()
	if err != nil {
		t.Fatalf(
			"Build: %v",
			err,
		)
	}

	t.Cleanup(func() { inst.Shutdown(context.Background()) })
	return inst
}

func eight(
	s store.Memory,
) *asynx.Builder[mocks.Order] {
	return asynx.New[mocks.Order]().
		WithEventStore(s).
		WithSnapshotStore(store.NewSnapshots()).
		WithShardingOpts(asynx.ShardingOpts{Shards: 1, WorkersPerShard: 8, QueueDepth: 1000})
}

type ackErrStore struct{ store.Memory }

// AckDelivery fails with the configured error.
func (ackErrStore) AckDelivery(
	context.Context,
	string,
	int64,
) error {
	return errors.New("ack failed")
}

type pendingErrStore struct{ store.Memory }

// PendingDeliveries fails with the configured error.
func (pendingErrStore) PendingDeliveries(
	context.Context,
	int,
) ([]event.Delivery, error) {
	return nil,
		errors.New("delivery log unreadable")
}

func eventually(
	t *testing.T,
	what string,
	cond func() bool,
) {
	t.Helper()
	deadline := time.Now().Add(eventuallyTimeout)
	for !cond() {
		failIfPast(
			t,
			deadline,
			what,
		)
		runtime.Gosched()
	}
}

const eventuallyTimeout = 5 * time.Second

func failIfPast(
	t *testing.T,
	deadline time.Time,
	what string,
) {
	t.Helper()

	if time.Now().After(deadline) {
		t.Fatalf(
			"timed out waiting for %s",
			what,
		)
	}
}

type gatedUpdate struct {
	mocks.UpdateOrderCmd
	entered chan<- struct{}
	gate    <-chan struct{}
}

// Validate checks the command against the current aggregate state.
func (c gatedUpdate) Validate(
	*mocks.Order,
) error {
	c.entered <- struct{}{}
	<-c.gate
	return nil
}

func sendHot(
	ctx context.Context,
	inst asynx.Asynx[mocks.Order],
	refused chan<- struct{},
) {
	_, err := inst.Send(
		ctx,
		upd(
			"hot",
			3,
		),
	)
	if errors.Is(
		err,
		failure.ErrQueueFull,
	) {
		refused <- struct{}{}
	}
}

func noopDurable(
	context.Context,
	event.Event[mocks.Order],
) error {
	return nil
}

func sendAll(
	t *testing.T,
	inst asynx.Asynx[mocks.Order],
	cmds ...command.Command[mocks.Order],
) {
	t.Helper()
	for _, cmd := range cmds {
		mustSend(
			t,
			inst,
			cmd,
		)
	}
}

func mustSend(
	t *testing.T,
	inst asynx.Asynx[mocks.Order],
	cmd command.Command[mocks.Order],
) {
	t.Helper()
	_, err := inst.Send(
		context.Background(),
		cmd,
	)
	if err != nil {
		t.Fatal(err)
	}
}

func assertRedelivery(
	t *testing.T,
	i int,
	request dispatch.Request[mocks.Order],
) {
	t.Helper()
	wrongRedelivery := request.Kind != dispatch.KindRedelivery || request.Event.Version != int64(i+2) || request.Ack == nil
	if wrongRedelivery {
		t.Errorf(
			"request %d = %+v, want the redelivery of v%d with an Ack",
			i,
			request,
			i+2,
		)
	}
}

func assertFactoryConfig(
	t *testing.T,
	retry [2]time.Duration,
	attempts int,
	want dispatcher.Config[mocks.Order],
) {
	t.Helper()
	fake := mockbuses.NewDispatcher[mocks.Order]()
	b := eight(store.New()).
		WithDispatcher(fake.Factory()).
		WithSubscriber(
			"OrderUpdated",
			noopDurable,
		).
		WithDeliveryRetry(
			retry[0],
			retry[1],
		).
		WithDeliveryMaxAttempts(attempts)
	build(
		t,
		b,
	)

	got := fake.Config()
	configDiffers := got.MaxAttempts != want.MaxAttempts || got.RetryInitial != want.RetryInitial || got.RetryMax != want.RetryMax
	if configDiffers {
		t.Errorf(
			"config = (%d, %v, %v), want (%d, %v, %v)",
			got.MaxAttempts,
			got.RetryInitial,
			got.RetryMax,
			want.MaxAttempts,
			want.RetryInitial,
			want.RetryMax,
		)
	}

	if got.Bus == nil ||
		len(got.DurableSubs) != 1 {
		t.Errorf(
			"config = %+v, want the bus and the one durable subscriber",
			got,
		)
	}
}
