package asynx_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/char2cs/asynx"
	"github.com/char2cs/asynx/internal/mockbuses"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/mockstores"
	"github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBuildRequiresEventStore(
	t *testing.T,
) {
	_, err := asynx.New[mocks.Order]().Build()
	if err != models.ErrMissingEventStore {
		t.Errorf(
			"expected ErrMissingEventStore, got %v",
			err,
		)
	}
}

func TestBuildSucceeds(
	t *testing.T,
) {
	instance, err := asynx.New[mocks.Order]().
		WithEventStore(&mockstores.Store{}).
		WithSnapshotStore(&mockstores.SnapshotStore{}).
		Build()
	if err != nil {
		t.Fatalf(
			"Build() error: %v",
			err,
		)
	}

	if instance == nil {
		t.Fatal("Build() returned nil instance")
	}
}

func TestBuildMultipleTimesIndependent(
	t *testing.T,
) {
	b := asynx.New[mocks.Order]().
		WithEventStore(&mockstores.Store{}).
		WithSnapshotStore(&mockstores.SnapshotStore{})

	i1, _ := b.Build()
	i2, _ := b.Build()
	if i1 == i2 {
		t.Error("Build() must return a new instance each call")
	}
}

func TestBuilderFluent(
	t *testing.T,
) {
	upcaster := func(
		ctx context.Context,
		eventName string,
		raw []byte,
	) ([]byte, error) {
		return raw,
			nil
	}
	panicHandler := func(
		_ context.Context,
		_ models.Event[mocks.Order],
		_ any,
	) {
	}

	_, err := asynx.New[mocks.Order]().
		WithEventStore(&mockstores.Store{}).
		WithSnapshotStore(&mockstores.SnapshotStore{}).
		WithBus(&mockbuses.Bus[mocks.Order]{}).
		WithShardingOpts(asynx.ShardingOpts{Shards: 16, QueueDepth: 100, WorkersPerShard: 4}).
		WithSchemaVersion(3).
		WithUpcaster(
			1,
			upcaster,
		).
		WithUpcaster(
			2,
			upcaster,
		).
		WithPanicHandler(panicHandler).
		Build()
	if err != nil {
		t.Fatalf(
			"Build() error: %v",
			err,
		)
	}
}

func TestBuilderWithCorruptionHook(
	t *testing.T,
) {
	called := false
	hook := func(
		err error,
	) {
		called = true
	}

	_, err := asynx.New[mocks.Order]().
		WithEventStore(&mockstores.Store{}).
		WithSnapshotStore(&mockstores.SnapshotStore{}).
		WithCorruptionHook(hook).
		Build()
	if err != nil {
		t.Fatalf(
			"Build() error: %v",
			err,
		)
	}

	_ = called
}

func TestBuilderWithPublishErrorHandler(
	t *testing.T,
) {
	called := false
	handler := func(
		_ context.Context,
		_ models.Event[mocks.Order],
		_ error,
	) {
		called = true
	}

	_, err := asynx.New[mocks.Order]().
		WithEventStore(&mockstores.Store{}).
		WithSnapshotStore(&mockstores.SnapshotStore{}).
		WithPublishErrorHandler(handler).
		Build()
	if err != nil {
		t.Fatalf(
			"Build() error: %v",
			err,
		)
	}

	_ = called
}

func TestBuildWithWorkersPerShard(
	t *testing.T,
) {
	instance, err := asynx.New[mocks.Order]().
		WithEventStore(store.New()).
		WithSnapshotStore(store.NewSnapshots()).
		WithShardingOpts(asynx.ShardingOpts{Shards: 8, WorkersPerShard: 1}).
		Build()
	if err != nil {
		t.Fatalf(
			"Build() error: %v",
			err,
		)
	}

	if instance == nil {
		t.Fatal("Build() returned nil instance")
	}

	t.Cleanup(func() { instance.Shutdown(context.Background()) })

	if _, err := instance.Send(
		context.Background(),
		mocks.CreateOrderCmd{ID: "order-1", Total: 10.0},
	); err != nil {
		t.Fatalf(
			"Send: %v",
			err,
		)
	}
}

func TestBuild_NilForgetHandler_ReturnsError(
	t *testing.T,
) {
	_, err := asynx.New[mocks.Order]().
		WithEventStore(&mockstores.Store{}).
		WithSnapshotStore(&mockstores.SnapshotStore{}).
		WithForgetHandler(nil).
		Build()
	if err == nil {
		t.Fatal("expected error for nil ForgetHandler, got nil")
	}
}

func TestBuilder_MissingSnapshotStoreIsAnError(
	t *testing.T,
) {
	_, err := asynx.New[mocks.Order]().
		WithEventStore(store.New()).
		Build()
	if !errors.Is(
		err,
		models.ErrMissingSnapshotStore,
	) {
		t.Errorf(
			"err = %v, want ErrMissingSnapshotStore",
			err,
		)
	}
}

func TestBuilder_SnapshotStoreSatisfiesBuild(
	t *testing.T,
) {
	ax, err := asynx.New[mocks.Order]().
		WithEventStore(store.New()).
		WithSnapshotStore(store.NewSnapshots()).
		Build()
	if err != nil {
		t.Fatalf(
			"build: %v",
			err,
		)
	}

	if ax == nil {
		t.Fatal("instance = nil")
	}
}

type floodLog struct {
	mu   sync.Mutex
	seen []int64
}

func (l *floodLog) handle(
	_ context.Context,
	e models.Event[mocks.Order],
) {
	time.Sleep(100 * time.Microsecond)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seen = append(
		l.seen,
		e.Version,
	)
}

func (l *floodLog) snapshot() []int64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	return slices.Clone(l.seen)
}

func sendFlood(
	t *testing.T,
	inst asynx.Asynx[mocks.Order],
	wg *sync.WaitGroup,
	i int,
) {
	defer wg.Done()

	_, err := inst.Send(
		context.Background(),
		upd(
			"agg",
			float64(i+1),
		),
	)
	if err != nil {
		t.Errorf(
			"Send: %v",
			err,
		)
	}
}

func requireInVersionOrder(
	t *testing.T,
	seen []int64,
) {
	t.Helper()

	want := make(
		[]int64,
		len(seen),
	)
	for i := range want {
		want[i] = int64(i + 1)
	}

	if !slices.Equal(
		seen,
		want,
	) {
		t.Fatalf(
			"out-of-order delivery: %v",
			seen,
		)
	}
}

func TestGuarantee_SameAggregateFloodWithManyWorkers(
	t *testing.T,
) {
	inst := build(
		t,
		eight(store.New()),
	)

	log := &floodLog{}
	inst.Subscribe(
		"OrderUpdated",
		log.handle,
	)

	const n = 100
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go sendFlood(
			t,
			inst,
			&wg,
			i,
		)
	}

	wg.Wait()
	waitPublish(
		t,
		inst,
	)

	seen := log.snapshot()
	if len(seen) != n {
		t.Fatalf(
			"handler saw %d events, want %d",
			len(seen),
			n,
		)
	}

	requireInVersionOrder(
		t,
		seen,
	)
}

type versionLog struct {
	mu       sync.Mutex
	versions []int64
}

func (l *versionLog) handle(
	_ context.Context,
	e models.Event[mocks.Order],
) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.versions = append(
		l.versions,
		e.Version,
	)

	return nil
}

func (l *versionLog) snapshot() []int64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	return slices.Clone(l.versions)
}

func blockUntil(
	release <-chan struct{},
) func(context.Context, models.Event[mocks.Order]) error {
	return func(
		context.Context,
		models.Event[mocks.Order],
	) error {
		<-release
		return nil
	}
}

func TestGuarantee_DurableSubscriberRecoversAfterCrash(
	t *testing.T,
) {
	s := store.New()
	ctx := context.Background()

	stuck := make(chan struct{})
	first := build(
		t,
		eight(s).WithSubscriber(
			"OrderUpdated",
			blockUntil(stuck),
		),
	)
	t.Cleanup(func() { close(stuck) })
	for i := range 3 {
		sendUpdate(
			t,
			first,
			float64(i+1),
		)
	}

	log := &versionLog{}
	second := build(
		t,
		eight(s).WithSubscriber(
			"OrderUpdated",
			log.handle,
		),
	)

	sendUpdate(
		t,
		second,
		4,
	)
	waitPublish(
		t,
		second,
	)

	versions := log.snapshot()
	if !slices.Equal(
		versions,
		[]int64{1, 2, 3, 4},
	) {
		t.Fatalf(
			"delivered versions = %v, want [1 2 3 4]",
			versions,
		)
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 0 {
		t.Fatalf(
			"pending after recovery = %v, want none",
			pending,
		)
	}
}

func sendUpdate(
	t *testing.T,
	inst asynx.Asynx[mocks.Order],
	total float64,
) {
	t.Helper()

	_, err := inst.Send(
		context.Background(),
		upd(
			"agg",
			total,
		),
	)
	if err != nil {
		t.Fatal(err)
	}
}

func countingSubscriber(
	calls *atomic.Int32,
) func(context.Context, models.Event[mocks.Order]) error {
	return func(
		context.Context,
		models.Event[mocks.Order],
	) error {
		calls.Add(1)
		return nil
	}
}

func TestGuarantee_RecoverySkipsDeliveriesWhoseEventIsGone(
	t *testing.T,
) {
	s := store.New()
	ctx := context.Background()
	enqueueGhost := func(
		tx models.Tx,
	) error {
		return tx.EnqueueDelivery(
			ctx,
			"ghost",
			1,
		)
	}
	s.Atomically(
		ctx,
		enqueueGhost,
	)

	var calls atomic.Int32
	build(
		t,
		eight(s).WithSubscriber(
			"OrderUpdated",
			countingSubscriber(&calls),
		),
	)

	if calls.Load() != 0 {
		t.Fatalf(
			"handler called %d times for a vanished event",
			calls.Load(),
		)
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 0 {
		t.Fatalf(
			"pending = %v, want none",
			pending,
		)
	}
}

func TestGuarantee_AckFailuresAreReportedAndTheDeliveryStaysPending(
	t *testing.T,
) {
	s := store.New()
	ctx := context.Background()
	var reported atomic.Int32
	countReport := func(
		context.Context,
		models.Event[mocks.Order],
		error,
	) {
		reported.Add(1)
	}
	inst := build(
		t,
		eight(nil).
			WithEventStore(ackErrStore{s}).
			WithPublishErrorHandler(countReport).
			WithSubscriber(
				"OrderUpdated",
				noopSubscriber,
			),
	)

	if _, err := inst.SendWait(
		ctx,
		upd(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	waitPublish(
		t,
		inst,
	)

	if reported.Load() != 1 {
		t.Fatalf(
			"ack failures reported = %d, want 1",
			reported.Load(),
		)
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 1 {
		t.Fatalf(
			"pending = %v, want the unacked delivery to stay for the next start",
			pending,
		)
	}
}

func TestGuarantee_RecoveryReadFailureFailsBuild(
	t *testing.T,
) {
	_, err := eight(nil).
		WithEventStore(pendingErrStore{store.New()}).
		WithSubscriber(
			"OrderUpdated",
			noopSubscriber,
		).
		Build()
	if err == nil {
		t.Fatal("Build succeeded with an unreadable delivery log")
	}
}

func TestGuarantee_NoDurableSubscribersWritesNoDeliveries(
	t *testing.T,
) {
	s := store.New()
	inst := build(
		t,
		eight(s),
	)
	if _, err := inst.Send(
		context.Background(),
		upd(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	waitPublish(
		t,
		inst,
	)
	if pending, _ := s.PendingDeliveries(
		context.Background(),
		10,
	); len(pending) != 0 {
		t.Fatalf(
			"pending = %v, want none",
			pending,
		)
	}
}

func failingSyncProjection(
	context.Context,
	models.Tx,
	models.Event[mocks.Order],
) error {
	return errors.New("read model write failed")
}

func TestGuarantee_SyncProjectionFailureRollsEverythingBack(
	t *testing.T,
) {
	s := store.New()
	var calls atomic.Int32
	inst := build(
		t,
		eight(s).
			WithSyncProjection(
				"OrderUpdated",
				failingSyncProjection,
			).
			WithSubscriber(
				"OrderUpdated",
				countingSubscriber(&calls),
			),
	)

	_, err := inst.Send(
		context.Background(),
		idemCmd{UpdateOrderCmd: upd(
			"agg",
			1,
		), Cmd: "c1"},
	)
	if !errors.Is(
		err,
		models.ErrPipelineFailed,
	) ||
		!errors.Is(
			err,
			models.ErrSyncProjection,
		) {
		t.Fatalf(
			"err = %v, want ErrPipelineFailed + ErrSyncProjection",
			err,
		)
	}

	waitPublish(
		t,
		inst,
	)

	if ok, _ := inst.Exists(
		context.Background(),
		"agg",
	); ok {
		t.Error("event survived a failed sync projection")
	}

	if _, found, _ := s.GetReceipt(
		context.Background(),
		"c1",
	); found {
		t.Error("receipt survived a failed sync projection")
	}

	if pending, _ := s.PendingDeliveries(
		context.Background(),
		10,
	); len(pending) != 0 {
		t.Errorf(
			"pending = %v, want none",
			pending,
		)
	}

	if calls.Load() != 0 {
		t.Errorf(
			"subscriber called %d times",
			calls.Load(),
		)
	}
}

func TestGuarantee_IdempotentCommands(
	t *testing.T,
) {
	s := store.New()
	inst := build(
		t,
		eight(s),
	)
	ctx := context.Background()

	first, err := inst.Send(
		ctx,
		idemCmd{UpdateOrderCmd: upd(
			"agg",
			7,
		), Cmd: "c1"},
	)
	if err != nil {
		t.Fatal(err)
	}

	retry, err := inst.Send(
		ctx,
		idemCmd{UpdateOrderCmd: upd(
			"agg",
			7,
		), Cmd: "c1"},
	)
	if err != nil ||
		retry.ID != first.ID ||
		retry.Version != 1 {
		t.Fatalf(
			"retry = %+v, %v; want the original event",
			retry,
			err,
		)
	}

	if _, err := inst.Send(
		ctx,
		idemCreate{CreateOrderCmd: mocks.CreateOrderCmd{ID: "bad", Total: -1}, Cmd: "c2"},
	); !errors.Is(
		err,
		models.ErrValidation,
	) {
		t.Fatalf(
			"err = %v, want ErrValidation",
			err,
		)
	}

	if _, err := inst.Send(
		ctx,
		idemCreate{CreateOrderCmd: mocks.CreateOrderCmd{ID: "bad", Total: 5}, Cmd: "c2"},
	); !errors.Is(
		err,
		models.ErrValidation,
	) {
		t.Fatalf(
			"retry err = %v, want the rejection to stick",
			err,
		)
	}

	other, err := inst.Send(
		ctx,
		idemCmd{UpdateOrderCmd: upd(
			"other",
			9,
		), Cmd: "c1"},
	)
	if err != nil ||
		other.ID != first.ID {
		t.Fatalf(
			"same id on another aggregate = %+v, %v; want the original event",
			other,
			err,
		)
	}

	if ok, _ := inst.Exists(
		ctx,
		"other",
	); ok {
		t.Error("second aggregate was written")
	}
}

func TestGuarantee_SnapshotFailureIsReportedNotFatal(
	t *testing.T,
) {
	ss := store.NewSnapshots()
	var reported atomic.Int32
	countReport := func(
		context.Context,
		models.Event[mocks.Order],
		error,
	) {
		reported.Add(1)
	}
	inst := build(
		t,
		asynx.New[mocks.Order]().
			WithEventStore(store.New()).
			WithSnapshotStore(ss).
			WithSnapshotErrorHandler(countReport),
	)

	ss.SetError(
		"agg",
		errors.New("snapshot disk full"),
	)
	evt, err := inst.Send(
		context.Background(),
		mocks.UpdateOrderCmd{ID: "agg", NewState: mocks.Order{ID: "agg"}, Snapshot: true},
	)
	if err != nil ||
		evt.Version != 1 {
		t.Fatalf(
			"Send = %+v, %v; want success",
			evt,
			err,
		)
	}

	if reported.Load() != 1 {
		t.Fatalf(
			"reported = %d, want 1",
			reported.Load(),
		)
	}
}

func TestGuarantee_AutoSnapshotFailureOnReadIsReportedNotFatal(
	t *testing.T,
) {
	events := store.New()
	ss := store.NewSnapshots()
	old := build(
		t,
		asynx.New[mocks.Order]().
			WithEventStore(events).
			WithSnapshotStore(ss),
	)
	_, err := old.Send(
		context.Background(),
		mocks.UpdateOrderCmd{ID: "agg", NewState: mocks.Order{ID: "agg", Status: "Old"}},
	)
	if err != nil {
		t.Fatalf(
			"Send: %v",
			err,
		)
	}

	var reported atomic.Int32
	countReport := func(
		context.Context,
		models.Event[mocks.Order],
		error,
	) {
		reported.Add(1)
	}
	identity := func(
		_ context.Context,
		_ string,
		patches []byte,
	) ([]byte, error) {
		return patches,
			nil
	}
	upgraded := build(
		t,
		asynx.New[mocks.Order]().
			WithEventStore(events).
			WithSnapshotStore(ss).
			WithSchemaVersion(2).
			WithUpcaster(
				1,
				identity,
			).
			WithSnapshotErrorHandler(countReport),
	)

	ss.SetError(
		"agg",
		errors.New("snapshot disk full"),
	)
	got, err := upgraded.Get(
		context.Background(),
		"agg",
	)
	if err != nil ||
		got.Status != "Old" {
		t.Fatalf(
			"Get = %+v, %v; want Old, nil",
			got,
			err,
		)
	}

	if reported.Load() != 1 {
		t.Fatalf(
			"reported = %d, want 1",
			reported.Load(),
		)
	}
}

func forgetOrSend(
	t *testing.T,
	inst asynx.Asynx[mocks.Order],
	wg *sync.WaitGroup,
	i int,
) {
	defer wg.Done()

	ctx := context.Background()
	if i != 15 {
		inst.Send(
			ctx,
			upd(
				"agg",
				float64(i),
			),
		)
		return
	}

	err := inst.Forget(
		ctx,
		"agg",
	)
	if err != nil {
		t.Errorf(
			"Forget: %v",
			err,
		)
	}
}

func requireReadableIfPresent(
	t *testing.T,
	inst asynx.Asynx[mocks.Order],
) {
	t.Helper()

	ctx := context.Background()
	ok, _ := inst.Exists(
		ctx,
		"agg",
	)
	if !ok {
		return
	}

	_, err := inst.Get(
		ctx,
		"agg",
	)
	if err != nil {
		t.Fatalf(
			"surviving aggregate is unreadable: %v",
			err,
		)
	}
}

func TestGuarantee_ForgetFencedAgainstCommands(
	t *testing.T,
) {
	s := store.New()
	inst := build(
		t,
		eight(s).WithSubscriber(
			"OrderUpdated",
			noopSubscriber,
		),
	)
	ctx := context.Background()
	if _, err := inst.Send(
		ctx,
		upd(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := range 30 {
		wg.Add(1)
		go forgetOrSend(
			t,
			inst,
			&wg,
			i,
		)
	}

	wg.Wait()
	waitPublish(
		t,
		inst,
	)

	requireReadableIfPresent(
		t,
		inst,
	)
}

func TestGuarantee_EventsNoDurableSubscriberWantsGetNoDeliveryEntry(
	t *testing.T,
) {
	s := store.New()
	release := make(chan struct{})
	inst := build(
		t,
		eight(s).WithSubscriber(
			"OrderUpdated",
			blockUntil(release),
		),
	)
	t.Cleanup(func() { close(release) })
	ctx := context.Background()

	if _, err := inst.Send(
		ctx,
		mocks.CreateOrderCmd{ID: "a", Total: 1},
	); err != nil {
		t.Fatal(err)
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 0 {
		t.Fatalf(
			"OrderCreated got a delivery entry: %v",
			pending,
		)
	}
}

func requireFailedBuild(
	t *testing.T,
) {
	t.Helper()

	_, err := eight(nil).
		WithEventStore(pendingErrStore{store.New()}).
		WithSubscriber(
			"OrderUpdated",
			noopSubscriber,
		).
		Build()
	if err == nil {
		t.Fatal("Build should fail")
	}
}

func TestGuarantee_FailedBuildShutsTheProcessorDown(
	t *testing.T,
) {
	before := runtime.NumGoroutine()
	for range 5 {
		requireFailedBuild(t)
	}

	settled := func() bool {
		return runtime.NumGoroutine() <= before+5
	}
	eventually(
		t,
		"goroutines to settle after failed builds",
		settled,
	)
}

type retryLog struct {
	versionLog
	failures atomic.Int32
}

func (l *retryLog) handleFlaky(
	ctx context.Context,
	e models.Event[mocks.Order],
) error {
	if e.Version == 1 &&
		l.failures.Add(1) <= 2 {
		return errors.New("read model unavailable")
	}

	return l.handle(
		ctx,
		e,
	)
}

func TestGuarantee_DurableSubscriberFailureRetriesAndKeepsOrder(
	t *testing.T,
) {
	s := store.New()
	ctx := context.Background()

	log := &retryLog{}
	var reported atomic.Int32
	countDelivery := func(
		context.Context,
		models.Event[mocks.Order],
		error,
		int,
	) {
		reported.Add(1)
	}
	inst := build(
		t,
		eight(s).
			WithDeliveryRetry(
				time.Millisecond,
				5*time.Millisecond,
			).
			WithDeliveryErrorHandler(countDelivery).
			WithSubscriber(
				"OrderUpdated",
				log.handleFlaky,
			),
	)

	for i := range 3 {
		sendUpdate(
			t,
			inst,
			float64(i+1),
		)
	}

	drained := func() bool {
		pending, _ := s.PendingDeliveries(
			ctx,
			10,
		)
		return len(pending) == 0
	}
	eventually(
		t,
		"deliveries to complete",
		drained,
	)

	seen := log.snapshot()
	if !slices.Equal(
		seen,
		[]int64{1, 2, 3},
	) {
		t.Fatalf(
			"delivered versions = %v, want [1 2 3] (v2 must wait for v1)",
			seen,
		)
	}

	if reported.Load() != 2 {
		t.Fatalf(
			"failures reported = %d, want 2",
			reported.Load(),
		)
	}
}

func failingCounter(
	attempts *atomic.Int32,
	msg string,
) func(context.Context, models.Event[mocks.Order]) error {
	return func(
		context.Context,
		models.Event[mocks.Order],
	) error {
		attempts.Add(1)
		return errors.New(msg)
	}
}

func sendWaitInto(
	inst asynx.Asynx[mocks.Order],
	done chan<- error,
) {
	_, err := inst.SendWait(
		context.Background(),
		upd(
			"agg",
			1,
		),
	)
	done <- err
}

func requireNoError(
	t *testing.T,
	what string,
	err error,
) {
	t.Helper()

	if err != nil {
		t.Fatalf(
			"%s: %v",
			what,
			err,
		)
	}
}

func TestGuarantee_SendWaitReturnsAfterTheFirstAttemptEvenIfTheHandlerFails(
	t *testing.T,
) {
	var attempts atomic.Int32
	inst := build(
		t,
		eight(store.New()).
			WithDeliveryRetry(
				time.Hour,
				time.Hour,
			).
			WithSubscriber(
				"OrderUpdated",
				failingCounter(
					&attempts,
					"down",
				),
			),
	)

	done := make(
		chan error,
		1,
	)
	go sendWaitInto(
		inst,
		done,
	)
	select {
	case err := <-done:
		requireNoError(
			t,
			"SendWait",
			err,
		)
	case <-time.After(2 * time.Second):
		t.Fatal("SendWait blocked on a failing durable handler's retries")
	}

	if attempts.Load() != 1 {
		t.Fatalf(
			"attempts = %d, want 1",
			attempts.Load(),
		)
	}
}

func buildAndSignal(
	t *testing.T,
	b *asynx.Builder[mocks.Order],
	built chan<- struct{},
) {
	build(
		t,
		b,
	)
	close(built)
}

func TestGuarantee_BuildDoesNotHangOnAPoisonDelivery(
	t *testing.T,
) {
	s := store.New()
	ctx := context.Background()
	dies := make(chan struct{})
	first := build(
		t,
		eight(s).WithSubscriber(
			"OrderUpdated",
			blockUntil(dies),
		),
	)
	t.Cleanup(func() { close(dies) })
	if _, err := first.Send(
		ctx,
		upd(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	var attempts atomic.Int32
	built := make(chan struct{})
	go buildAndSignal(
		t,
		eight(s).
			WithDeliveryRetry(
				time.Hour,
				time.Hour,
			).
			WithSubscriber(
				"OrderUpdated",
				failingCounter(
					&attempts,
					"poison",
				),
			),
		built,
	)
	select {
	case <-built:
	case <-time.After(3 * time.Second):
		t.Fatal("Build hung on a poison delivery")
	}

	eventually(
		t,
		"the recovered delivery to be attempted",
		func() bool { return attempts.Load() >= 1 },
	)
	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 1 {
		t.Fatalf(
			"pending = %v, want the poison delivery to stay pending",
			pending,
		)
	}
}

type panicOnce struct {
	attempts atomic.Int32
}

func (p *panicOnce) handle(
	context.Context,
	models.Event[mocks.Order],
) error {
	if p.attempts.Add(1) == 1 {
		panic("boom")
	}

	return nil
}

func TestGuarantee_DurablePanicIsAFailureNotADelivery(
	t *testing.T,
) {
	flaky := &panicOnce{}
	var panics atomic.Int32
	countPanic := func(
		context.Context,
		models.Event[mocks.Order],
		any,
	) {
		panics.Add(1)
	}
	s := store.New()
	inst := build(
		t,
		eight(s).
			WithDeliveryRetry(
				time.Millisecond,
				2*time.Millisecond,
			).
			WithPanicHandler(countPanic).
			WithSubscriber(
				"OrderUpdated",
				flaky.handle,
				models.WithHandlerTimeout[mocks.Order](time.Second),
			),
	)

	if _, err := inst.Send(
		context.Background(),
		upd(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	completed := func() bool {
		p, _ := s.PendingDeliveries(
			context.Background(),
			1,
		)
		return flaky.attempts.Load() >= 2 && len(p) == 0
	}
	eventually(
		t,
		"the delivery to complete after the panic",
		completed,
	)
	if panics.Load() != 1 {
		t.Fatalf(
			"panic handler calls = %d, want 1",
			panics.Load(),
		)
	}
}

func floodHot(
	ctx context.Context,
	inst asynx.Asynx[mocks.Order],
	flooding *sync.WaitGroup,
	i int,
) {
	flooding.Done()
	inst.Send(
		ctx,
		upd(
			"hot",
			float64(i+3),
		),
	)
}

func probeCold(
	t *testing.T,
	inst asynx.Asynx[mocks.Order],
	i int,
) {
	t.Helper()

	cold, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	_, err := inst.SendWait(
		cold,
		mocks.CreateOrderCmd{ID: fmt.Sprintf(
			"cold-%d",
			i,
		), Total: 1},
	)
	if err != nil {
		t.Fatalf(
			"an unrelated aggregate on the same shard, probe %d: %v",
			i,
			err,
		)
	}
}

func TestShardingOpts_ABusyAggregateDoesNotStallItsShard(
	t *testing.T,
) {
	ctx := context.Background()
	inst := build(
		t,
		asynx.New[mocks.Order]().WithEventStore(store.New()).WithSnapshotStore(store.NewSnapshots()).
			WithShardingOpts(asynx.ShardingOpts{Shards: 1, WorkersPerShard: 2, QueueDepth: 1000}),
	)
	if _, err := inst.SendWait(
		ctx,
		mocks.CreateOrderCmd{ID: "hot", Total: 1},
	); err != nil {
		t.Fatal(err)
	}

	gate := make(chan struct{})
	entered := make(
		chan struct{},
		1,
	)
	defer close(gate)
	go inst.Send(
		ctx,
		gatedUpdate{UpdateOrderCmd: upd(
			"hot",
			2,
		), entered: entered, gate: gate},
	)
	within(
		t,
		entered,
		"the hot command to start",
	)
	var flooding sync.WaitGroup
	for i := range 20 {
		flooding.Add(1)
		go floodHot(
			ctx,
			inst,
			&flooding,
			i,
		)
	}

	flooding.Wait()

	for i := range 50 {
		probeCold(
			t,
			inst,
			i,
		)
	}
}

func TestShardingOpts_QueueDepthBoundsTheBacklogOfOneAggregate(
	t *testing.T,
) {
	ctx := context.Background()
	inst := build(
		t,
		asynx.New[mocks.Order]().WithEventStore(store.New()).WithSnapshotStore(store.NewSnapshots()).
			WithShardingOpts(asynx.ShardingOpts{Shards: 1, WorkersPerShard: 2}),
	)
	if _, err := inst.SendWait(
		ctx,
		mocks.CreateOrderCmd{ID: "hot", Total: 1},
	); err != nil {
		t.Fatal(err)
	}

	gate := make(chan struct{})
	entered := make(
		chan struct{},
		1,
	)
	defer close(gate)
	go inst.Send(
		ctx,
		gatedUpdate{UpdateOrderCmd: upd(
			"hot",
			2,
		), entered: entered, gate: gate},
	)
	within(
		t,
		entered,
		"the hot command to start",
	)

	refused := make(
		chan struct{},
		32,
	)
	for range 20 {
		go sendHot(
			ctx,
			inst,
			refused,
		)
	}

	for range 18 {
		within(
			t,
			refused,
			"a hot Send to be refused with ErrQueueFull",
		)
	}

	if _, err := inst.Send(
		ctx,
		mocks.CreateOrderCmd{ID: "cold", Total: 1},
	); err != nil {
		t.Fatalf(
			"an unrelated aggregate got %v while another was flooded",
			err,
		)
	}
}

func TestBuild_UsesTheSuppliedDispatcher(
	t *testing.T,
) {
	ctx := context.Background()
	s := store.New()
	fake := mockbuses.NewDispatcher[mocks.Order]()
	inst := build(
		t,
		eight(s).WithDispatcher(fake.Factory()).WithSubscriber(
			"OrderCreated",
			noopDurable,
		),
	)

	if _, err := inst.Send(
		ctx,
		mocks.CreateOrderCmd{ID: "a", Total: 1},
	); err != nil {
		t.Fatal(err)
	}

	requests := fake.Requests()
	if len(requests) != 1 {
		t.Fatalf(
			"requests = %d, want 1",
			len(requests),
		)
	}

	request := requests[0]
	wrongRequest := request.Kind != models.DispatchEvent || request.Event.AggregateID != "a" || request.Event.Version != 1
	if wrongRequest {
		t.Errorf(
			"request = %+v, want the DispatchEvent of a v1",
			request,
		)
	}

	if request.Ack == nil {
		t.Fatal("a durable event was enqueued without an Ack")
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 1 {
		t.Fatalf(
			"pending before the Ack = %v, want the entry",
			pending,
		)
	}

	if err := request.Ack(ctx); err != nil {
		t.Fatal(err)
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 0 {
		t.Errorf(
			"pending after the Ack = %v, want none",
			pending,
		)
	}
}

func TestBuild_RecoverEnqueuesRedeliveries(
	t *testing.T,
) {
	s := store.New()
	stuck := make(chan struct{})
	first := build(
		t,
		eight(s).WithSubscriber(
			"OrderUpdated",
			blockUntil(stuck),
		),
	)
	t.Cleanup(func() { close(stuck) })
	sendAll(
		t,
		first,
		mocks.CreateOrderCmd{ID: "a", Total: 1},
		upd(
			"a",
			2,
		),
		upd(
			"a",
			3,
		),
	)

	fake := mockbuses.NewDispatcher[mocks.Order]()
	build(
		t,
		eight(s).WithDispatcher(fake.Factory()).WithSubscriber(
			"OrderUpdated",
			noopDurable,
		),
	)

	requests := fake.Requests()
	if len(requests) != 2 {
		t.Fatalf(
			"requests = %d, want the 2 pending deliveries",
			len(requests),
		)
	}

	for i, request := range requests {
		assertRedelivery(
			t,
			i,
			request,
		)
	}
}

func TestForget_SupersedesThenEnqueuesATombstone(
	t *testing.T,
) {
	ctx := context.Background()
	fake := mockbuses.NewDispatcher[mocks.Order]()
	inst := build(
		t,
		eight(store.New()).WithDispatcher(fake.Factory()),
	)
	if _, err := inst.Send(
		ctx,
		mocks.CreateOrderCmd{ID: "a", Total: 1},
	); err != nil {
		t.Fatal(err)
	}

	if err := inst.Forget(
		ctx,
		"a",
	); err != nil {
		t.Fatal(err)
	}

	calls := fake.Calls()
	supersede := slices.Index(
		calls,
		"Supersede",
	)
	if supersede < 0 ||
		calls[supersede+1] != "Enqueue" {
		t.Fatalf(
			"calls = %v, want Supersede immediately followed by Enqueue",
			calls,
		)
	}

	requests := fake.Requests()
	tombstone := requests[len(requests)-1]
	wrongTombstone := tombstone.Kind != models.DispatchTombstone || tombstone.Event.AggregateID != "a" || tombstone.Ack == nil
	if wrongTombstone {
		t.Errorf(
			"last request = %+v, want a tombstone of a with an Ack",
			tombstone,
		)
	}
}

func TestBuild_ADispatcherFactoryErrorFailsBuild(
	t *testing.T,
) {
	sentinel := errors.New("no dispatcher today")
	factory := func(
		models.DispatcherConfig[mocks.Order],
	) (models.Dispatcher[mocks.Order], error) {
		return nil,
			sentinel
	}
	bus := &mockbuses.ClosingBus[mocks.Order]{}

	inst, err := eight(store.New()).WithBus(bus).WithDispatcher(factory).Build()
	if !errors.Is(
		err,
		sentinel,
	) ||
		inst != nil {
		t.Fatalf(
			"Build = (%v, %v), want the factory error and no instance",
			inst,
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

	if got := bus.Closes.Load(); got != 1 {
		t.Errorf(
			"bus closed %d times, want 1",
			got,
		)
	}
}

func isNotShutdownCall(
	call string,
) bool {
	return call != "Interrupt" && call != "Close"
}

func TestShutdown_InterruptsThenClosesTheDispatcher(
	t *testing.T,
) {
	fake := mockbuses.NewDispatcher[mocks.Order]()
	inst, err := eight(store.New()).WithDispatcher(fake.Factory()).Build()
	if err != nil {
		t.Fatal(err)
	}

	if err := inst.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	got := slices.DeleteFunc(
		fake.Calls(),
		isNotShutdownCall,
	)
	if !slices.Equal(
		got,
		[]string{"Interrupt", "Close"},
	) {
		t.Errorf(
			"shutdown calls = %v, want Interrupt then Close",
			got,
		)
	}
}

func TestSend_AnEnqueueFailureAfterCommitParksThroughTheSuppliedDispatcher(
	t *testing.T,
) {
	ctx := context.Background()
	sentinel := errors.New("enqueue refused")
	fake := mockbuses.NewDispatcher[mocks.Order]()
	fake.FailEnqueue(sentinel)
	inst := build(
		t,
		eight(store.New()).WithDispatcher(fake.Factory()).WithSubscriber(
			"OrderCreated",
			noopDurable,
		),
	)

	_, err := inst.Send(
		ctx,
		mocks.CreateOrderCmd{ID: "a", Total: 1},
	)
	if !errors.Is(
		err,
		models.ErrPipelineFailed,
	) ||
		!errors.Is(
			err,
			sentinel,
		) {
		t.Fatalf(
			"Send = %v, want ErrPipelineFailed wrapping the enqueue failure",
			err,
		)
	}

	if got := fake.ParkCalls(); len(got) != 1 ||
		got[0].AggregateID != "a" {
		t.Errorf(
			"Park calls = %v, want the one park of a",
			got,
		)
	}

	if got := inst.PausedAggregates(); !slices.Equal(
		got,
		[]string{"a"},
	) {
		t.Errorf(
			"PausedAggregates = %v, want [a]",
			got,
		)
	}
}

type defaultsCase struct {
	name     string
	retry    [2]time.Duration
	attempts int
	want     models.DispatcherConfig[mocks.Order]
}

func (c defaultsCase) run(
	t *testing.T,
) {
	assertFactoryConfig(
		t,
		c.retry,
		c.attempts,
		c.want,
	)
}

func TestBuild_DefaultsAreAppliedBeforeTheFactoryRuns(
	t *testing.T,
) {
	defaults := models.DispatcherConfig[mocks.Order]{MaxAttempts: 20, RetryInitial: 100 * time.Millisecond, RetryMax: 30 * time.Second}
	tests := []defaultsCase{
		{
			name:     "non-positive builder values are ignored",
			retry:    [2]time.Duration{-1, 0},
			attempts: -3,
			want:     defaults,
		},
		{
			name: "unset builder values keep the defaults",
			want: defaults,
		},
		{
			name:     "positive builder values are carried",
			retry:    [2]time.Duration{5 * time.Millisecond, 50 * time.Millisecond},
			attempts: 3,
			want:     models.DispatcherConfig[mocks.Order]{MaxAttempts: 3, RetryInitial: 5 * time.Millisecond, RetryMax: 50 * time.Millisecond},
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name,
			tt.run,
		)
	}
}

func TestBuild_ATypedNilDispatcherFailsBuild(
	t *testing.T,
) {
	factory := func(
		models.DispatcherConfig[mocks.Order],
	) (d models.Dispatcher[mocks.Order], err error) {
		d = (*mockbuses.Dispatcher[mocks.Order])(nil)
		return d,
			nil
	}

	inst, err := eight(store.New()).WithBus(&mockbuses.ClosingBus[mocks.Order]{}).WithDispatcher(factory).Build()

	notRejected := inst != nil || err == nil || !strings.Contains(
		err.Error(),
		"returned no dispatcher",
	)
	if notRejected {
		t.Fatalf(
			"Build = (%v, %v), want the no-dispatcher error and no instance",
			inst,
			err,
		)
	}
}
