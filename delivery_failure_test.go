package asynx_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/char2cs/asynx"
	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

func within[V any](t *testing.T, ch <-chan V, what string) V {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

type versions struct {
	mu  sync.Mutex
	got []int64
}

func (v *versions) add(n int64) {
	v.mu.Lock()
	v.got = append(v.got, n)
	v.mu.Unlock()
}

func (v *versions) list() []int64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]int64(nil), v.got...)
}

func equalVersions(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// A restart after a shutdown that interrupted a retry must still deliver in
// version order: v2 may not be delivered and acknowledged past a pending v1.
func TestFailure_ShutdownDuringRetryKeepsVersionOrderAcrossRestart(t *testing.T) {
	s := store.New()
	ctx := context.Background()

	var seen versions
	attempted := make(chan struct{}, 64)
	first, err := eight(s).
		WithDeliveryRetry(time.Hour, time.Hour).
		WithSubscriber("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) error {
			seen.add(e.Version)
			if e.Version == 1 {
				attempted <- struct{}{}
				return errors.New("poison")
			}
			return nil
		}).Build()
	if err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if _, err := first.Send(ctx, upd("agg", float64(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	within(t, attempted, "first attempt")

	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := first.Shutdown(sctx); err != nil {
		t.Fatalf("Shutdown with a retrying delivery: %v", err)
	}

	if got := seen.list(); !equalVersions(got, []int64{1}) {
		t.Fatalf("handler saw versions %v before restart, want only the failing v1", got)
	}
	pending, _ := s.PendingDeliveries(ctx, 10)
	if len(pending) != 2 || pending[0].Version != 1 || pending[1].Version != 2 {
		t.Fatalf("pending after shutdown = %v, want v1 and v2 both still pending", pending)
	}

	var after versions
	second := build(t, eight(s).WithSubscriber("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) error {
		after.add(e.Version)
		return nil
	}))
	waitPublish(t, second)
	if got := after.list(); !equalVersions(got, []int64{1, 2}) {
		t.Fatalf("after restart delivered %v, want [1 2]", got)
	}
}

// One event whose handler always fails must not hold a shard worker, outlast
// its caller's context, or keep the process from shutting down.
func TestFailure_PoisonEventDoesNotBlockTheShardOrShutdown(t *testing.T) {
	s := store.New()
	ctx := context.Background()

	attempted := make(chan struct{}, 64)
	inst, err := asynx.New[mocks.Order]().
		WithEventStore(s).
		WithSnapshotStore(store.NewSnapshots()).
		WithShardingOpts(asynx.ShardingOpts{Shards: 1, WorkersPerShard: 1, QueueDepth: 100}).
		WithDeliveryRetry(time.Hour, time.Hour).
		WithSubscriber("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) error {
			if strings.HasPrefix(e.AggregateID, "bad") && e.EventName != models.ForgetEventName {
				attempted <- struct{}{}
				return errors.New("poison")
			}
			return nil
		}).
		Build()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := inst.Send(ctx, upd("bad", 1)); err != nil {
		t.Fatal(err)
	}
	within(t, attempted, "poison attempt")

	// SendWait for the poisoned aggregate waits behind the failing delivery but
	// obeys its context.
	wctx, cancelWait := context.WithCancel(ctx)
	waited := make(chan error, 1)
	go func() {
		_, err := inst.SendWait(wctx, upd("bad", 2))
		waited <- err
	}()
	// The command commits (the worker is free), then waits for its handlers.
	eventually(t, "the second command to commit", func() bool {
		select {
		case err := <-waited:
			t.Fatalf("SendWait returned %v before committing", err)
		default:
		}
		blobs, _ := s.ReadFrom(ctx, "events:bad", 1)
		return len(blobs) == 2
	})
	cancelWait()
	if err := within(t, waited, "SendWait to give up"); !errors.Is(err, models.ErrContextCancelled) {
		t.Fatalf("SendWait err = %v, want ErrContextCancelled", err)
	}

	// The single worker of the single shard is free for other aggregates.
	if _, err := inst.SendWait(ctx, upd("good", 1)); err != nil {
		t.Fatalf("unrelated aggregate on the same shard: %v", err)
	}

	// Forget of the poisoned aggregate goes through: its failing deliveries are
	// abandoned, then the tombstone is delivered.
	fctx, cancelForget := context.WithTimeout(ctx, 5*time.Second)
	defer cancelForget()
	if err := inst.Forget(fctx, "bad"); err != nil {
		t.Fatalf("Forget of a poisoned aggregate: %v", err)
	}
	if ok, _ := inst.Exists(ctx, "bad"); ok {
		t.Fatal("poisoned aggregate survived Forget")
	}

	// Shutdown with another poison delivery in flight finishes.
	if _, err := inst.Send(ctx, upd("bad2", 1)); err != nil {
		t.Fatal(err)
	}
	within(t, attempted, "second poison attempt")
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := inst.Shutdown(sctx); err != nil {
		t.Fatalf("Shutdown with a poison delivery: %v", err)
	}
}

func TestFailure_RetriesStopAndTheAggregateIsParkedUntilResumed(t *testing.T) {
	s := store.New()
	ctx := context.Background()

	var healthy bool
	var mu sync.Mutex
	var seen versions
	parked := make(chan error, 8)
	inst := build(t, eight(s).
		WithDeliveryRetry(time.Millisecond, 2*time.Millisecond).
		WithDeliveryMaxAttempts(3).
		WithDeliveryErrorHandler(func(_ context.Context, _ models.Event[mocks.Order], err error, _ int) {
			if errors.Is(err, models.ErrDeliveryParked) {
				parked <- err
			}
		}).
		WithSubscriber("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) error {
			mu.Lock()
			ok := healthy || e.AggregateID != "bad"
			mu.Unlock()
			if !ok {
				return errors.New("read model down")
			}
			if e.AggregateID == "bad" {
				seen.add(e.Version)
			}
			return nil
		}))

	for i := range 2 {
		if _, err := inst.Send(ctx, upd("bad", float64(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	within(t, parked, "park report")

	if _, err := inst.SendWait(ctx, upd("other", 1)); err != nil {
		t.Fatalf("another aggregate: %v", err)
	}
	// A new event of the parked aggregate commits but is not delivered.
	if _, err := inst.SendWait(ctx, upd("bad", 3)); err != nil {
		t.Fatalf("Send to a parked aggregate: %v", err)
	}
	if got := seen.list(); len(got) != 0 {
		t.Fatalf("parked aggregate delivered %v", got)
	}
	pending, _ := s.PendingDeliveries(ctx, 10)
	nBad := 0
	for _, p := range pending {
		if p.AggregateID == "bad" {
			nBad++
		}
	}
	if nBad != 3 {
		t.Fatalf("pending deliveries of the parked aggregate = %d, want 3 (%v)", nBad, pending)
	}

	mu.Lock()
	healthy = true
	mu.Unlock()
	if err := inst.ResumeDeliveries(ctx); err != nil {
		t.Fatalf("ResumeDeliveries: %v", err)
	}
	waitPublish(t, inst)
	if got := seen.list(); !equalVersions(got, []int64{1, 2, 3}) {
		t.Fatalf("after resume delivered %v, want [1 2 3] in order", got)
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 0 {
		t.Fatalf("pending after resume = %v", pending)
	}
}

func TestFailure_DeadLetterSkipsThePoisonEvent(t *testing.T) {
	ctx := context.Background()
	var seen versions
	dead := make(chan int64, 4)
	inst := build(t, eight(store.New()).
		WithDeliveryRetry(time.Millisecond, 2*time.Millisecond).
		WithDeliveryMaxAttempts(2).
		WithDeadLetterHandler(func(_ context.Context, e models.Event[mocks.Order], _ error) error {
			dead <- e.Version
			return nil
		}).
		WithSubscriber("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) error {
			if e.Version == 1 {
				return errors.New("poison")
			}
			seen.add(e.Version)
			return nil
		}))
	for i := range 3 {
		if _, err := inst.Send(ctx, upd("agg", float64(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	if v := within(t, dead, "dead letter"); v != 1 {
		t.Fatalf("dead-lettered v%d, want v1", v)
	}
	waitPublish(t, inst)
	if got := seen.list(); !equalVersions(got, []int64{2, 3}) {
		t.Fatalf("delivered %v after skipping v1, want [2 3]", got)
	}
}

// Between the tombstone and the erase, no command may commit.
func TestFailure_CommandsAreRejectedWhileForgetRuns(t *testing.T) {
	s := store.New()
	ctx := context.Background()
	inst := build(t, eight(s))

	entered := make(chan struct{})
	release := make(chan struct{})
	var reaction error
	if _, err := inst.OnForget(func(c context.Context, e models.Event[mocks.Order]) {
		close(entered)
		// A reaction sending a command to the aggregate it is told was forgotten
		// is refused, not deadlocked.
		_, reaction = inst.Send(c, upd("agg", 99))
		<-release
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := inst.Send(ctx, upd("agg", 1)); err != nil {
		t.Fatal(err)
	}

	forgot := make(chan error, 1)
	go func() { forgot <- inst.Forget(ctx, "agg") }()
	within(t, entered, "forget handler")

	if _, err := inst.Send(ctx, upd("agg", 2)); !errors.Is(err, models.ErrForgetInProgress) {
		t.Fatalf("Send during Forget err = %v, want ErrForgetInProgress", err)
	}
	// Other aggregates are unaffected.
	if _, err := inst.Send(ctx, upd("other", 1)); err != nil {
		t.Fatalf("unrelated aggregate during Forget: %v", err)
	}

	close(release)
	if err := within(t, forgot, "Forget"); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if !errors.Is(reaction, models.ErrForgetInProgress) {
		t.Fatalf("reaction err = %v, want ErrForgetInProgress", reaction)
	}
	if blobs, _ := s.ReadFrom(ctx, "events:agg", 1); len(blobs) != 0 {
		t.Fatalf("stream has %d events after Forget: a command slipped in", len(blobs))
	}

	// After the erase the aggregate can be created again, from version 1.
	evt, err := inst.Send(ctx, upd("agg", 3))
	if err != nil {
		t.Fatalf("Send after Forget: %v", err)
	}
	if evt.Version != 1 {
		t.Fatalf("re-created aggregate version = %d, want 1", evt.Version)
	}
}

// A subscriber that dedupes on (AggregateID, Version) needs the deletion marker
// to know a re-created aggregate starts over.
func TestFailure_DurableSubscribersReceiveTheDeletionMarker(t *testing.T) {
	ctx := context.Background()
	type key struct {
		id string
		v  int64
	}
	var mu sync.Mutex
	seen := map[key]bool{}
	var applied []int64
	var markers int

	inst := build(t, eight(store.New()).
		WithSubscriber("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) error {
			mu.Lock()
			defer mu.Unlock()
			if e.EventName == models.ForgetEventName {
				markers++
				for k := range seen {
					if k.id == e.AggregateID {
						delete(seen, k)
					}
				}
				return nil
			}
			k := key{e.AggregateID, e.Version}
			if seen[k] {
				return nil // duplicate delivery
			}
			seen[k] = true
			applied = append(applied, e.Version)
			return nil
		}))

	if _, err := inst.SendWait(ctx, upd("agg", 1)); err != nil {
		t.Fatal(err)
	}
	if err := inst.Forget(ctx, "agg"); err != nil {
		t.Fatal(err)
	}
	if _, err := inst.SendWait(ctx, upd("agg", 2)); err != nil {
		t.Fatal(err)
	}
	waitPublish(t, inst)

	mu.Lock()
	defer mu.Unlock()
	if markers != 1 {
		t.Fatalf("deletion markers seen = %d, want 1 (pattern OrderUpdated does not match it, it is delivered anyway)", markers)
	}
	if !equalVersions(applied, []int64{1, 1}) {
		t.Fatalf("applied versions = %v, want the re-created aggregate's v1 applied again", applied)
	}
}

// If a durable subscriber cannot process the deletion marker, the aggregate is
// not erased: Forget fails and can be retried.
func TestFailure_ForgetFailsWhenADurableSubscriberCannotTakeTheMarker(t *testing.T) {
	ctx := context.Background()
	var healthy bool
	var mu sync.Mutex
	inst := build(t, eight(store.New()).
		WithDeliveryRetry(time.Millisecond, 2*time.Millisecond).
		WithDeliveryMaxAttempts(2).
		WithSubscriber("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) error {
			mu.Lock()
			defer mu.Unlock()
			if e.EventName == models.ForgetEventName && !healthy {
				return errors.New("cannot drop state")
			}
			return nil
		}))
	if _, err := inst.SendWait(ctx, upd("agg", 1)); err != nil {
		t.Fatal(err)
	}

	if err := inst.Forget(ctx, "agg"); !errors.Is(err, models.ErrForgetFailed) {
		t.Fatalf("Forget err = %v, want ErrForgetFailed", err)
	}
	// Forget aborted before erasing; commands are accepted again.
	if _, err := inst.SendWait(ctx, upd("agg", 2)); err != nil {
		t.Fatalf("Send after a failed Forget: %v", err)
	}

	mu.Lock()
	healthy = true
	mu.Unlock()
	if err := inst.Forget(ctx, "agg"); err != nil {
		t.Fatalf("retried Forget: %v", err)
	}
	if ok, _ := inst.Exists(ctx, "agg"); ok {
		t.Fatal("aggregate survived the retried Forget")
	}
}

type failingLiveBus struct{ models.Bus[mocks.Order] }

func (failingLiveBus) PublishSync(context.Context, models.Event[mocks.Order]) error {
	return errors.New("live bus down")
}

// A failing live bus must not make durable delivery skip an event, or the
// aggregate's next event would be delivered past it.
func TestFailure_LiveBusErrorDoesNotSkipDurableDelivery(t *testing.T) {
	s := store.New()
	ctx := context.Background()
	var seen versions
	reported := make(chan error, 8)
	inst := build(t, eight(s).
		WithBus(failingLiveBus{bus.NewChannelBus[mocks.Order]()}).
		WithPublishErrorHandler(func(_ context.Context, _ models.Event[mocks.Order], err error) { reported <- err }).
		WithSubscriber("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) error {
			seen.add(e.Version)
			return nil
		}))
	for i := range 3 {
		if _, err := inst.SendWait(ctx, upd("agg", float64(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	waitPublish(t, inst)
	if got := seen.list(); !equalVersions(got, []int64{1, 2, 3}) {
		t.Fatalf("durable deliveries = %v, want [1 2 3]", got)
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 0 {
		t.Fatalf("pending = %v, want none", pending)
	}
	within(t, reported, "bus error report")
}

// Events piling up behind a stuck delivery are bounded in memory: past the
// bound the aggregate is parked, the events stay in the store's delivery log,
// and ResumeDeliveries brings them back in order.
func TestFailure_QueueBoundParksInsteadOfGrowing(t *testing.T) {
	s := store.New()
	ctx := context.Background()

	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	parked := make(chan error, 4)
	var seen versions
	inst := build(t, eight(s).
		WithDeliveryMaxQueued(2).
		WithDeliveryErrorHandler(func(_ context.Context, _ models.Event[mocks.Order], err error, _ int) {
			if errors.Is(err, models.ErrDeliveryParked) {
				parked <- err
			}
		}).
		WithSubscriber("OrderUpdated", func(_ context.Context, e models.Event[mocks.Order]) error {
			if e.Version == 1 {
				started <- struct{}{}
				<-gate
			}
			seen.add(e.Version)
			return nil
		}))

	if _, err := inst.Send(ctx, upd("agg", 1)); err != nil {
		t.Fatal(err)
	}
	within(t, started, "v1 running")
	for i := 2; i <= 5; i++ {
		if _, err := inst.Send(ctx, upd("agg", float64(i))); err != nil {
			t.Fatal(err)
		}
	}
	within(t, parked, "park report")

	close(gate)
	waitPublish(t, inst)
	if got := seen.list(); !equalVersions(got, []int64{1}) {
		t.Fatalf("delivered %v while parked, want only v1", got)
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 4 {
		t.Fatalf("pending = %v, want v2..v5 kept in the log", pending)
	}

	if err := inst.ResumeDeliveries(ctx); err != nil {
		t.Fatal(err)
	}
	waitPublish(t, inst)
	if got := seen.list(); !equalVersions(got, []int64{1, 2, 3, 4, 5}) {
		t.Fatalf("after resume delivered %v, want [1 2 3 4 5]", got)
	}
}
