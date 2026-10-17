package exec

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/bus/dispatcher"
	"github.com/char2cs/asynx/internal/eventstore"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

func TestWaitDone_ReturnsWhenDoneClosesOrContextEnds(t *testing.T) {
	done := make(chan struct{})
	close(done)
	if err := WaitDone(context.Background(), done); err != nil {
		t.Fatalf("closed done: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := WaitDone(ctx, make(chan struct{})); !errors.Is(err, asynxmd.ErrContextCancelled) {
		t.Fatalf("cancelled: err = %v, want ErrContextCancelled", err)
	}
}

func TestExecute_WaitingForHandlersGivesUpWithTheCommittedEvent(t *testing.T) {
	ex, _, _, _, b := newEnv(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	b.Subscribe("OrderUpdated", func(context.Context, asynxmd.Event[order]) {
		close(entered)
		<-release
	})

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		evt asynxmd.Event[order]
		err error
	}
	got := make(chan result, 1)
	go func() {
		evt, err := ex.Execute(ctx, update("agg", 1), true)
		got <- result{evt, err}
	}()
	<-entered
	cancel()
	r := <-got
	if !errors.Is(r.err, asynxmd.ErrContextCancelled) || r.evt.Version != 1 {
		t.Fatalf("got (%+v, %v), want the committed v1 with ErrContextCancelled", r.evt, r.err)
	}
}

func TestWithLock_HonoursContext(t *testing.T) {
	ex, _, _, _, _ := newEnv(t)
	ran := false
	if err := ex.WithLock(context.Background(), "agg", func() error { ran = true; return nil }); err != nil || !ran {
		t.Fatalf("WithLock = %v, ran = %v", err, ran)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ex.WithLock(ctx, "agg", func() error { t.Fatal("ran"); return nil }); !errors.Is(err, asynxmd.ErrContextCancelled) {
		t.Fatalf("err = %v, want ErrContextCancelled", err)
	}
}

func TestForget_SecondConcurrentForgetIsRefusedWhileOneRuns(t *testing.T) {
	ex, _, _, _, b := newEnv(t)
	ctx := context.Background()
	if _, err := ex.Execute(ctx, update("agg", 1), false); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	b.Subscribe("asynx\\.aggregate\\.forget", func(context.Context, asynxmd.Event[order]) {
		close(entered)
		<-release
	})

	first := make(chan error, 1)
	go func() { first <- ex.Forget(ctx, "agg") }()
	<-entered

	if err := ex.Forget(ctx, "agg"); !errors.Is(err, asynxmd.ErrForgetInProgress) {
		t.Fatalf("second Forget err = %v, want ErrForgetInProgress", err)
	}
	if _, err := ex.Execute(ctx, update("agg", 2), false); !errors.Is(err, asynxmd.ErrForgetInProgress) {
		t.Fatalf("Execute err = %v, want ErrForgetInProgress", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first Forget: %v", err)
	}
}

func TestForget_AbortsWhenTheCallerGivesUpWaitingForHandlers(t *testing.T) {
	ex, s, _, _, b := newEnv(t)
	if _, err := ex.Execute(context.Background(), update("agg", 1), false); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	b.Subscribe("asynx\\.aggregate\\.forget", func(context.Context, asynxmd.Event[order]) {
		close(entered)
		<-release
	})

	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan error, 1)
	go func() { got <- ex.Forget(ctx, "agg") }()
	<-entered
	cancel()
	if err := <-got; !errors.Is(err, asynxmd.ErrForgetFailed) {
		t.Fatalf("err = %v, want ErrForgetFailed", err)
	}
	close(release)

	// Nothing was erased, and the aggregate accepts commands again.
	if n := streamLen(t, s, "agg"); n == 0 {
		t.Fatal("stream erased although Forget aborted")
	}
	if _, err := ex.Execute(context.Background(), update("agg", 2), false); errors.Is(err, asynxmd.ErrForgetInProgress) {
		t.Fatal("aggregate still fenced after Forget aborted")
	}
}

func TestForget_FailsWhenTheDispatcherIsClosed(t *testing.T) {
	ex, s, _, d, _ := newEnv(t)
	if _, err := ex.Execute(context.Background(), update("agg", 1), false); err != nil {
		t.Fatal(err)
	}
	d.Close(context.Background())

	if err := ex.Forget(context.Background(), "agg"); !errors.Is(err, asynxmd.ErrPipelineFailed) {
		t.Fatalf("err = %v, want ErrPipelineFailed", err)
	}
	if _, err := ex.Execute(context.Background(), update("agg", 2), false); errors.Is(err, asynxmd.ErrForgetInProgress) {
		t.Fatal("aggregate left fenced by a Forget that failed to enqueue")
	}
	_ = s
}

func durableEnv(t *testing.T, handler asynxmd.DurableHandler[order], opts ...dispatcher.Opt[order]) *CommandExecutor[order] {
	t.Helper()
	es := eventstore.New[order](store.New(), store.NewSnapshots(), nil, 1, nil)
	d := dispatcher.New[order](bus.NewChannelBus[order](), append([]dispatcher.Opt[order]{
		dispatcher.WithDurableSubs([]dispatcher.DurableSub[order]{{Match: func(string) bool { return true }, Handler: handler}}),
		dispatcher.WithRetryBackoff[order](time.Millisecond, 2*time.Millisecond),
		dispatcher.WithMaxAttempts[order](2),
	}, opts...)...)
	t.Cleanup(func() { d.Close(context.Background()) })
	return New(es, d, WithDurableDelivery[order](func(string) bool { return true }))
}

func TestForget_DoesNotEraseWhenADurableSubscriberCannotTakeTheTombstone(t *testing.T) {
	ex := durableEnv(t, func(_ context.Context, e asynxmd.Event[order]) error {
		if e.EventName == asynxmd.ForgetEventName {
			return errors.New("cannot drop state")
		}
		return nil
	})
	ctx := context.Background()
	if _, err := ex.Execute(ctx, update("agg", 1), true); err != nil {
		t.Fatal(err)
	}

	if err := ex.Forget(ctx, "agg"); !errors.Is(err, asynxmd.ErrForgetFailed) || !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("err = %v, want ErrForgetFailed wrapping ErrDeliveryParked", err)
	}
	if n := streamLen(t, ex.es.Store().(store.Memory), "agg"); n == 0 {
		t.Fatal("aggregate erased although a durable subscriber never took the tombstone")
	}
}

func TestForget_LetsAFailingDeliveryGoWhenTheTombstoneIsAccepted(t *testing.T) {
	attempted := make(chan struct{}, 8)
	ex := durableEnv(t, func(_ context.Context, e asynxmd.Event[order]) error {
		if e.EventName == asynxmd.ForgetEventName {
			return nil
		}
		attempted <- struct{}{}
		return errors.New("poison")
	}, dispatcher.WithRetryBackoff[order](time.Hour, time.Hour), dispatcher.WithMaxAttempts[order](100))
	ctx := context.Background()
	if _, err := ex.Execute(ctx, update("agg", 1), false); err != nil {
		t.Fatal(err)
	}
	<-attempted

	done := make(chan error, 1)
	go func() { done <- ex.Forget(ctx, "agg") }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Forget: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Forget held back by a failing delivery of the same aggregate")
	}
}
