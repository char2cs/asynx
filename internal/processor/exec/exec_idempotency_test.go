package exec

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

type idemUpdate struct {
	mocks.UpdateOrderCmd
	Cmd string
}

func (c idemUpdate) CommandID() string { return c.Cmd }

type idemCreate struct {
	mocks.CreateOrderCmd
	Cmd string
}

func (c idemCreate) CommandID() string { return c.Cmd }

func TestExecute_Idempotent_AcceptedRetryIsNoOpReturningOriginalEvent(t *testing.T) {
	ex, s, _, _, _ := newEnv(t)
	cmd := idemUpdate{UpdateOrderCmd: update("agg", 5), Cmd: "cmd-1"}

	first, err := ex.Execute(context.Background(), cmd, false)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ex.Execute(context.Background(), cmd, false)
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}

	if second.ID != first.ID || second.Version != first.Version || second.Aggregate.Total != 5 {
		t.Fatalf("retry returned %+v, want the original event %+v", second, first)
	}
	if n := streamLen(t, s, "agg"); n != 1 {
		t.Fatalf("stream length = %d, want 1 (retry must not write)", n)
	}
}

func TestExecute_Idempotent_RejectedRetryStaysRejected(t *testing.T) {
	ex, s, _, _, _ := newEnv(t)
	bad := idemCreate{CreateOrderCmd: mocks.CreateOrderCmd{ID: "agg", Total: -1}, Cmd: "cmd-bad"}

	_, err := ex.Execute(context.Background(), bad, false)
	if !errors.Is(err, asynxmd.ErrValidation) {
		t.Fatalf("first err = %v, want ErrValidation", err)
	}

	// Same id, now with a body that would pass validation: still rejected.
	good := idemCreate{CreateOrderCmd: mocks.CreateOrderCmd{ID: "agg", Total: 10}, Cmd: "cmd-bad"}
	_, err = ex.Execute(context.Background(), good, false)
	if !errors.Is(err, asynxmd.ErrValidation) {
		t.Fatalf("retry err = %v, want ErrValidation (a rejection stays rejected)", err)
	}
	if n := streamLen(t, s, "agg"); n != 0 {
		t.Fatalf("stream length = %d, want 0", n)
	}
}

func TestExecute_Idempotent_StorageFailureLeavesNoReceiptSoRetryRuns(t *testing.T) {
	ex, s, _, _, _ := newEnv(t)
	cmd := idemUpdate{UpdateOrderCmd: update("agg", 5), Cmd: "cmd-1"}

	s.SetError("events:agg", errors.New("disk full"))
	if _, err := ex.Execute(context.Background(), cmd, false); !errors.Is(err, asynxmd.ErrPipelineFailed) {
		t.Fatalf("err = %v, want ErrPipelineFailed", err)
	}
	if _, found, _ := s.GetReceipt(context.Background(), "cmd-1"); found {
		t.Fatal("a failed write left a receipt")
	}

	evt, err := ex.Execute(context.Background(), cmd, false)
	if err != nil || evt.Version != 1 {
		t.Fatalf("retry = %+v, %v; want version 1", evt, err)
	}
}

func TestExecute_Idempotent_SyncProjectionFailureLeavesNoReceipt(t *testing.T) {
	fail := true
	ex, s, _, _, _ := newEnv(t, syncAll(func(context.Context, asynxmd.Tx, asynxmd.Event[order]) error {
		if fail {
			return errors.New("projection down")
		}
		return nil
	}))
	cmd := idemUpdate{UpdateOrderCmd: update("agg", 5), Cmd: "cmd-1"}

	if _, err := ex.Execute(context.Background(), cmd, false); err == nil {
		t.Fatal("expected failure")
	}
	if _, found, _ := s.GetReceipt(context.Background(), "cmd-1"); found {
		t.Fatal("rolled-back write left a receipt")
	}

	fail = false
	if _, err := ex.Execute(context.Background(), cmd, false); err != nil {
		t.Fatalf("retry failed: %v", err)
	}
}

func TestExecute_Idempotent_SameIDOnAnotherAggregateAnswersFromReceipt(t *testing.T) {
	ex, s, _, _, _ := newEnv(t)

	first, err := ex.Execute(context.Background(), idemUpdate{UpdateOrderCmd: update("agg-a", 1), Cmd: "cmd-1"}, false)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ex.Execute(context.Background(), idemUpdate{UpdateOrderCmd: update("agg-b", 2), Cmd: "cmd-1"}, false)
	if err != nil {
		t.Fatalf("second: %v", err)
	}

	if second.ID != first.ID || second.AggregateID != "agg-a" {
		t.Fatalf("second = %+v, want the original event on agg-a", second)
	}
	if n := streamLen(t, s, "agg-b"); n != 0 {
		t.Fatalf("agg-b stream length = %d, want 0", n)
	}
}

func TestExecute_EmptyCommandIDIsNotIdempotent(t *testing.T) {
	ex, s, _, _, _ := newEnv(t)
	cmd := idemUpdate{UpdateOrderCmd: update("agg", 5), Cmd: ""}

	for range 2 {
		if _, err := ex.Execute(context.Background(), cmd, false); err != nil {
			t.Fatal(err)
		}
	}
	if n := streamLen(t, s, "agg"); n != 2 {
		t.Fatalf("stream length = %d, want 2", n)
	}
}

func TestExecute_Idempotent_ReceiptReadErrorFailsPipeline(t *testing.T) {
	ex, _, _, _, _ := newEnvOn(t, &getReceiptErrStore{Memory: store.New()})
	cmd := idemUpdate{UpdateOrderCmd: update("agg", 5), Cmd: "cmd-1"}
	_, err := ex.Execute(context.Background(), cmd, false)
	if !errors.Is(err, asynxmd.ErrPipelineFailed) || !strings.Contains(err.Error(), "receipt store down") {
		t.Fatalf("err = %v, want ErrPipelineFailed(receipt store down)", err)
	}
}

type getReceiptErrStore struct{ store.Memory }

func (getReceiptErrStore) GetReceipt(context.Context, string) (asynxmd.Receipt, bool, error) {
	return asynxmd.Receipt{}, false, errors.New("receipt store down")
}

// A duplicate-receipt failure whose receipt cannot then be found must not be
// mistaken for a retry answer: it is a pipeline failure.
type phantomDuplicateStore struct{ store.Memory }

func (s phantomDuplicateStore) Atomically(ctx context.Context, fn func(asynxmd.Tx) error) error {
	return s.Memory.Atomically(ctx, func(tx asynxmd.Tx) error { return fn(phantomTx{tx}) })
}

type phantomTx struct{ asynxmd.Tx }

func (phantomTx) PutReceipt(context.Context, string, asynxmd.Receipt) error {
	return asynxmd.ErrDuplicateReceipt
}

func TestExecute_Idempotent_UnresolvableDuplicateReceiptFailsPipeline(t *testing.T) {
	ex, _, _, _, _ := newEnvOn(t, phantomDuplicateStore{store.New()})
	_, err := ex.Execute(context.Background(), idemUpdate{UpdateOrderCmd: update("agg", 1), Cmd: "c1"}, false)
	if !errors.Is(err, asynxmd.ErrPipelineFailed) || !errors.Is(err, asynxmd.ErrDuplicateReceipt) {
		t.Fatalf("err = %v, want ErrPipelineFailed + ErrDuplicateReceipt", err)
	}
}

func TestExecute_Idempotent_AcceptedReceiptForErasedAggregateStillAnswers(t *testing.T) {
	ex, _, _, _, _ := newEnv(t)
	ctx := context.Background()
	cmd := idemUpdate{UpdateOrderCmd: update("agg", 1), Cmd: "c1"}
	first, err := ex.Execute(ctx, cmd, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := ex.Forget(ctx, "agg"); err != nil {
		t.Fatal(err)
	}

	retry, err := ex.Execute(ctx, cmd, false)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if retry.ID != first.ID || retry.Version != first.Version {
		t.Fatalf("retry = %+v, want id/version of %+v", retry, first)
	}
}

// Simulates losing the race for a command id between the receipt check and the
// commit: the transaction reports a duplicate and the receipt is then found.
type raceLostStore struct {
	store.Memory
	reads int
}

func (s *raceLostStore) GetReceipt(ctx context.Context, id string) (asynxmd.Receipt, bool, error) {
	s.reads++
	if s.reads == 1 {
		return asynxmd.Receipt{}, false, nil
	}
	return asynxmd.Receipt{Accepted: true, AggregateID: "winner", EventID: "e-win", Version: 4}, true, nil
}

func (s *raceLostStore) Atomically(ctx context.Context, fn func(asynxmd.Tx) error) error {
	return s.Memory.Atomically(ctx, func(tx asynxmd.Tx) error { return fn(phantomTx{tx}) })
}

func TestExecute_Idempotent_LosingTheReceiptRaceAnswersWithTheWinner(t *testing.T) {
	ex, _, _, _, _ := newEnvOn(t, &raceLostStore{Memory: store.New()})
	evt, err := ex.Execute(context.Background(), idemUpdate{UpdateOrderCmd: update("agg", 1), Cmd: "c1"}, false)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if evt.ID != "e-win" || evt.AggregateID != "winner" || evt.Version != 4 {
		t.Fatalf("event = %+v, want the winner's", evt)
	}
}

// A rejection that cannot be recorded must not be reported as a rejection: a
// retry could pass validation later and succeed.
type putReceiptFailStore struct{ store.Memory }

func (s putReceiptFailStore) Atomically(ctx context.Context, fn func(asynxmd.Tx) error) error {
	return s.Memory.Atomically(ctx, func(tx asynxmd.Tx) error { return fn(failingReceiptTx{tx}) })
}

type failingReceiptTx struct{ asynxmd.Tx }

func (failingReceiptTx) PutReceipt(context.Context, string, asynxmd.Receipt) error {
	return errors.New("receipt table locked")
}

func TestExecute_Idempotent_UnrecordableRejectionFailsThePipeline(t *testing.T) {
	ex, _, _, _, _ := newEnvOn(t, putReceiptFailStore{store.New()})
	bad := idemCreate{CreateOrderCmd: mocks.CreateOrderCmd{ID: "agg", Total: -1}, Cmd: "c1"}

	_, err := ex.Execute(context.Background(), bad, false)
	if !errors.Is(err, asynxmd.ErrPipelineFailed) || errors.Is(err, asynxmd.ErrValidation) {
		t.Fatalf("err = %v, want ErrPipelineFailed and not a (non-durable) rejection", err)
	}
}

// Two commands with one id race: the loser's rejection hits the winner's receipt.
type rejectionRaceStore struct {
	store.Memory
	reads int
}

func (s *rejectionRaceStore) GetReceipt(context.Context, string) (asynxmd.Receipt, bool, error) {
	s.reads++
	if s.reads == 1 {
		return asynxmd.Receipt{}, false, nil
	}
	return asynxmd.Receipt{Rejection: "decided elsewhere"}, true, nil
}

func (s *rejectionRaceStore) Atomically(ctx context.Context, fn func(asynxmd.Tx) error) error {
	return s.Memory.Atomically(ctx, func(tx asynxmd.Tx) error { return fn(phantomTx{tx}) })
}

func TestExecute_Idempotent_RejectionRaceAnswersFromTheExistingReceipt(t *testing.T) {
	ex, _, _, _, _ := newEnvOn(t, &rejectionRaceStore{Memory: store.New()})
	bad := idemCreate{CreateOrderCmd: mocks.CreateOrderCmd{ID: "agg", Total: -1}, Cmd: "c1"}

	_, err := ex.Execute(context.Background(), bad, false)
	if !errors.Is(err, asynxmd.ErrValidation) || !strings.Contains(err.Error(), "decided elsewhere") {
		t.Fatalf("err = %v, want the existing rejection", err)
	}
}

// A retry after the aggregate was forgotten and re-created, where the new
// aggregate reuses the version, must not be answered with the new aggregate's
// event.
func TestExecute_Idempotent_RetryAfterForgetAndRecreateDoesNotReturnTheNewAggregate(t *testing.T) {
	ex, _, _, _, _ := newEnv(t)
	ctx := context.Background()
	first, err := ex.Execute(ctx, idemUpdate{UpdateOrderCmd: update("agg", 5), Cmd: "cmd-1"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := ex.Forget(ctx, "agg"); err != nil {
		t.Fatal(err)
	}
	second, err := ex.Execute(ctx, update("agg", 99), false)
	if err != nil {
		t.Fatal(err)
	}

	retry, err := ex.Execute(ctx, idemUpdate{UpdateOrderCmd: update("agg", 5), Cmd: "cmd-1"}, false)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if retry.ID != first.ID || retry.ID == second.ID {
		t.Fatalf("retry returned event %s, want the original %s (not the new aggregate's %s)", retry.ID, first.ID, second.ID)
	}
	if retry.Aggregate.Total == 99 {
		t.Fatal("retry returned the re-created aggregate's state")
	}
}

// readFailStore fails the reads of event streams once switched on.
type readFailStore struct {
	store.Memory
	fail atomic.Bool
}

func (s *readFailStore) ReadFrom(
	ctx context.Context,
	streamID string,
	fromVersion int64,
) ([][]byte, error) {
	if s.fail.Load() {
		return nil, errors.New("stream unreadable")
	}
	return s.Memory.ReadFrom(ctx, streamID, fromVersion)
}

// A retry that cannot read the original event back must say so, not hand the
// caller a hollow event as if all were well.
func TestExecute_Idempotent_RetryFailsWhenTheOriginalEventCannotBeRead(t *testing.T) {
	s := &readFailStore{Memory: store.New()}
	ex, _, _, _, _ := newEnvOn(t, s)
	cmd := idemUpdate{UpdateOrderCmd: update("agg", 5), Cmd: "cmd-1"}
	if _, err := ex.Execute(context.Background(), cmd, false); err != nil {
		t.Fatal(err)
	}

	s.fail.Store(true)
	evt, err := ex.Execute(context.Background(), cmd, false)
	if !errors.Is(err, asynxmd.ErrPipelineFailed) {
		t.Fatalf("retry = %+v, %v; want ErrPipelineFailed", evt, err)
	}

	s.fail.Store(false)
	if evt, err := ex.Execute(context.Background(), cmd, false); err != nil || evt.Aggregate.Total != 5 {
		t.Fatalf("retry once the stream is readable = %+v, %v; want the original event", evt, err)
	}
}

// The retry of a command whose handlers are still running waits for them, as
// the first attempt would have: read-your-writes must not depend on whether the
// caller is on its first attempt or its second.
func TestExecute_Idempotent_RetryWaitsForTheOriginalEventsHandlers(t *testing.T) {
	ex, _, _, _, b := newEnv(t)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	if _, err := b.Subscribe("OrderUpdated", func(
		context.Context,
		asynxmd.Event[order],
	) {
		entered <- struct{}{}
		<-release
	}); err != nil {
		t.Fatal(err)
	}
	cmd := idemUpdate{UpdateOrderCmd: update("agg", 5), Cmd: "cmd-1"}
	ctx := context.Background()

	first, _, err := ex.ExecuteAsync(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	within(t, entered, "the handler")

	retried, wait, err := ex.ExecuteAsync(ctx, cmd)
	if err != nil || retried.ID != first.ID {
		t.Fatalf("retry = %+v, %v; want the original event", retried, err)
	}
	if wait == nil {
		t.Fatal("the retry has nothing to wait on although the handler is still running")
	}
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := wait(short); !errors.Is(err, asynxmd.ErrContextCancelled) {
		t.Fatalf("wait = %v while the handler runs, want ErrContextCancelled", err)
	}

	releaseOnce.Do(func() { close(release) })
	if err := wait(ctx); err != nil {
		t.Fatalf("wait after the handler finished = %v", err)
	}
	if _, wait, err := ex.ExecuteAsync(ctx, cmd); err != nil || wait != nil {
		t.Fatalf("retry after the delivery is over: wait set = %v, err = %v; want nothing to wait on", wait != nil, err)
	}
}

func TestExecute_Idempotent_RejectedRetryHasNothingToWaitOn(t *testing.T) {
	ex, _, _, _, _ := newEnv(t)
	bad := idemCreate{CreateOrderCmd: mocks.CreateOrderCmd{ID: "agg", Total: -1}, Cmd: "cmd-bad"}
	if _, _, err := ex.ExecuteAsync(context.Background(), bad); !errors.Is(err, asynxmd.ErrValidation) {
		t.Fatalf("first err = %v", err)
	}

	_, wait, err := ex.ExecuteAsync(context.Background(), bad)
	if !errors.Is(err, asynxmd.ErrValidation) || wait != nil {
		t.Fatalf("retry: err = %v, wait set = %v; want ErrValidation and no wait", err, wait != nil)
	}
}

func TestExecute_Idempotent_RetryWithoutADispatcherHasNothingToWaitOn(t *testing.T) {
	es := eventstore.New[order](store.New(), store.NewSnapshots(), nil, 1, nil)
	ex := New(es, nil)
	cmd := idemUpdate{UpdateOrderCmd: update("agg", 5), Cmd: "cmd-1"}
	if _, _, err := ex.ExecuteAsync(context.Background(), cmd); err != nil {
		t.Fatal(err)
	}

	if _, wait, err := ex.ExecuteAsync(context.Background(), cmd); err != nil || wait != nil {
		t.Fatalf("retry: err = %v, wait set = %v", err, wait != nil)
	}
}

// The retry of a command whose event the dispatcher left undelivered because its
// aggregate is parked is told so, as the first attempt was; the retry of one
// that was delivered before the park is not.
func TestExecute_Idempotent_RetryOnAParkedAggregateRepeatsWhatTheFirstAttemptWasTold(t *testing.T) {
	ex := durableEnv(t, func(
		context.Context,
		asynxmd.Event[order],
	) error {
		return nil
	})
	ctx := context.Background()
	delivered := idemUpdate{UpdateOrderCmd: update("agg", 1), Cmd: "cmd-1"}
	_, wait, err := ex.ExecuteAsync(ctx, delivered)
	if err != nil {
		t.Fatal(err)
	}
	if err := wait(ctx); err != nil {
		t.Fatal(err)
	}
	ex.dispatcher.Park("agg")
	leftBehind := idemUpdate{UpdateOrderCmd: update("agg", 2), Cmd: "cmd-2"}
	if _, _, err := ex.ExecuteAsync(ctx, leftBehind); err != nil {
		t.Fatal(err)
	}

	_, wait, err = ex.ExecuteAsync(ctx, leftBehind)
	if err != nil || wait == nil {
		t.Fatalf("retry of the command left undelivered: err = %v, wait set = %v", err, wait != nil)
	}
	if err := wait(ctx); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("wait = %v, want ErrDeliveryParked", err)
	}
	if _, wait, err := ex.ExecuteAsync(ctx, delivered); err != nil || wait != nil {
		t.Fatalf("retry of the command delivered before the park: err = %v, wait set = %v; want nothing to report", err, wait != nil)
	}
}
