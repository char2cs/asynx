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

// CommandID returns the idempotency key the test put in Cmd, making the
// update an IdempotentCommand.
func (c idemUpdate) CommandID() string { return c.Cmd }

type idemCreate struct {
	mocks.CreateOrderCmd
	Cmd string
}

// CommandID returns the idempotency key the test put in Cmd, making the
// create an IdempotentCommand.
func (c idemCreate) CommandID() string { return c.Cmd }

func TestExecute_Idempotent_AcceptedRetryIsNoOpReturningOriginalEvent(
	t *testing.T,
) {
	ex, s, _, _, _ := newEnv(t)
	cmd := idemUpdate{UpdateOrderCmd: update(
		"agg",
		5,
	), Cmd: "cmd-1"}

	first, err := ex.Execute(
		context.Background(),
		cmd,
	)
	if err != nil {
		t.Fatal(err)
	}

	second, err := ex.Execute(
		context.Background(),
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"retry failed: %v",
			err,
		)
	}

	if second.ID != first.ID ||
		second.Version != first.Version ||
		second.Aggregate.Total != 5 {
		t.Fatalf(
			"retry returned %+v, want the original event %+v",
			second,
			first,
		)
	}

	if n := streamLen(
		t,
		s,
		"agg",
	); n != 1 {
		t.Fatalf(
			"stream length = %d, want 1 (retry must not write)",
			n,
		)
	}
}

func TestExecute_Idempotent_RejectedRetryStaysRejected(
	t *testing.T,
) {
	ex, s, _, _, _ := newEnv(t)
	bad := idemCreate{CreateOrderCmd: mocks.CreateOrderCmd{ID: "agg", Total: -1}, Cmd: "cmd-bad"}

	_, err := ex.Execute(
		context.Background(),
		bad,
	)
	if !errors.Is(
		err,
		asynxmd.ErrValidation,
	) {
		t.Fatalf(
			"first err = %v, want ErrValidation",
			err,
		)
	}

	good := idemCreate{CreateOrderCmd: mocks.CreateOrderCmd{ID: "agg", Total: 10}, Cmd: "cmd-bad"}
	_, err = ex.Execute(
		context.Background(),
		good,
	)
	if !errors.Is(
		err,
		asynxmd.ErrValidation,
	) {
		t.Fatalf(
			"retry err = %v, want ErrValidation (a rejection stays rejected)",
			err,
		)
	}

	if n := streamLen(
		t,
		s,
		"agg",
	); n != 0 {
		t.Fatalf(
			"stream length = %d, want 0",
			n,
		)
	}
}

func TestExecute_Idempotent_StorageFailureLeavesNoReceiptSoRetryRuns(
	t *testing.T,
) {
	ex, s, _, _, _ := newEnv(t)
	cmd := idemUpdate{UpdateOrderCmd: update(
		"agg",
		5,
	), Cmd: "cmd-1"}

	s.SetError(
		"events:agg",
		errors.New("disk full"),
	)
	if _, err := ex.Execute(
		context.Background(),
		cmd,
	); !errors.Is(
		err,
		asynxmd.ErrPipelineFailed,
	) {
		t.Fatalf(
			"err = %v, want ErrPipelineFailed",
			err,
		)
	}

	if _, found, _ := s.GetReceipt(
		context.Background(),
		"cmd-1",
	); found {
		t.Fatal("a failed write left a receipt")
	}

	evt, err := ex.Execute(
		context.Background(),
		cmd,
	)
	if err != nil ||
		evt.Version != 1 {
		t.Fatalf(
			"retry = %+v, %v; want version 1",
			evt,
			err,
		)
	}
}

func TestExecute_Idempotent_SyncProjectionFailureLeavesNoReceipt(
	t *testing.T,
) {
	fail := true
	projection := func(
		context.Context,
		asynxmd.Tx,
		asynxmd.Event[order],
	) error {
		return readModelState(fail)
	}
	ex, s, _, _, _ := newEnv(
		t,
		syncAll(projection),
	)
	cmd := idemUpdate{UpdateOrderCmd: update(
		"agg",
		5,
	), Cmd: "cmd-1"}

	if _, err := ex.Execute(
		context.Background(),
		cmd,
	); err == nil {
		t.Fatal("expected failure")
	}

	if _, found, _ := s.GetReceipt(
		context.Background(),
		"cmd-1",
	); found {
		t.Fatal("rolled-back write left a receipt")
	}

	fail = false
	if _, err := ex.Execute(
		context.Background(),
		cmd,
	); err != nil {
		t.Fatalf(
			"retry failed: %v",
			err,
		)
	}
}

func TestExecute_Idempotent_SameIDOnAnotherAggregateAnswersFromReceipt(
	t *testing.T,
) {
	ex, s, _, _, _ := newEnv(t)

	first, err := ex.Execute(
		context.Background(),
		idemUpdate{UpdateOrderCmd: update(
			"agg-a",
			1,
		), Cmd: "cmd-1"},
	)
	if err != nil {
		t.Fatal(err)
	}

	second, err := ex.Execute(
		context.Background(),
		idemUpdate{UpdateOrderCmd: update(
			"agg-b",
			2,
		), Cmd: "cmd-1"},
	)
	if err != nil {
		t.Fatalf(
			"second: %v",
			err,
		)
	}

	if second.ID != first.ID ||
		second.AggregateID != "agg-a" {
		t.Fatalf(
			"second = %+v, want the original event on agg-a",
			second,
		)
	}

	if n := streamLen(
		t,
		s,
		"agg-b",
	); n != 0 {
		t.Fatalf(
			"agg-b stream length = %d, want 0",
			n,
		)
	}
}

func requireExecuted(
	t *testing.T,
	ex *CommandExecutor[order],
	cmd asynxmd.Command[order],
) {
	t.Helper()
	if _, err := ex.Execute(
		context.Background(),
		cmd,
	); err != nil {
		t.Fatal(err)
	}
}

func TestExecute_EmptyCommandIDIsNotIdempotent(
	t *testing.T,
) {
	ex, s, _, _, _ := newEnv(t)
	cmd := idemUpdate{UpdateOrderCmd: update(
		"agg",
		5,
	), Cmd: ""}

	for range 2 {
		requireExecuted(
			t,
			ex,
			cmd,
		)
	}

	if n := streamLen(
		t,
		s,
		"agg",
	); n != 2 {
		t.Fatalf(
			"stream length = %d, want 2",
			n,
		)
	}
}

func TestExecute_Idempotent_ReceiptReadErrorFailsPipeline(
	t *testing.T,
) {
	ex, _, _, _, _ := newEnvOn(
		t,
		&getReceiptErrStore{Memory: store.New()},
	)
	cmd := idemUpdate{UpdateOrderCmd: update(
		"agg",
		5,
	), Cmd: "cmd-1"}
	_, err := ex.Execute(
		context.Background(),
		cmd,
	)
	if !errors.Is(
		err,
		asynxmd.ErrPipelineFailed,
	) ||
		!strings.Contains(
			err.Error(),
			"receipt store down",
		) {
		t.Fatalf(
			"err = %v, want ErrPipelineFailed(receipt store down)",
			err,
		)
	}
}

type getReceiptErrStore struct{ store.Memory }

// GetReceipt always fails with "receipt store down", simulating an unreadable
// receipt table.
func (getReceiptErrStore) GetReceipt(
	context.Context,
	string,
) (asynxmd.Receipt, bool, error) {
	return asynxmd.Receipt{},
		false,
		errors.New("receipt store down")
}

type phantomDuplicateStore struct{ store.Memory }

// Atomically runs fn in a transaction whose PutReceipt always reports
// ErrDuplicateReceipt, simulating a duplicate that GetReceipt cannot find.
func (s phantomDuplicateStore) Atomically(
	ctx context.Context,
	fn func(asynxmd.Tx) error,
) error {
	wrap := func(
		tx asynxmd.Tx,
	) error {
		return fn(phantomTx{tx})
	}
	return s.Memory.Atomically(
		ctx,
		wrap,
	)
}

type phantomTx struct{ asynxmd.Tx }

// PutReceipt always fails with ErrDuplicateReceipt, as if another writer had
// recorded the command first.
func (phantomTx) PutReceipt(
	context.Context,
	string,
	asynxmd.Receipt,
) error {
	return asynxmd.ErrDuplicateReceipt
}

func TestExecute_Idempotent_UnresolvableDuplicateReceiptFailsPipeline(
	t *testing.T,
) {
	ex, _, _, _, _ := newEnvOn(
		t,
		phantomDuplicateStore{store.New()},
	)
	_, err := ex.Execute(
		context.Background(),
		idemUpdate{UpdateOrderCmd: update(
			"agg",
			1,
		), Cmd: "c1"},
	)
	if !errors.Is(
		err,
		asynxmd.ErrPipelineFailed,
	) ||
		!errors.Is(
			err,
			asynxmd.ErrDuplicateReceipt,
		) {
		t.Fatalf(
			"err = %v, want ErrPipelineFailed + ErrDuplicateReceipt",
			err,
		)
	}
}

func TestExecute_Idempotent_AcceptedReceiptForErasedAggregateStillAnswers(
	t *testing.T,
) {
	ex, _, _, _, _ := newEnv(t)
	ctx := context.Background()
	cmd := idemUpdate{UpdateOrderCmd: update(
		"agg",
		1,
	), Cmd: "c1"}
	first, err := ex.Execute(
		ctx,
		cmd,
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := ex.Forget(
		ctx,
		"agg",
	); err != nil {
		t.Fatal(err)
	}

	retry, err := ex.Execute(
		ctx,
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"retry: %v",
			err,
		)
	}

	if retry.ID != first.ID ||
		retry.Version != first.Version {
		t.Fatalf(
			"retry = %+v, want id/version of %+v",
			retry,
			first,
		)
	}
}

type raceLostStore struct {
	store.Memory
	reads int
}

// GetReceipt finds no receipt on its first call and an accepted receipt for
// another aggregate afterwards, simulating losing a race to a concurrent
// writer of the same command id.
func (s *raceLostStore) GetReceipt(
	ctx context.Context,
	id string,
) (asynxmd.Receipt, bool, error) {
	s.reads++
	if s.reads == 1 {
		return asynxmd.Receipt{},
			false,
			nil
	}

	return asynxmd.Receipt{Accepted: true, AggregateID: "winner", EventID: "e-win", Version: 4},
		true,
		nil
}

// Atomically runs fn in a transaction whose PutReceipt reports
// ErrDuplicateReceipt, completing the lost-race simulation.
func (s *raceLostStore) Atomically(
	ctx context.Context,
	fn func(asynxmd.Tx) error,
) error {
	wrap := func(
		tx asynxmd.Tx,
	) error {
		return fn(phantomTx{tx})
	}
	return s.Memory.Atomically(
		ctx,
		wrap,
	)
}

func TestExecute_Idempotent_LosingTheReceiptRaceAnswersWithTheWinner(
	t *testing.T,
) {
	ex, _, _, _, _ := newEnvOn(
		t,
		&raceLostStore{Memory: store.New()},
	)
	evt, err := ex.Execute(
		context.Background(),
		idemUpdate{UpdateOrderCmd: update(
			"agg",
			1,
		), Cmd: "c1"},
	)
	if err != nil {
		t.Fatalf(
			"err = %v",
			err,
		)
	}

	if evt.ID != "e-win" ||
		evt.AggregateID != "winner" ||
		evt.Version != 4 {
		t.Fatalf(
			"event = %+v, want the winner's",
			evt,
		)
	}
}

type putReceiptFailStore struct{ store.Memory }

// Atomically runs fn in a transaction whose PutReceipt fails, simulating a
// store that cannot record receipts.
func (s putReceiptFailStore) Atomically(
	ctx context.Context,
	fn func(asynxmd.Tx) error,
) error {
	wrap := func(
		tx asynxmd.Tx,
	) error {
		return fn(failingReceiptTx{tx})
	}
	return s.Memory.Atomically(
		ctx,
		wrap,
	)
}

type failingReceiptTx struct{ asynxmd.Tx }

// PutReceipt always fails with "receipt table locked".
func (failingReceiptTx) PutReceipt(
	context.Context,
	string,
	asynxmd.Receipt,
) error {
	return errors.New("receipt table locked")
}

func TestExecute_Idempotent_UnrecordableRejectionFailsThePipeline(
	t *testing.T,
) {
	ex, _, _, _, _ := newEnvOn(
		t,
		putReceiptFailStore{store.New()},
	)
	bad := idemCreate{CreateOrderCmd: mocks.CreateOrderCmd{ID: "agg", Total: -1}, Cmd: "c1"}

	_, err := ex.Execute(
		context.Background(),
		bad,
	)
	if !errors.Is(
		err,
		asynxmd.ErrPipelineFailed,
	) ||
		errors.Is(
			err,
			asynxmd.ErrValidation,
		) {
		t.Fatalf(
			"err = %v, want ErrPipelineFailed and not a (non-durable) rejection",
			err,
		)
	}
}

type rejectionRaceStore struct {
	store.Memory
	reads int
}

// GetReceipt finds no receipt on its first call and a recorded rejection
// afterwards, simulating another writer rejecting the same command id first.
func (s *rejectionRaceStore) GetReceipt(
	context.Context,
	string,
) (asynxmd.Receipt, bool, error) {
	s.reads++
	if s.reads == 1 {
		return asynxmd.Receipt{},
			false,
			nil
	}

	return asynxmd.Receipt{Rejection: "decided elsewhere"},
		true,
		nil
}

// Atomically runs fn in a transaction whose PutReceipt reports
// ErrDuplicateReceipt, completing the rejection-race simulation.
func (s *rejectionRaceStore) Atomically(
	ctx context.Context,
	fn func(asynxmd.Tx) error,
) error {
	wrap := func(
		tx asynxmd.Tx,
	) error {
		return fn(phantomTx{tx})
	}
	return s.Memory.Atomically(
		ctx,
		wrap,
	)
}

func TestExecute_Idempotent_RejectionRaceAnswersFromTheExistingReceipt(
	t *testing.T,
) {
	ex, _, _, _, _ := newEnvOn(
		t,
		&rejectionRaceStore{Memory: store.New()},
	)
	bad := idemCreate{CreateOrderCmd: mocks.CreateOrderCmd{ID: "agg", Total: -1}, Cmd: "c1"}

	_, err := ex.Execute(
		context.Background(),
		bad,
	)
	if !errors.Is(
		err,
		asynxmd.ErrValidation,
	) ||
		!strings.Contains(
			err.Error(),
			"decided elsewhere",
		) {
		t.Fatalf(
			"err = %v, want the existing rejection",
			err,
		)
	}
}

func TestExecute_Idempotent_RetryAfterForgetAndRecreateDoesNotReturnTheNewAggregate(
	t *testing.T,
) {
	ex, _, _, _, _ := newEnv(t)
	ctx := context.Background()
	first, err := ex.Execute(
		ctx,
		idemUpdate{UpdateOrderCmd: update(
			"agg",
			5,
		), Cmd: "cmd-1"},
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := ex.Forget(
		ctx,
		"agg",
	); err != nil {
		t.Fatal(err)
	}

	second, err := ex.Execute(
		ctx,
		update(
			"agg",
			99,
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	retry, err := ex.Execute(
		ctx,
		idemUpdate{UpdateOrderCmd: update(
			"agg",
			5,
		), Cmd: "cmd-1"},
	)
	if err != nil {
		t.Fatalf(
			"retry: %v",
			err,
		)
	}

	if retry.ID != first.ID ||
		retry.ID == second.ID {
		t.Fatalf(
			"retry returned event %s, want the original %s (not the new aggregate's %s)",
			retry.ID,
			first.ID,
			second.ID,
		)
	}

	if retry.Aggregate.Total == 99 {
		t.Fatal("retry returned the re-created aggregate's state")
	}
}

type readFailStore struct {
	store.Memory
	fail atomic.Bool
}

// ReadFrom fails with "stream unreadable" while fail is set and otherwise
// reads normally.
func (s *readFailStore) ReadFrom(
	ctx context.Context,
	streamID string,
	fromVersion int64,
) ([][]byte, error) {
	if s.fail.Load() {
		return nil,
			errors.New("stream unreadable")
	}

	return s.Memory.ReadFrom(
		ctx,
		streamID,
		fromVersion,
	)
}

func TestExecute_Idempotent_RetryFailsWhenTheOriginalEventCannotBeRead(
	t *testing.T,
) {
	s := &readFailStore{Memory: store.New()}
	ex, _, _, _, _ := newEnvOn(
		t,
		s,
	)
	cmd := idemUpdate{UpdateOrderCmd: update(
		"agg",
		5,
	), Cmd: "cmd-1"}
	if _, err := ex.Execute(
		context.Background(),
		cmd,
	); err != nil {
		t.Fatal(err)
	}

	s.fail.Store(true)
	evt, err := ex.Execute(
		context.Background(),
		cmd,
	)
	if !errors.Is(
		err,
		asynxmd.ErrPipelineFailed,
	) {
		t.Fatalf(
			"retry = %+v, %v; want ErrPipelineFailed",
			evt,
			err,
		)
	}

	s.fail.Store(false)
	if evt, err := ex.Execute(
		context.Background(),
		cmd,
	); err != nil ||
		evt.Aggregate.Total != 5 {
		t.Fatalf(
			"retry once the stream is readable = %+v, %v; want the original event",
			evt,
			err,
		)
	}
}

func TestExecute_Idempotent_RetryWaitsForTheOriginalEventsHandlers(
	t *testing.T,
) {
	ex, _, _, _, b := newEnv(t)
	entered := make(
		chan struct{},
		1,
	)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	hold := func(
		context.Context,
		asynxmd.Event[order],
	) {
		entered <- struct{}{}
		<-release
	}
	if _, err := b.Subscribe(
		"OrderUpdated",
		hold,
	); err != nil {
		t.Fatal(err)
	}

	cmd := idemUpdate{UpdateOrderCmd: update(
		"agg",
		5,
	), Cmd: "cmd-1"}
	ctx := context.Background()

	first, _, err := ex.ExecuteAsync(
		ctx,
		cmd,
	)
	if err != nil {
		t.Fatal(err)
	}

	within(
		t,
		entered,
		"the handler",
	)

	retried, wait, err := ex.ExecuteAsync(
		ctx,
		cmd,
	)
	if err != nil ||
		retried.ID != first.ID {
		t.Fatalf(
			"retry = %+v, %v; want the original event",
			retried,
			err,
		)
	}

	if wait == nil {
		t.Fatal("the retry has nothing to wait on although the handler is still running")
	}

	short, cancel := context.WithTimeout(
		ctx,
		20*time.Millisecond,
	)
	defer cancel()
	if err := wait(short); !errors.Is(
		err,
		asynxmd.ErrContextCancelled,
	) {
		t.Fatalf(
			"wait = %v while the handler runs, want ErrContextCancelled",
			err,
		)
	}

	releaseOnce.Do(func() { close(release) })
	if err := wait(ctx); err != nil {
		t.Fatalf(
			"wait after the handler finished = %v",
			err,
		)
	}

	awaitNothingToWaitOn(
		t,
		ex,
		cmd,
	)
}

func TestExecute_Idempotent_RejectedRetryHasNothingToWaitOn(
	t *testing.T,
) {
	ex, _, _, _, _ := newEnv(t)
	bad := idemCreate{CreateOrderCmd: mocks.CreateOrderCmd{ID: "agg", Total: -1}, Cmd: "cmd-bad"}
	if _, _, err := ex.ExecuteAsync(
		context.Background(),
		bad,
	); !errors.Is(
		err,
		asynxmd.ErrValidation,
	) {
		t.Fatalf(
			"first err = %v",
			err,
		)
	}

	_, wait, err := ex.ExecuteAsync(
		context.Background(),
		bad,
	)
	if !errors.Is(
		err,
		asynxmd.ErrValidation,
	) ||
		wait != nil {
		t.Fatalf(
			"retry: err = %v, wait set = %v; want ErrValidation and no wait",
			err,
			wait != nil,
		)
	}
}

func TestExecute_Idempotent_RetryWithoutADispatcherHasNothingToWaitOn(
	t *testing.T,
) {
	es := eventstore.New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	ex := New(
		es,
		nil,
	)
	cmd := idemUpdate{UpdateOrderCmd: update(
		"agg",
		5,
	), Cmd: "cmd-1"}
	if _, _, err := ex.ExecuteAsync(
		context.Background(),
		cmd,
	); err != nil {
		t.Fatal(err)
	}

	if _, wait, err := ex.ExecuteAsync(
		context.Background(),
		cmd,
	); err != nil ||
		wait != nil {
		t.Fatalf(
			"retry: err = %v, wait set = %v",
			err,
			wait != nil,
		)
	}
}

func TestExecute_Idempotent_RetryOnAParkedAggregateRepeatsWhatTheFirstAttemptWasTold(
	t *testing.T,
) {
	acceptAll := func(
		context.Context,
		asynxmd.Event[order],
	) error {
		return nil
	}
	ex := durableEnv(
		t,
		acceptAll,
	)
	ctx := context.Background()
	delivered := idemUpdate{UpdateOrderCmd: update(
		"agg",
		1,
	), Cmd: "cmd-1"}
	_, wait, err := ex.ExecuteAsync(
		ctx,
		delivered,
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := wait(ctx); err != nil {
		t.Fatal(err)
	}

	ex.dispatcher.Park(
		"agg",
		errors.New("parked by the test"),
	)
	leftBehind := idemUpdate{UpdateOrderCmd: update(
		"agg",
		2,
	), Cmd: "cmd-2"}
	if _, _, err := ex.ExecuteAsync(
		ctx,
		leftBehind,
	); err != nil {
		t.Fatal(err)
	}

	_, wait, err = ex.ExecuteAsync(
		ctx,
		leftBehind,
	)
	if err != nil ||
		wait == nil {
		t.Fatalf(
			"retry of the command left undelivered: err = %v, wait set = %v",
			err,
			wait != nil,
		)
	}

	if err := wait(ctx); !errors.Is(
		err,
		asynxmd.ErrDeliveryParked,
	) {
		t.Fatalf(
			"wait = %v, want ErrDeliveryParked",
			err,
		)
	}

	awaitNothingToWaitOn(
		t,
		ex,
		delivered,
	)
}

func awaitNothingToWaitOn(
	t *testing.T,
	ex *CommandExecutor[order],
	cmd asynxmd.Command[order],
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	done := false
	for !done && time.Now().Before(deadline) {
		done = retryFinished(
			t,
			ex,
			cmd,
		)
	}

	if !done {
		t.Fatal("retry of a delivered command: still something to wait on after 5s; want nothing")
	}
}

func retryFinished(
	t *testing.T,
	ex *CommandExecutor[order],
	cmd asynxmd.Command[order],
) bool {
	t.Helper()
	ctx := context.Background()
	_, wait, err := ex.ExecuteAsync(
		ctx,
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"retry of a delivered command: err = %v",
			err,
		)
	}

	if wait == nil {
		return true
	}

	if err := wait(ctx); err != nil {
		t.Fatalf(
			"a retry that still finds the finished delivery must not wait: %v",
			err,
		)
	}

	time.Sleep(time.Millisecond)
	return false
}
