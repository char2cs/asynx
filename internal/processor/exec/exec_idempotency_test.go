package exec

import (
	"context"
	"errors"
	"strings"
	"testing"

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
