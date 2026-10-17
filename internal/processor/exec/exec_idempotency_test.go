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

func streamLen(t *testing.T, s interface {
	ReadFrom(context.Context, string, int64) ([][]byte, error)
}, agg string) int {
	t.Helper()
	blobs, err := s.ReadFrom(context.Background(), "events:"+agg, 1)
	if err != nil {
		t.Fatal(err)
	}
	return len(blobs)
}

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
