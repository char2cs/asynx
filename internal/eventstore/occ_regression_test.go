package eventstore

import (
	"context"
	"errors"
	"testing"

	"github.com/char2cs/asynx/internal/mocks"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

// gatedCancel is a cancel command whose Validate runs a hook, letting the test
// pause a writer at the exact seam between loading state and appending — the
// window where the stale-validation race lives.
type gatedCancel struct {
	id         string
	onValidate func(current *order)
}

func (c gatedCancel) AggregateID() string { return c.id }
func (c gatedCancel) EventName() string   { return "OrderCancelled" }
func (c gatedCancel) ShouldSnapshot() bool { return false }

func (c gatedCancel) Validate(current *order) error {
	if c.onValidate != nil {
		c.onValidate(current)
	}
	if current == nil || current.Status == "Cancelled" {
		return asynxmd.ErrValidation
	}
	return nil
}

func (c gatedCancel) EmitEvent(current *order) order {
	o := *current
	o.Status = "Cancelled"
	return o
}

// TestWrite_StaleValidation_ConflictsInsteadOfDoubleCommit pins the exact race
// that let a stale-validated write slip past the version check:
//
//  1. B loads Pending state and parks inside Validate.
//  2. A cancels and commits while B is parked.
//  3. B leaves Validate still judging the stale Pending state.
//
// Before the fix B re-derived a fresh version (v3) and committed a second
// cancel. Now B appends at its validated version (v2), which A already took, so
// B gets ErrPipelineFailed and exactly one cancel survives.
func TestWrite_StaleValidation_ConflictsInsteadOfDoubleCommit(t *testing.T) {
	s := store.New()
	es := New[order](s, store.NewSnapshots(), nil, 1, nil)
	ctx := context.Background()

	if _, err := es.Write(ctx, mocks.CreateOrderCmd{ID: "X", Total: 100}); err != nil {
		t.Fatalf("seed create: %v", err)
	}

	aCommitted := make(chan struct{})
	bInValidate := make(chan struct{})

	bCmd := gatedCancel{id: "X", onValidate: func(cur *order) {
		if cur == nil || cur.Status != "Pending" {
			t.Errorf("B expected to load Pending, got %+v", cur)
		}
		close(bInValidate)
		<-aCommitted
	}}

	var bErr error
	bDone := make(chan struct{})
	go func() {
		_, bErr = es.Write(ctx, bCmd)
		close(bDone)
	}()

	<-bInValidate
	if _, err := es.Write(ctx, gatedCancel{id: "X"}); err != nil {
		t.Fatalf("A cancel: %v", err)
	}
	close(aCommitted)
	<-bDone

	if !errors.Is(bErr, asynxmd.ErrPipelineFailed) {
		t.Errorf("B err = %v, want ErrPipelineFailed (stale write must be rejected)", bErr)
	}

	cancels := 0
	if err := es.Replay(ctx, "X", 1, 0, func(_ context.Context, e asynxmd.Event[order]) {
		if e.EventName == "OrderCancelled" {
			cancels++
		}
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if cancels != 1 {
		t.Fatalf("order X cancelled %d times, want exactly 1", cancels)
	}
}
