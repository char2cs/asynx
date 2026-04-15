package commands

import (
	"errors"
	"testing"

	"github.com/char2cs/asynx/models"
)

type order struct {
	ID    string
	Total float64
}

func TestForgetCommand_Metadata(t *testing.T) {
	cmd := NewForgetCommand[order]("agg-1")
	if cmd.AggregateID() != "agg-1" {
		t.Errorf("AggregateID = %q, want agg-1", cmd.AggregateID())
	}
	if cmd.EventName() != "asynx.aggregate.forget" {
		t.Errorf("EventName = %q, want asynx.aggregate.forget", cmd.EventName())
	}
	if cmd.ShouldSnapshot() {
		t.Error("ShouldSnapshot = true, want false")
	}
}

func TestForgetCommand_Validate_NilCurrent_ReturnsErrValidation(t *testing.T) {
	cmd := NewForgetCommand[order]("agg-1")
	err := cmd.Validate(nil)
	if !errors.Is(err, models.ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestForgetCommand_Validate_ExistingAggregate_SetsLast(t *testing.T) {
	cmd := NewForgetCommand[order]("agg-1")
	state := order{ID: "agg-1", Total: 50.0}
	if err := cmd.Validate(&state); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	got := cmd.EmitEvent(nil)
	if got.Total != 50.0 {
		t.Errorf("EmitEvent Total = %v, want 50.0", got.Total)
	}
}

func TestForgetCommand_EmitEvent_NilLast_ReturnsZeroValue(t *testing.T) {
	cmd := NewForgetCommand[order]("agg-1")
	// EmitEvent without prior Validate — c.last is nil, should return zero value.
	got := cmd.EmitEvent(nil)
	var zero order
	if got != zero {
		t.Errorf("EmitEvent = %+v, want zero value", got)
	}
}

// Ensure ForgetCommand satisfies models.Command[T] at compile time.
var _ models.Command[order] = (*ForgetCommand[order])(nil)

