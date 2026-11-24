package mocks

import (
	"github.com/char2cs/asynx/v2/models/failure"
	"testing"
)

func TestCancelOrderCmd_AggregateID(
	t *testing.T,
) {
	c := CancelOrderCmd{ID: "ord-2"}
	if c.AggregateID() != "ord-2" {
		t.Errorf(
			"AggregateID = %q, want ord-2",
			c.AggregateID(),
		)
	}
}

func TestCancelOrderCmd_EventName(
	t *testing.T,
) {
	c := CancelOrderCmd{}
	if c.EventName() != "OrderCancelled" {
		t.Errorf(
			"EventName = %q, want OrderCancelled",
			c.EventName(),
		)
	}
}

func TestCancelOrderCmd_ShouldSnapshot_False(
	t *testing.T,
) {
	c := CancelOrderCmd{}
	if c.ShouldSnapshot() {
		t.Error("ShouldSnapshot = true, want false")
	}
}

func TestCancelOrderCmd_Validate_FailsWhenCurrentNil(
	t *testing.T,
) {
	err := CancelOrderCmd{}.Validate(nil)
	if err != failure.ErrValidation {
		t.Errorf(
			"Validate(nil) = %v, want ErrValidation",
			err,
		)
	}
}

func TestCancelOrderCmd_Validate_FailsWhenAlreadyCancelled(
	t *testing.T,
) {
	current := &Order{Status: "Cancelled"}
	err := CancelOrderCmd{}.Validate(current)
	if err != failure.ErrValidation {
		t.Errorf(
			"Validate(Cancelled) = %v, want ErrValidation",
			err,
		)
	}
}

func TestCancelOrderCmd_Validate_PassesForActiveOrder(
	t *testing.T,
) {
	current := &Order{Status: "Pending"}
	if err := (CancelOrderCmd{}).Validate(current); err != nil {
		t.Errorf(
			"Validate(Pending) = %v, want nil",
			err,
		)
	}
}

func TestCancelOrderCmd_EmitEvent_SetsCancelledStatus(
	t *testing.T,
) {
	current := &Order{ID: "ord-3", Total: 50, Status: "Pending"}
	o := CancelOrderCmd{}.EmitEvent(current)
	if o.Status != "Cancelled" {
		t.Errorf(
			"Status = %q, want Cancelled",
			o.Status,
		)
	}

	if o.ID != "ord-3" ||
		o.Total != 50 {
		t.Errorf(
			"non-status fields mutated: got %+v",
			o,
		)
	}
}
