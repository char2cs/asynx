package mocks

import (
	"github.com/char2cs/asynx/models/failure"
	"testing"
)

func TestCreateOrderCmd_AggregateID(
	t *testing.T,
) {
	c := CreateOrderCmd{ID: "ord-1"}
	if c.AggregateID() != "ord-1" {
		t.Errorf(
			"AggregateID = %q, want ord-1",
			c.AggregateID(),
		)
	}
}

func TestCreateOrderCmd_EventName(
	t *testing.T,
) {
	c := CreateOrderCmd{}
	if c.EventName() != "OrderCreated" {
		t.Errorf(
			"EventName = %q, want OrderCreated",
			c.EventName(),
		)
	}
}

func TestCreateOrderCmd_ShouldSnapshot_False(
	t *testing.T,
) {
	c := CreateOrderCmd{}
	if c.ShouldSnapshot() {
		t.Error("ShouldSnapshot = true, want false")
	}
}

func TestCreateOrderCmd_Validate_FailsWhenTotalZero(
	t *testing.T,
) {
	err := CreateOrderCmd{ID: "x", Total: 0}.Validate(nil)
	if err != failure.ErrValidation {
		t.Errorf(
			"Validate(Total=0) = %v, want ErrValidation",
			err,
		)
	}
}

func TestCreateOrderCmd_Validate_FailsWhenTotalNegative(
	t *testing.T,
) {
	err := CreateOrderCmd{ID: "x", Total: -1}.Validate(nil)
	if err != failure.ErrValidation {
		t.Errorf(
			"Validate(Total=-1) = %v, want ErrValidation",
			err,
		)
	}
}

func TestCreateOrderCmd_Validate_PassesWhenTotalPositive(
	t *testing.T,
) {
	if err := (CreateOrderCmd{ID: "x", Total: 10}).Validate(nil); err != nil {
		t.Errorf(
			"Validate(Total=10) = %v, want nil",
			err,
		)
	}
}

func TestCreateOrderCmd_EmitEvent_ReturnsPendingOrder(
	t *testing.T,
) {
	cmd := CreateOrderCmd{ID: "ord-1", Total: 99}
	o := cmd.EmitEvent(nil)
	if o.ID != "ord-1" ||
		o.Total != 99 ||
		o.Status != "Pending" {
		t.Errorf(
			"EmitEvent = %+v, want {ID:ord-1 Total:99 Status:Pending}",
			o,
		)
	}
}
