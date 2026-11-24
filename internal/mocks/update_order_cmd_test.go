package mocks

import (
	"testing"
)

func TestUpdateOrderCmd_AggregateID(
	t *testing.T,
) {
	c := UpdateOrderCmd{ID: "ord-4"}
	if c.AggregateID() != "ord-4" {
		t.Errorf(
			"AggregateID = %q, want ord-4",
			c.AggregateID(),
		)
	}
}

func TestUpdateOrderCmd_EventName(
	t *testing.T,
) {
	c := UpdateOrderCmd{}
	if c.EventName() != "OrderUpdated" {
		t.Errorf(
			"EventName = %q, want OrderUpdated",
			c.EventName(),
		)
	}
}

func TestUpdateOrderCmd_ShouldSnapshot_WhenTrue(
	t *testing.T,
) {
	c := UpdateOrderCmd{Snapshot: true}
	if !c.ShouldSnapshot() {
		t.Error("ShouldSnapshot = false, want true")
	}
}

func TestUpdateOrderCmd_ShouldSnapshot_WhenFalse(
	t *testing.T,
) {
	c := UpdateOrderCmd{}
	if c.ShouldSnapshot() {
		t.Error("ShouldSnapshot = true, want false")
	}
}

func TestUpdateOrderCmd_Validate_AlwaysNil(
	t *testing.T,
) {
	c := UpdateOrderCmd{}
	if err := c.Validate(nil); err != nil {
		t.Errorf(
			"Validate(nil) = %v, want nil",
			err,
		)
	}

	if err := c.Validate(&Order{}); err != nil {
		t.Errorf(
			"Validate(Order{}) = %v, want nil",
			err,
		)
	}
}

func TestUpdateOrderCmd_EmitEvent_ReturnsNewState(
	t *testing.T,
) {
	newState := Order{ID: "x", Status: "Shipped", Total: 200}
	o := UpdateOrderCmd{NewState: newState}.EmitEvent(nil)
	if o != newState {
		t.Errorf(
			"EmitEvent = %+v, want %+v",
			o,
			newState,
		)
	}
}
