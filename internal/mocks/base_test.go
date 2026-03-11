package mocks

import (
	"testing"

	asynxmd "github.com/char2cs/asynx/models"
)

// --- CreateOrderCmd ---

func TestCreateOrderCmd_AggregateID(t *testing.T) {
	c := CreateOrderCmd{ID: "ord-1"}
	if c.AggregateID() != "ord-1" {
		t.Errorf("AggregateID = %q, want ord-1", c.AggregateID())
	}
}

func TestCreateOrderCmd_EventName(t *testing.T) {
	c := CreateOrderCmd{}
	if c.EventName() != "OrderCreated" {
		t.Errorf("EventName = %q, want OrderCreated", c.EventName())
	}
}

func TestCreateOrderCmd_ShouldSnapshot_False(t *testing.T) {
	c := CreateOrderCmd{}
	if c.ShouldSnapshot() {
		t.Error("ShouldSnapshot = true, want false")
	}
}

func TestCreateOrderCmd_Validate_FailsWhenTotalZero(t *testing.T) {
	err := CreateOrderCmd{ID: "x", Total: 0}.Validate(nil)
	if err != asynxmd.ErrValidation {
		t.Errorf("Validate(Total=0) = %v, want ErrValidation", err)
	}
}

func TestCreateOrderCmd_Validate_FailsWhenTotalNegative(t *testing.T) {
	err := CreateOrderCmd{ID: "x", Total: -1}.Validate(nil)
	if err != asynxmd.ErrValidation {
		t.Errorf("Validate(Total=-1) = %v, want ErrValidation", err)
	}
}

func TestCreateOrderCmd_Validate_PassesWhenTotalPositive(t *testing.T) {
	if err := (CreateOrderCmd{ID: "x", Total: 10}).Validate(nil); err != nil {
		t.Errorf("Validate(Total=10) = %v, want nil", err)
	}
}

func TestCreateOrderCmd_EmitEvent_ReturnsPendingOrder(t *testing.T) {
	cmd := CreateOrderCmd{ID: "ord-1", Total: 99}
	o := cmd.EmitEvent(nil)
	if o.ID != "ord-1" || o.Total != 99 || o.Status != "Pending" {
		t.Errorf("EmitEvent = %+v, want {ID:ord-1 Total:99 Status:Pending}", o)
	}
}

// --- CancelOrderCmd ---

func TestCancelOrderCmd_AggregateID(t *testing.T) {
	c := CancelOrderCmd{ID: "ord-2"}
	if c.AggregateID() != "ord-2" {
		t.Errorf("AggregateID = %q, want ord-2", c.AggregateID())
	}
}

func TestCancelOrderCmd_EventName(t *testing.T) {
	c := CancelOrderCmd{}
	if c.EventName() != "OrderCancelled" {
		t.Errorf("EventName = %q, want OrderCancelled", c.EventName())
	}
}

func TestCancelOrderCmd_ShouldSnapshot_False(t *testing.T) {
	c := CancelOrderCmd{}
	if c.ShouldSnapshot() {
		t.Error("ShouldSnapshot = true, want false")
	}
}

func TestCancelOrderCmd_Validate_FailsWhenCurrentNil(t *testing.T) {
	err := CancelOrderCmd{}.Validate(nil)
	if err != asynxmd.ErrValidation {
		t.Errorf("Validate(nil) = %v, want ErrValidation", err)
	}
}

func TestCancelOrderCmd_Validate_FailsWhenAlreadyCancelled(t *testing.T) {
	current := &Order{Status: "Cancelled"}
	err := CancelOrderCmd{}.Validate(current)
	if err != asynxmd.ErrValidation {
		t.Errorf("Validate(Cancelled) = %v, want ErrValidation", err)
	}
}

func TestCancelOrderCmd_Validate_PassesForActiveOrder(t *testing.T) {
	current := &Order{Status: "Pending"}
	if err := (CancelOrderCmd{}).Validate(current); err != nil {
		t.Errorf("Validate(Pending) = %v, want nil", err)
	}
}

func TestCancelOrderCmd_EmitEvent_SetsCancelledStatus(t *testing.T) {
	current := &Order{ID: "ord-3", Total: 50, Status: "Pending"}
	o := CancelOrderCmd{}.EmitEvent(current)
	if o.Status != "Cancelled" {
		t.Errorf("Status = %q, want Cancelled", o.Status)
	}
	if o.ID != "ord-3" || o.Total != 50 {
		t.Errorf("non-status fields mutated: got %+v", o)
	}
}

// --- UpdateOrderCmd ---

func TestUpdateOrderCmd_AggregateID(t *testing.T) {
	c := UpdateOrderCmd{ID: "ord-4"}
	if c.AggregateID() != "ord-4" {
		t.Errorf("AggregateID = %q, want ord-4", c.AggregateID())
	}
}

func TestUpdateOrderCmd_EventName(t *testing.T) {
	c := UpdateOrderCmd{}
	if c.EventName() != "OrderUpdated" {
		t.Errorf("EventName = %q, want OrderUpdated", c.EventName())
	}
}

func TestUpdateOrderCmd_ShouldSnapshot_WhenTrue(t *testing.T) {
	c := UpdateOrderCmd{Snapshot: true}
	if !c.ShouldSnapshot() {
		t.Error("ShouldSnapshot = false, want true")
	}
}

func TestUpdateOrderCmd_ShouldSnapshot_WhenFalse(t *testing.T) {
	c := UpdateOrderCmd{}
	if c.ShouldSnapshot() {
		t.Error("ShouldSnapshot = true, want false")
	}
}

func TestUpdateOrderCmd_Validate_AlwaysNil(t *testing.T) {
	c := UpdateOrderCmd{}
	if err := c.Validate(nil); err != nil {
		t.Errorf("Validate(nil) = %v, want nil", err)
	}
	if err := c.Validate(&Order{}); err != nil {
		t.Errorf("Validate(Order{}) = %v, want nil", err)
	}
}

func TestUpdateOrderCmd_EmitEvent_ReturnsNewState(t *testing.T) {
	newState := Order{ID: "x", Status: "Shipped", Total: 200}
	o := UpdateOrderCmd{NewState: newState}.EmitEvent(nil)
	if o != newState {
		t.Errorf("EmitEvent = %+v, want %+v", o, newState)
	}
}
