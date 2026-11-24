package commands

import (
	"github.com/char2cs/asynx/v2/example/domain"
	"github.com/char2cs/asynx/v2/models/failure"
	"testing"
)

func TestShipOrderCmd_AggregateID(
	t *testing.T,
) {
	cmd := ShipOrderCmd{ID: "ORD-001"}
	if got := cmd.AggregateID(); got != "ORD-001" {
		t.Errorf(
			"AggregateID() = %q, want %q",
			got,
			"ORD-001",
		)
	}
}

func TestShipOrderCmd_EventName(
	t *testing.T,
) {
	cmd := ShipOrderCmd{}
	if got := cmd.EventName(); got != "OrderShipped" {
		t.Errorf(
			"EventName() = %q, want %q",
			got,
			"OrderShipped",
		)
	}
}

func TestShipOrderCmd_ShouldSnapshot(
	t *testing.T,
) {
	cmd := ShipOrderCmd{}
	if got := cmd.ShouldSnapshot(); got != true {
		t.Errorf(
			"ShouldSnapshot() = %v, want true",
			got,
		)
	}
}

func TestShipOrderCmd_Validate_NoCurrentOrder(
	t *testing.T,
) {
	cmd := ShipOrderCmd{ID: "ORD-001"}
	err := cmd.Validate(nil)
	if err != failure.ErrValidation {
		t.Errorf(
			"Validate(nil) = %v, want %v",
			err,
			failure.ErrValidation,
		)
	}
}

func TestShipOrderCmd_Validate_NotConfirmed(
	t *testing.T,
) {
	cmd := ShipOrderCmd{ID: "ORD-001"}
	current := &domain.Order{
		ID:     "ORD-001",
		Status: domain.OrderPending,
		Total:  99.99,
		Items:  2,
		ShipTo: "123 Main St",
	}
	err := cmd.Validate(current)
	if err != failure.ErrValidation {
		t.Errorf(
			"Validate(Pending) = %v, want %v",
			err,
			failure.ErrValidation,
		)
	}
}

func TestShipOrderCmd_Validate_AlreadyShipped(
	t *testing.T,
) {
	cmd := ShipOrderCmd{ID: "ORD-001"}
	current := &domain.Order{
		ID:     "ORD-001",
		Status: domain.OrderShipped,
		Total:  99.99,
		Items:  2,
		ShipTo: "123 Main St",
	}
	err := cmd.Validate(current)
	if err != failure.ErrValidation {
		t.Errorf(
			"Validate(Shipped) = %v, want %v",
			err,
			failure.ErrValidation,
		)
	}
}

func TestShipOrderCmd_Validate_ValidCommand(
	t *testing.T,
) {
	cmd := ShipOrderCmd{ID: "ORD-001"}
	current := &domain.Order{
		ID:     "ORD-001",
		Status: domain.OrderConfirmed,
		Total:  99.99,
		Items:  2,
		ShipTo: "123 Main St",
	}
	err := cmd.Validate(current)
	if err != nil {
		t.Errorf(
			"Validate(Confirmed) = %v, want nil",
			err,
		)
	}
}

func TestShipOrderCmd_EmitEvent(
	t *testing.T,
) {
	cmd := ShipOrderCmd{ID: "ORD-001"}
	current := &domain.Order{
		ID:     "ORD-001",
		Status: domain.OrderConfirmed,
		Total:  99.99,
		Items:  2,
		ShipTo: "123 Main St",
	}
	event := cmd.EmitEvent(current)
	if event.ID != "ORD-001" {
		t.Errorf(
			"EmitEvent().ID = %q, want %q",
			event.ID,
			"ORD-001",
		)
	}

	if event.Status != domain.OrderShipped {
		t.Errorf(
			"EmitEvent().Status = %q, want %q",
			event.Status,
			domain.OrderShipped,
		)
	}

	if event.Total != 99.99 {
		t.Errorf(
			"EmitEvent().Total = %v, want %v",
			event.Total,
			99.99,
		)
	}

	if event.Items != 2 {
		t.Errorf(
			"EmitEvent().Items = %d, want %d",
			event.Items,
			2,
		)
	}

	if event.ShipTo != "123 Main St" {
		t.Errorf(
			"EmitEvent().ShipTo = %q, want %q",
			event.ShipTo,
			"123 Main St",
		)
	}
}
