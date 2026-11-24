package commands

import (
	"github.com/char2cs/asynx/example/domain"
	"github.com/char2cs/asynx/models/failure"
	"testing"
)

func TestCreateOrderCmd_AggregateID(
	t *testing.T,
) {
	cmd := CreateOrderCmd{ID: "ORD-001"}
	if got := cmd.AggregateID(); got != "ORD-001" {
		t.Errorf(
			"AggregateID() = %q, want %q",
			got,
			"ORD-001",
		)
	}
}

func TestCreateOrderCmd_EventName(
	t *testing.T,
) {
	cmd := CreateOrderCmd{}
	if got := cmd.EventName(); got != "OrderCreated" {
		t.Errorf(
			"EventName() = %q, want %q",
			got,
			"OrderCreated",
		)
	}
}

func TestCreateOrderCmd_ShouldSnapshot(
	t *testing.T,
) {
	cmd := CreateOrderCmd{}
	if got := cmd.ShouldSnapshot(); got != false {
		t.Errorf(
			"ShouldSnapshot() = %v, want false",
			got,
		)
	}
}

func TestCreateOrderCmd_Validate_ValidCommand(
	t *testing.T,
) {
	cmd := CreateOrderCmd{
		ID:     "ORD-001",
		Total:  99.99,
		Items:  2,
		ShipTo: "123 Main St",
	}
	err := cmd.Validate(nil)
	if err != nil {
		t.Errorf(
			"Validate(nil) = %v, want nil",
			err,
		)
	}
}

func TestCreateOrderCmd_Validate_InvalidTotal(
	t *testing.T,
) {
	cmd := CreateOrderCmd{
		ID:     "ORD-001",
		Total:  0,
		Items:  2,
		ShipTo: "123 Main St",
	}
	err := cmd.Validate(nil)
	if err != failure.ErrValidation {
		t.Errorf(
			"Validate(nil) with Total=0 = %v, want %v",
			err,
			failure.ErrValidation,
		)
	}
}

func TestCreateOrderCmd_Validate_NegativeTotal(
	t *testing.T,
) {
	cmd := CreateOrderCmd{
		ID:     "ORD-001",
		Total:  -10.0,
		Items:  2,
		ShipTo: "123 Main St",
	}
	err := cmd.Validate(nil)
	if err != failure.ErrValidation {
		t.Errorf(
			"Validate(nil) with negative Total = %v, want %v",
			err,
			failure.ErrValidation,
		)
	}
}

func TestCreateOrderCmd_Validate_InvalidItems(
	t *testing.T,
) {
	cmd := CreateOrderCmd{
		ID:     "ORD-001",
		Total:  99.99,
		Items:  0,
		ShipTo: "123 Main St",
	}
	err := cmd.Validate(nil)
	if err != failure.ErrValidation {
		t.Errorf(
			"Validate(nil) with Items=0 = %v, want %v",
			err,
			failure.ErrValidation,
		)
	}
}

func TestCreateOrderCmd_Validate_NegativeItems(
	t *testing.T,
) {
	cmd := CreateOrderCmd{
		ID:     "ORD-001",
		Total:  99.99,
		Items:  -5,
		ShipTo: "123 Main St",
	}
	err := cmd.Validate(nil)
	if err != failure.ErrValidation {
		t.Errorf(
			"Validate(nil) with negative Items = %v, want %v",
			err,
			failure.ErrValidation,
		)
	}
}

func TestCreateOrderCmd_Validate_AlreadyExists(
	t *testing.T,
) {
	cmd := CreateOrderCmd{
		ID:     "ORD-001",
		Total:  99.99,
		Items:  2,
		ShipTo: "123 Main St",
	}
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
			"Validate(current) when already exists = %v, want %v",
			err,
			failure.ErrValidation,
		)
	}
}

func TestCreateOrderCmd_EmitEvent(
	t *testing.T,
) {
	cmd := CreateOrderCmd{
		ID:     "ORD-001",
		Total:  99.99,
		Items:  2,
		ShipTo: "123 Main St",
	}
	event := cmd.EmitEvent(nil)
	if event.ID != "ORD-001" {
		t.Errorf(
			"EmitEvent().ID = %q, want %q",
			event.ID,
			"ORD-001",
		)
	}

	if event.Status != domain.OrderPending {
		t.Errorf(
			"EmitEvent().Status = %q, want %q",
			event.Status,
			domain.OrderPending,
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

func TestOrderStatusConstants(
	t *testing.T,
) {
	if domain.OrderPending != "Pending" {
		t.Errorf(
			"OrderPending = %q, want %q",
			domain.OrderPending,
			"Pending",
		)
	}

	if domain.OrderConfirmed != "Confirmed" {
		t.Errorf(
			"OrderConfirmed = %q, want %q",
			domain.OrderConfirmed,
			"Confirmed",
		)
	}

	if domain.OrderShipped != "Shipped" {
		t.Errorf(
			"OrderShipped = %q, want %q",
			domain.OrderShipped,
			"Shipped",
		)
	}
}

func TestOrderStruct(
	t *testing.T,
) {
	order := domain.Order{
		ID:     "ORD-001",
		Status: domain.OrderPending,
		Total:  99.99,
		Items:  2,
		ShipTo: "123 Main St",
	}
	if order.ID != "ORD-001" {
		t.Errorf(
			"ID = %q, want %q",
			order.ID,
			"ORD-001",
		)
	}

	if order.Status != domain.OrderPending {
		t.Errorf(
			"Status = %q, want %q",
			order.Status,
			domain.OrderPending,
		)
	}

	if order.Total != 99.99 {
		t.Errorf(
			"Total = %v, want %v",
			order.Total,
			99.99,
		)
	}

	if order.Items != 2 {
		t.Errorf(
			"Items = %d, want %d",
			order.Items,
			2,
		)
	}

	if order.ShipTo != "123 Main St" {
		t.Errorf(
			"ShipTo = %q, want %q",
			order.ShipTo,
			"123 Main St",
		)
	}
}
