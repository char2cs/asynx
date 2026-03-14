package commands

import (
	"testing"

	"github.com/char2cs/asynx/example/domain"
	"github.com/char2cs/asynx/models"
)

// ============================================================================
// CreateOrderCmd Tests
// ============================================================================

func TestCreateOrderCmd_AggregateID(t *testing.T) {
	cmd := CreateOrderCmd{ID: "ORD-001"}
	if got := cmd.AggregateID(); got != "ORD-001" {
		t.Errorf("AggregateID() = %q, want %q", got, "ORD-001")
	}
}

func TestCreateOrderCmd_EventName(t *testing.T) {
	cmd := CreateOrderCmd{}
	if got := cmd.EventName(); got != "OrderCreated" {
		t.Errorf("EventName() = %q, want %q", got, "OrderCreated")
	}
}

func TestCreateOrderCmd_ShouldSnapshot(t *testing.T) {
	cmd := CreateOrderCmd{}
	if got := cmd.ShouldSnapshot(); got != false {
		t.Errorf("ShouldSnapshot() = %v, want false", got)
	}
}

func TestCreateOrderCmd_Validate_ValidCommand(t *testing.T) {
	cmd := CreateOrderCmd{
		ID:     "ORD-001",
		Total:  99.99,
		Items:  2,
		ShipTo: "123 Main St",
	}
	err := cmd.Validate(nil)
	if err != nil {
		t.Errorf("Validate(nil) = %v, want nil", err)
	}
}

func TestCreateOrderCmd_Validate_InvalidTotal(t *testing.T) {
	cmd := CreateOrderCmd{
		ID:     "ORD-001",
		Total:  0,
		Items:  2,
		ShipTo: "123 Main St",
	}
	err := cmd.Validate(nil)
	if err != models.ErrValidation {
		t.Errorf("Validate(nil) with Total=0 = %v, want %v", err, models.ErrValidation)
	}
}

func TestCreateOrderCmd_Validate_NegativeTotal(t *testing.T) {
	cmd := CreateOrderCmd{
		ID:     "ORD-001",
		Total:  -10.0,
		Items:  2,
		ShipTo: "123 Main St",
	}
	err := cmd.Validate(nil)
	if err != models.ErrValidation {
		t.Errorf("Validate(nil) with negative Total = %v, want %v", err, models.ErrValidation)
	}
}

func TestCreateOrderCmd_Validate_InvalidItems(t *testing.T) {
	cmd := CreateOrderCmd{
		ID:     "ORD-001",
		Total:  99.99,
		Items:  0,
		ShipTo: "123 Main St",
	}
	err := cmd.Validate(nil)
	if err != models.ErrValidation {
		t.Errorf("Validate(nil) with Items=0 = %v, want %v", err, models.ErrValidation)
	}
}

func TestCreateOrderCmd_Validate_NegativeItems(t *testing.T) {
	cmd := CreateOrderCmd{
		ID:     "ORD-001",
		Total:  99.99,
		Items:  -5,
		ShipTo: "123 Main St",
	}
	err := cmd.Validate(nil)
	if err != models.ErrValidation {
		t.Errorf("Validate(nil) with negative Items = %v, want %v", err, models.ErrValidation)
	}
}

func TestCreateOrderCmd_Validate_AlreadyExists(t *testing.T) {
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
	if err != models.ErrValidation {
		t.Errorf("Validate(current) when already exists = %v, want %v", err, models.ErrValidation)
	}
}

func TestCreateOrderCmd_EmitEvent(t *testing.T) {
	cmd := CreateOrderCmd{
		ID:     "ORD-001",
		Total:  99.99,
		Items:  2,
		ShipTo: "123 Main St",
	}
	event := cmd.EmitEvent(nil)

	if event.ID != "ORD-001" {
		t.Errorf("EmitEvent().ID = %q, want %q", event.ID, "ORD-001")
	}
	if event.Status != domain.OrderPending {
		t.Errorf("EmitEvent().Status = %q, want %q", event.Status, domain.OrderPending)
	}
	if event.Total != 99.99 {
		t.Errorf("EmitEvent().Total = %v, want %v", event.Total, 99.99)
	}
	if event.Items != 2 {
		t.Errorf("EmitEvent().Items = %d, want %d", event.Items, 2)
	}
	if event.ShipTo != "123 Main St" {
		t.Errorf("EmitEvent().ShipTo = %q, want %q", event.ShipTo, "123 Main St")
	}
}

// ============================================================================
// ConfirmOrderCmd Tests
// ============================================================================

func TestConfirmOrderCmd_AggregateID(t *testing.T) {
	cmd := ConfirmOrderCmd{ID: "ORD-001"}
	if got := cmd.AggregateID(); got != "ORD-001" {
		t.Errorf("AggregateID() = %q, want %q", got, "ORD-001")
	}
}

func TestConfirmOrderCmd_EventName(t *testing.T) {
	cmd := ConfirmOrderCmd{}
	if got := cmd.EventName(); got != "OrderConfirmed" {
		t.Errorf("EventName() = %q, want %q", got, "OrderConfirmed")
	}
}

func TestConfirmOrderCmd_ShouldSnapshot(t *testing.T) {
	cmd := ConfirmOrderCmd{}
	if got := cmd.ShouldSnapshot(); got != false {
		t.Errorf("ShouldSnapshot() = %v, want false", got)
	}
}

func TestConfirmOrderCmd_Validate_NoCurrentOrder(t *testing.T) {
	cmd := ConfirmOrderCmd{ID: "ORD-001"}
	err := cmd.Validate(nil)
	if err != models.ErrValidation {
		t.Errorf("Validate(nil) = %v, want %v", err, models.ErrValidation)
	}
}

func TestConfirmOrderCmd_Validate_NotPending(t *testing.T) {
	cmd := ConfirmOrderCmd{ID: "ORD-001"}
	current := &domain.Order{
		ID:     "ORD-001",
		Status: domain.OrderConfirmed,
		Total:  99.99,
		Items:  2,
		ShipTo: "123 Main St",
	}
	err := cmd.Validate(current)
	if err != models.ErrValidation {
		t.Errorf("Validate(Confirmed) = %v, want %v", err, models.ErrValidation)
	}
}

func TestConfirmOrderCmd_Validate_Shipped(t *testing.T) {
	cmd := ConfirmOrderCmd{ID: "ORD-001"}
	current := &domain.Order{
		ID:     "ORD-001",
		Status: domain.OrderShipped,
		Total:  99.99,
		Items:  2,
		ShipTo: "123 Main St",
	}
	err := cmd.Validate(current)
	if err != models.ErrValidation {
		t.Errorf("Validate(Shipped) = %v, want %v", err, models.ErrValidation)
	}
}

func TestConfirmOrderCmd_Validate_ValidCommand(t *testing.T) {
	cmd := ConfirmOrderCmd{ID: "ORD-001"}
	current := &domain.Order{
		ID:     "ORD-001",
		Status: domain.OrderPending,
		Total:  99.99,
		Items:  2,
		ShipTo: "123 Main St",
	}
	err := cmd.Validate(current)
	if err != nil {
		t.Errorf("Validate(Pending) = %v, want nil", err)
	}
}

func TestConfirmOrderCmd_EmitEvent(t *testing.T) {
	cmd := ConfirmOrderCmd{ID: "ORD-001"}
	current := &domain.Order{
		ID:     "ORD-001",
		Status: domain.OrderPending,
		Total:  99.99,
		Items:  2,
		ShipTo: "123 Main St",
	}
	event := cmd.EmitEvent(current)

	if event.ID != "ORD-001" {
		t.Errorf("EmitEvent().ID = %q, want %q", event.ID, "ORD-001")
	}
	if event.Status != domain.OrderConfirmed {
		t.Errorf("EmitEvent().Status = %q, want %q", event.Status, domain.OrderConfirmed)
	}
	if event.Total != 99.99 {
		t.Errorf("EmitEvent().Total = %v, want %v", event.Total, 99.99)
	}
	if event.Items != 2 {
		t.Errorf("EmitEvent().Items = %d, want %d", event.Items, 2)
	}
	if event.ShipTo != "123 Main St" {
		t.Errorf("EmitEvent().ShipTo = %q, want %q", event.ShipTo, "123 Main St")
	}
}

// ============================================================================
// ShipOrderCmd Tests
// ============================================================================

func TestShipOrderCmd_AggregateID(t *testing.T) {
	cmd := ShipOrderCmd{ID: "ORD-001"}
	if got := cmd.AggregateID(); got != "ORD-001" {
		t.Errorf("AggregateID() = %q, want %q", got, "ORD-001")
	}
}

func TestShipOrderCmd_EventName(t *testing.T) {
	cmd := ShipOrderCmd{}
	if got := cmd.EventName(); got != "OrderShipped" {
		t.Errorf("EventName() = %q, want %q", got, "OrderShipped")
	}
}

func TestShipOrderCmd_ShouldSnapshot(t *testing.T) {
	cmd := ShipOrderCmd{}
	if got := cmd.ShouldSnapshot(); got != true {
		t.Errorf("ShouldSnapshot() = %v, want true", got)
	}
}

func TestShipOrderCmd_Validate_NoCurrentOrder(t *testing.T) {
	cmd := ShipOrderCmd{ID: "ORD-001"}
	err := cmd.Validate(nil)
	if err != models.ErrValidation {
		t.Errorf("Validate(nil) = %v, want %v", err, models.ErrValidation)
	}
}

func TestShipOrderCmd_Validate_NotConfirmed(t *testing.T) {
	cmd := ShipOrderCmd{ID: "ORD-001"}
	current := &domain.Order{
		ID:     "ORD-001",
		Status: domain.OrderPending,
		Total:  99.99,
		Items:  2,
		ShipTo: "123 Main St",
	}
	err := cmd.Validate(current)
	if err != models.ErrValidation {
		t.Errorf("Validate(Pending) = %v, want %v", err, models.ErrValidation)
	}
}

func TestShipOrderCmd_Validate_AlreadyShipped(t *testing.T) {
	cmd := ShipOrderCmd{ID: "ORD-001"}
	current := &domain.Order{
		ID:     "ORD-001",
		Status: domain.OrderShipped,
		Total:  99.99,
		Items:  2,
		ShipTo: "123 Main St",
	}
	err := cmd.Validate(current)
	if err != models.ErrValidation {
		t.Errorf("Validate(Shipped) = %v, want %v", err, models.ErrValidation)
	}
}

func TestShipOrderCmd_Validate_ValidCommand(t *testing.T) {
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
		t.Errorf("Validate(Confirmed) = %v, want nil", err)
	}
}

func TestShipOrderCmd_EmitEvent(t *testing.T) {
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
		t.Errorf("EmitEvent().ID = %q, want %q", event.ID, "ORD-001")
	}
	if event.Status != domain.OrderShipped {
		t.Errorf("EmitEvent().Status = %q, want %q", event.Status, domain.OrderShipped)
	}
	if event.Total != 99.99 {
		t.Errorf("EmitEvent().Total = %v, want %v", event.Total, 99.99)
	}
	if event.Items != 2 {
		t.Errorf("EmitEvent().Items = %d, want %d", event.Items, 2)
	}
	if event.ShipTo != "123 Main St" {
		t.Errorf("EmitEvent().ShipTo = %q, want %q", event.ShipTo, "123 Main St")
	}
}
