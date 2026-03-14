package commands

import (
	"github.com/char2cs/asynx/example/domain"
	"github.com/char2cs/asynx/models"
)

// CreateOrderCmd creates a new order.
type CreateOrderCmd struct {
	ID     string
	Total  float64
	Items  int
	ShipTo string
}

func (c CreateOrderCmd) AggregateID() string { return c.ID }
func (c CreateOrderCmd) EventName() string   { return "OrderCreated" }
func (c CreateOrderCmd) ShouldSnapshot() bool { return false }

func (c CreateOrderCmd) Validate(current *domain.Order) error {
	if c.Total <= 0 {
		return models.ErrValidation
	}
	if c.Items <= 0 {
		return models.ErrValidation
	}
	if current != nil {
		return models.ErrValidation // Already exists
	}
	return nil
}

func (c CreateOrderCmd) EmitEvent(current *domain.Order) domain.Order {
	return domain.Order{
		ID:     c.ID,
		Status: domain.OrderPending,
		Total:  c.Total,
		Items:  c.Items,
		ShipTo: c.ShipTo,
	}
}

// ConfirmOrderCmd confirms a pending order.
type ConfirmOrderCmd struct {
	ID string
}

func (c ConfirmOrderCmd) AggregateID() string { return c.ID }
func (c ConfirmOrderCmd) EventName() string   { return "OrderConfirmed" }
func (c ConfirmOrderCmd) ShouldSnapshot() bool { return false }

func (c ConfirmOrderCmd) Validate(current *domain.Order) error {
	if current == nil {
		return models.ErrValidation
	}
	if current.Status != domain.OrderPending {
		return models.ErrValidation
	}
	return nil
}

func (c ConfirmOrderCmd) EmitEvent(current *domain.Order) domain.Order {
	order := *current
	order.Status = domain.OrderConfirmed
	return order
}

// ShipOrderCmd marks an order as shipped.
type ShipOrderCmd struct {
	ID string
}

func (c ShipOrderCmd) AggregateID() string { return c.ID }
func (c ShipOrderCmd) EventName() string   { return "OrderShipped" }
func (c ShipOrderCmd) ShouldSnapshot() bool { return true }

func (c ShipOrderCmd) Validate(current *domain.Order) error {
	if current == nil {
		return models.ErrValidation
	}
	if current.Status != domain.OrderConfirmed {
		return models.ErrValidation
	}
	return nil
}

func (c ShipOrderCmd) EmitEvent(current *domain.Order) domain.Order {
	order := *current
	order.Status = domain.OrderShipped
	return order
}
