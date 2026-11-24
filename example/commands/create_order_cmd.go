package commands

import (
	"github.com/char2cs/asynx/v2/example/domain"
	"github.com/char2cs/asynx/v2/models/failure"
)

// CreateOrderCmd creates a new order. It is the command that starts an order's
// stream: it is valid only for a positive Total and a positive Items count, and
// only while no order with ID exists. It emits OrderCreated with the order in
// the Pending status and writes no snapshot.
type CreateOrderCmd struct {
	ID     string
	Total  float64
	Items  int
	ShipTo string
}

// AggregateID returns ID, the order the command targets.
func (c CreateOrderCmd) AggregateID() string { return c.ID }

// EventName returns the name of the event the command emits.
func (c CreateOrderCmd) EventName() string { return "OrderCreated" }

// ShouldSnapshot reports whether a snapshot of the order is written after the command commits.
func (c CreateOrderCmd) ShouldSnapshot() bool { return false }

// Validate returns failure.ErrValidation when Total or Items is not positive or
// when the order already exists, and nil otherwise.
func (c CreateOrderCmd) Validate(
	current *domain.Order,
) error {
	if c.Total <= 0 {
		return failure.ErrValidation
	}

	if c.Items <= 0 {
		return failure.ErrValidation
	}

	if current != nil {
		return failure.ErrValidation
	}

	return nil
}

// EmitEvent returns the order as it is after the command. It does not modify current, which Validate has already checked.
func (c CreateOrderCmd) EmitEvent(
	current *domain.Order,
) domain.Order {
	return domain.Order{
		ID:     c.ID,
		Status: domain.OrderPending,
		Total:  c.Total,
		Items:  c.Items,
		ShipTo: c.ShipTo,
	}
}
