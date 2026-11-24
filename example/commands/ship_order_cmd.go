package commands

import (
	"github.com/char2cs/asynx/example/domain"
	"github.com/char2cs/asynx/models"
)

// ShipOrderCmd marks an order as shipped.
type ShipOrderCmd struct {
	ID string
}

// AggregateID returns the ID of the aggregate the command targets.
func (c ShipOrderCmd) AggregateID() string { return c.ID }

// EventName returns the name of the event the command emits.
func (c ShipOrderCmd) EventName() string { return "OrderShipped" }

// ShouldSnapshot reports whether a snapshot is written after the command.
func (c ShipOrderCmd) ShouldSnapshot() bool { return true }

// Validate checks the command against the current aggregate state.
func (c ShipOrderCmd) Validate(
	current *domain.Order,
) error {
	if current == nil {
		return models.ErrValidation
	}

	if current.Status != domain.OrderConfirmed {
		return models.ErrValidation
	}

	return nil
}

// EmitEvent applies the command to the current aggregate and returns the resulting state.
func (c ShipOrderCmd) EmitEvent(
	current *domain.Order,
) domain.Order {
	order := *current
	order.Status = domain.OrderShipped
	return order
}
