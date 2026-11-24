package commands

import (
	"github.com/char2cs/asynx/example/domain"
	"github.com/char2cs/asynx/models"
)

// ShipOrderCmd marks a confirmed order as shipped. It is valid only for an
// existing order in the Confirmed status, and emits OrderShipped with the
// order in the Shipped status. It requests a snapshot after it commits.
type ShipOrderCmd struct {
	ID string
}

// AggregateID returns ID, the order the command targets.
func (c ShipOrderCmd) AggregateID() string { return c.ID }

// EventName returns the name of the event the command emits.
func (c ShipOrderCmd) EventName() string { return "OrderShipped" }

// ShouldSnapshot reports whether a snapshot of the order is written after the command commits.
func (c ShipOrderCmd) ShouldSnapshot() bool { return true }

// Validate returns models.ErrValidation when the order is not in the state the command requires, and nil otherwise.
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

// EmitEvent returns the order as it is after the command. It does not modify current, which Validate has already checked.
func (c ShipOrderCmd) EmitEvent(
	current *domain.Order,
) domain.Order {
	order := *current
	order.Status = domain.OrderShipped
	return order
}
