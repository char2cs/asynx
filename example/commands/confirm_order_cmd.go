package commands

import (
	"github.com/char2cs/asynx/example/domain"
	"github.com/char2cs/asynx/models"
)

// ConfirmOrderCmd confirms a pending order. It is valid only for an existing
// order in the Pending status, and emits OrderConfirmed with the order in the
// Confirmed status. It writes no snapshot.
type ConfirmOrderCmd struct {
	ID string
}

// AggregateID returns ID, the order the command targets.
func (c ConfirmOrderCmd) AggregateID() string { return c.ID }

// EventName returns the name of the event the command emits.
func (c ConfirmOrderCmd) EventName() string { return "OrderConfirmed" }

// ShouldSnapshot reports whether a snapshot of the order is written after the command commits.
func (c ConfirmOrderCmd) ShouldSnapshot() bool { return false }

// Validate returns models.ErrValidation when the order is not in the state the command requires, and nil otherwise.
func (c ConfirmOrderCmd) Validate(
	current *domain.Order,
) error {
	if current == nil {
		return models.ErrValidation
	}

	if current.Status != domain.OrderPending {
		return models.ErrValidation
	}

	return nil
}

// EmitEvent returns the order as it is after the command. It does not modify current, which Validate has already checked.
func (c ConfirmOrderCmd) EmitEvent(
	current *domain.Order,
) domain.Order {
	order := *current
	order.Status = domain.OrderConfirmed
	return order
}
