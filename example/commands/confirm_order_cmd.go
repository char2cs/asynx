package commands

import (
	"github.com/char2cs/asynx/example/domain"
	"github.com/char2cs/asynx/models"
)

// ConfirmOrderCmd confirms a pending order.
type ConfirmOrderCmd struct {
	ID string
}

// AggregateID returns the ID of the aggregate the command targets.
func (c ConfirmOrderCmd) AggregateID() string { return c.ID }

// EventName returns the name of the event the command emits.
func (c ConfirmOrderCmd) EventName() string { return "OrderConfirmed" }

// ShouldSnapshot reports whether a snapshot is written after the command.
func (c ConfirmOrderCmd) ShouldSnapshot() bool { return false }

// Validate checks the command against the current aggregate state.
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

// EmitEvent applies the command to the current aggregate and returns the resulting state.
func (c ConfirmOrderCmd) EmitEvent(
	current *domain.Order,
) domain.Order {
	order := *current
	order.Status = domain.OrderConfirmed
	return order
}
