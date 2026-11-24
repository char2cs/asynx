package mocks

import (
	"github.com/char2cs/asynx/models/failure"
)

// CancelOrderCmd is the test command that cancels the Order with the given ID.
// It is valid only for an Order that exists and is not already cancelled, emits
// OrderCancelled and never asks for a snapshot.
type CancelOrderCmd struct {
	ID string
}

// AggregateID returns the ID of the aggregate the command targets.
func (c CancelOrderCmd) AggregateID() string {
	return c.ID
}

// EventName returns the name of the event the command emits.
func (c CancelOrderCmd) EventName() string {
	return "OrderCancelled"
}

// ShouldSnapshot reports whether a snapshot is written after the command.
func (c CancelOrderCmd) ShouldSnapshot() bool {
	return false
}

// Validate checks the command against the current aggregate state.
func (c CancelOrderCmd) Validate(
	current *Order,
) error {
	if current == nil ||
		current.Status == "Cancelled" {
		return failure.ErrValidation
	}

	return nil
}

// EmitEvent applies the command to the current aggregate and returns the resulting state.
func (c CancelOrderCmd) EmitEvent(
	current *Order,
) Order {
	o := *current
	o.Status = "Cancelled"
	return o
}
