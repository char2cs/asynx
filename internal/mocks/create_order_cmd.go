package mocks

import (
	"github.com/char2cs/asynx/models"
)

// CreateOrderCmd creates an Order.
type CreateOrderCmd struct {
	ID    string
	Total float64
}

// AggregateID returns the ID of the aggregate the command targets.
func (c CreateOrderCmd) AggregateID() string {
	return c.ID
}

// EventName returns the name of the event the command emits.
func (c CreateOrderCmd) EventName() string {
	return "OrderCreated"
}

// ShouldSnapshot reports whether a snapshot is written after the command.
func (c CreateOrderCmd) ShouldSnapshot() bool {
	return false
}

// Validate checks the command against the current aggregate state.
func (c CreateOrderCmd) Validate(
	current *Order,
) error {
	if c.Total <= 0 {
		return models.ErrValidation
	}

	return nil
}

// EmitEvent applies the command to the current aggregate and returns the resulting state.
func (c CreateOrderCmd) EmitEvent(
	current *Order,
) Order {
	return Order{ID: c.ID, Total: c.Total, Status: "Pending"}
}
