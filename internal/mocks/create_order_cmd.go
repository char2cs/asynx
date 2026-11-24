package mocks

import (
	"github.com/char2cs/asynx/models"
)

type CreateOrderCmd struct {
	ID    string
	Total float64
}

func (c CreateOrderCmd) AggregateID() string {
	return c.ID
}

func (c CreateOrderCmd) EventName() string {
	return "OrderCreated"
}

func (c CreateOrderCmd) ShouldSnapshot() bool {
	return false
}

func (c CreateOrderCmd) Validate(
	current *Order,
) error {
	if c.Total <= 0 {
		return models.ErrValidation
	}

	return nil
}

func (c CreateOrderCmd) EmitEvent(
	current *Order,
) Order {
	return Order{ID: c.ID, Total: c.Total, Status: "Pending"}
}
