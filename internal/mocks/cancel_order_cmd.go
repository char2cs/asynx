package mocks

import (
	"github.com/char2cs/asynx/models"
)

type CancelOrderCmd struct {
	ID string
}

func (c CancelOrderCmd) AggregateID() string {
	return c.ID
}

func (c CancelOrderCmd) EventName() string {
	return "OrderCancelled"
}

func (c CancelOrderCmd) ShouldSnapshot() bool {
	return false
}

func (c CancelOrderCmd) Validate(
	current *Order,
) error {
	if current == nil ||
		current.Status == "Cancelled" {
		return models.ErrValidation
	}

	return nil
}

func (c CancelOrderCmd) EmitEvent(
	current *Order,
) Order {
	o := *current
	o.Status = "Cancelled"
	return o
}
