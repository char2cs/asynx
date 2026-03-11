package mocks

import "github.com/char2cs/asynx"

type Order struct {
	ID     string
	Total  float64
	Status string
}

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
		return asynx.ErrValidation
	}
	return nil
}

func (c CreateOrderCmd) EmitEvent(
	current *Order,
) Order {
	return Order{ID: c.ID, Total: c.Total, Status: "Pending"}
}

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
	if current == nil || current.Status == "Cancelled" {
		return asynx.ErrValidation
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
