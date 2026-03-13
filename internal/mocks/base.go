package mocks

import "github.com/char2cs/asynx/models"

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
		return models.ErrValidation
	}
	return nil
}

func (c CreateOrderCmd) EmitEvent(
	current *Order,
) Order {
	return Order{ID: c.ID, Total: c.Total, Status: "Pending"}
}

// UpdateOrderCmd is a flexible test command that emits an arbitrary state and
// supports snapshots. Validate always passes.
type UpdateOrderCmd struct {
	ID       string
	NewState Order
	Snapshot bool
}

func (c UpdateOrderCmd) AggregateID() string {
	return c.ID
}

func (c UpdateOrderCmd) EventName() string {
	return "OrderUpdated"
}

func (c UpdateOrderCmd) ShouldSnapshot() bool {
	return c.Snapshot
}

func (c UpdateOrderCmd) Validate(_ *Order) error {
	return nil
}

func (c UpdateOrderCmd) EmitEvent(_ *Order) Order {
	return c.NewState
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
