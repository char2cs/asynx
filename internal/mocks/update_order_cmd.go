package mocks

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

func (c UpdateOrderCmd) Validate(
	_ *Order,
) error {
	return nil
}

func (c UpdateOrderCmd) EmitEvent(
	_ *Order,
) Order {
	return c.NewState
}
