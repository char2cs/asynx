package mocks

// UpdateOrderCmd is a flexible test command that emits an arbitrary state and
// supports snapshots. Validate always passes.
type UpdateOrderCmd struct {
	ID       string
	NewState Order
	Snapshot bool
}

// AggregateID returns the ID of the aggregate the command targets.
func (c UpdateOrderCmd) AggregateID() string {
	return c.ID
}

// EventName returns the name of the event the command emits.
func (c UpdateOrderCmd) EventName() string {
	return "OrderUpdated"
}

// ShouldSnapshot reports whether a snapshot is written after the command.
func (c UpdateOrderCmd) ShouldSnapshot() bool {
	return c.Snapshot
}

// Validate checks the command against the current aggregate state.
func (c UpdateOrderCmd) Validate(
	_ *Order,
) error {
	return nil
}

// EmitEvent applies the command to the current aggregate and returns the resulting state.
func (c UpdateOrderCmd) EmitEvent(
	_ *Order,
) Order {
	return c.NewState
}
