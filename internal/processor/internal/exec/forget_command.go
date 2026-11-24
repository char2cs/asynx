package exec

import (
	"fmt"

	"github.com/char2cs/asynx/models"
)

// ForgetCommand implements models.Command[T] for the Forget operation.
// Validate captures the aggregate's last state so EmitEvent can embed it
// in the tombstone event. EmitEvent is safe to call only after Validate has set last.
type ForgetCommand[T any] struct {
	id   string
	last *T
}

// NewForgetCommand constructs a ForgetCommand for the given aggregate.
func NewForgetCommand[T any](
	aggregateID string,
) *ForgetCommand[T] {
	return &ForgetCommand[T]{id: aggregateID}
}

// AggregateID returns the aggregate the tombstone is written to and later
// erases. The executor keys the aggregate's command lock and its in-progress
// mark on it, so a Forget is serialised with every command for that aggregate.
func (c *ForgetCommand[T]) AggregateID() string { return c.id }

// EventName returns the name of the tombstone event.
func (c *ForgetCommand[T]) EventName() string { return models.ForgetEventName }

// ShouldSnapshot reports false: a tombstone is never snapshotted.
func (c *ForgetCommand[T]) ShouldSnapshot() bool { return false }

// Validate captures the current state of the aggregate for the tombstone and
// fails when the aggregate does not exist.
func (c *ForgetCommand[T]) Validate(
	current *T,
) error {
	if current == nil {
		return fmt.Errorf(
			"%w: aggregate %s not found",
			models.ErrValidation,
			c.id,
		)
	}

	c.last = current
	return nil
}

// EmitEvent returns the state captured by Validate as the tombstone payload.
func (c *ForgetCommand[T]) EmitEvent(
	_ *T,
) (state T) {
	if c.last == nil {
		return state
	}

	return *c.last
}
