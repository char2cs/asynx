// Package commands holds internal Asynx commands that are dispatched by the
// library itself rather than by application code.
package commands

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
func NewForgetCommand[T any](aggregateID string) *ForgetCommand[T] {
	return &ForgetCommand[T]{id: aggregateID}
}

func (c *ForgetCommand[T]) AggregateID() string { return c.id }
func (c *ForgetCommand[T]) EventName() string    { return "asynx.aggregate.forget" }
func (c *ForgetCommand[T]) ShouldSnapshot() bool { return false }

func (c *ForgetCommand[T]) Validate(current *T) error {
	if current == nil {
		return fmt.Errorf("%w: aggregate %s not found", models.ErrValidation, c.id)
	}
	c.last = current
	return nil
}

func (c *ForgetCommand[T]) EmitEvent(_ *T) T {
	if c.last == nil {
		var zero T
		return zero
	}
	return *c.last
}
