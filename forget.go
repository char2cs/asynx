package asynx

import (
	"context"
	"fmt"

	"github.com/char2cs/asynx/models"
)

// forgetCommand implements models.Command[T] for the Forget operation.
// Validate captures the aggregate's last state so EmitEvent can embed it
// in the tombstone event. EmitEvent is safe to call only after Validate has set c.last.
type forgetCommand[T any] struct {
	aggregateID string
	last        *T
}

func (c *forgetCommand[T]) AggregateID() string { return c.aggregateID }
func (c *forgetCommand[T]) EventName() string    { return "asynx.aggregate.forget" }
func (c *forgetCommand[T]) ShouldSnapshot() bool { return false }

func (c *forgetCommand[T]) Validate(current *T) error {
	if current == nil {
		return fmt.Errorf("%w: aggregate %s not found", models.ErrValidation, c.aggregateID)
	}
	c.last = current
	return nil
}

func (c *forgetCommand[T]) EmitEvent(_ *T) T {
	if c.last == nil {
		var zero T
		return zero
	}
	return *c.last
}

func (i *asynxImpl[T]) Forget(ctx context.Context, aggregateID string) error {
	_, err := i.proc.SendWait(ctx, &forgetCommand[T]{aggregateID: aggregateID})
	if err != nil {
		return err
	}
	deleteCtx := context.WithoutCancel(ctx)
	if err := i.es.Delete(deleteCtx, aggregateID); err != nil {
		return fmt.Errorf("%w: %w", models.ErrForgetFailed, err)
	}
	return nil
}

func (i *asynxImpl[T]) OnForget(fn models.ForgetHandler[T]) (string, error) {
	return i.bus.Subscribe("asynx.aggregate.forget", models.ProjectionHandler[T](fn))
}
