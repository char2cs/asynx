package processor

import (
	"context"

	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/processor/internal/core"
	"github.com/char2cs/asynx/internal/processor/models"
	asynxmd "github.com/char2cs/asynx/models"
)

// Processor routes commands to per-aggregate shards, runs them through the
// write pipeline and delivers the resulting events. Construct it with New.
type Processor[T any] interface {
	// Send runs cmd and returns its event once it is committed.
	Send(
		ctx context.Context,
		cmd asynxmd.Command[T],
	) (asynxmd.Event[T], error)
	// SendWait is Send that also waits for the event's handlers to run once.
	SendWait(
		ctx context.Context,
		cmd asynxmd.Command[T],
	) (asynxmd.Event[T], error)
	// Forget erases the aggregate.
	Forget(
		ctx context.Context,
		aggregateID string,
	) error
	// Shutdown stops accepting work, drains it and closes the dispatcher and bus.
	Shutdown(
		ctx context.Context,
	) error
	// WaitPublish blocks until all dispatched events have been delivered.
	WaitPublish()
	// Paused returns the aggregates whose deliveries are parked.
	Paused() []string
	// Inspect reports the delivery status of an aggregate.
	Inspect(
		aggregateID string,
	) asynxmd.DeliveryStatus
	// Resume lifts the park of an aggregate and redelivers what is pending.
	Resume(
		ctx context.Context,
		batch int,
	) error
	// Recover redelivers every pending durable delivery.
	Recover(
		ctx context.Context,
		batch int,
	) error
}

// New builds a Processor. The dispatcher comes from the factory given by
// WithDispatcherFactory, or from the in-memory default when there is none and a
// bus is present. A factory error, or a factory that returns no dispatcher,
// fails New and nothing is left running.
func New[T any](
	es eventstore.EventStore[T],
	bus asynxmd.Bus[T],
	opts ...models.Opt[T],
) (Processor[T], error) {
	p, err := core.New(
		es,
		bus,
		opts...,
	)
	if err != nil {
		return nil,
			err
	}

	return p,
		nil
}
