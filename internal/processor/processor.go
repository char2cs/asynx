package processor

import (
	"github.com/char2cs/asynx/v2/internal/eventstore"
	"github.com/char2cs/asynx/v2/internal/processor/internal/api"
	"github.com/char2cs/asynx/v2/internal/processor/internal/core"
	"github.com/char2cs/asynx/v2/internal/processor/models"
	"github.com/char2cs/asynx/v2/models/pubsub"
)

// Processor is the command side of asynx: it routes each command to a
// per-aggregate shard so that one aggregate's commands run strictly one at a
// time, runs them through the write pipeline, delivers the committed events to
// subscribers and the bus, and offers the operations to inspect, recover and
// shut down delivery. It is composed of Sender (write), Lifecycle (stop) and
// Delivery (inspect and redeliver). Implementations are safe for concurrent
// use; construct one with New and end it with Shutdown.
type Processor[T any] interface {
	api.Sender[T]
	api.Lifecycle
	api.Delivery
}

// New builds a Processor. The dispatcher comes from the factory given by
// WithDispatcherFactory, or from the in-memory default when there is none and a
// bus is present. A factory error, or a factory that returns no dispatcher,
// fails New and nothing is left running.
func New[T any](
	es eventstore.EventStore[T],
	bus pubsub.Bus[T],
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
