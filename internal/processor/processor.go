package processor

import (
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/processor/internal/api"
	"github.com/char2cs/asynx/internal/processor/internal/core"
	"github.com/char2cs/asynx/internal/processor/models"
	asynxmd "github.com/char2cs/asynx/models"
)

// Processor routes commands to per-aggregate shards, runs them through the
// write pipeline and delivers the resulting events. Construct it with New.
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
