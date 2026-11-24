package contracts

import asynxmd "github.com/char2cs/asynx/models"

// JobIdentity names a job and carries the event it delivers.
type JobIdentity[T any] interface {
	// AggregateID returns the aggregate the event belongs to.
	AggregateID() string

	// Version returns the event's version within its aggregate.
	Version() int64

	// Event returns the event being delivered.
	Event() asynxmd.Event[T]
}
