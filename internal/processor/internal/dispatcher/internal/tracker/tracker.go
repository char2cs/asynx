package tracker

import (
	"github.com/char2cs/asynx/models/dispatch"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/jobs"
)

// Tracker keeps the jobs queued, running or retrying, until each completes.
// It is safe for concurrent use.
type Tracker[T any] interface {
	// Track registers job until its delivery is over, replacing the job
	// tracked for the same (aggregate, version). It takes over the job's
	// completion callback: when the job completes it removes itself, and never
	// the job that replaced it.
	Track(
		job jobs.Job[T],
	)

	// For returns the ticket of the job tracked for (aggregateID, version), or
	// false when there is none: it was never tracked or its delivery is over.
	For(
		aggregateID string,
		version int64,
	) (dispatch.Ticket, bool)

	// Idle blocks until every job tracked when it was called has completed.
	// Jobs tracked while it waits are not waited for.
	Idle()
}

// New creates an empty Tracker.
func New[T any]() Tracker[T] {
	return &inflightTracker[T]{
		inflight: make(map[jobs.Job[T]]struct{}),
		tickets:  make(map[ticketKey]jobs.Job[T]),
	}
}
