package api

import (
	"context"
)

// Recoverer redelivers durable deliveries that did not complete: Recover once
// at start-up for what a crash left pending, Resume at run time for aggregates
// a failing delivery parked. Both enqueue and return without waiting for the
// handlers, and both honour ctx between aggregates.
type Recoverer interface {
	// Resume lifts the park of every parked aggregate and redelivers its
	// pending deliveries in order, under the aggregate's lock. An aggregate
	// that fails stays parked and the others still resume; errors are joined.
	// A non-positive batch selects the default log read size.
	Resume(
		ctx context.Context,
		batch int,
	) error
	// Recover re-enqueues every unacknowledged durable delivery in enqueue
	// order. It takes no aggregate lock and must run before commands are sent.
	// An aggregate whose event cannot be rebuilt is parked and reported to the
	// publish error handler while the others recover. A non-positive batch
	// selects the default log read size.
	Recover(
		ctx context.Context,
		batch int,
	) error
}
