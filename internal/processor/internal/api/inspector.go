package api

import (
	"github.com/char2cs/asynx/models/dispatch"
)

// Inspector is the read-only view of durable delivery. Both methods are safe to
// call concurrently with deliveries and report the dispatcher's state at the
// moment of the call; without a dispatcher they report nothing parked.
type Inspector interface {
	// Paused returns, sorted, the aggregates whose durable deliveries are
	// parked after a delivery gave up or a Forget could not deliver its marker.
	Paused() []string
	// Inspect reports whether the aggregate is parked, the park's generation,
	// the error that parked it and the versions left undelivered.
	Inspect(
		aggregateID string,
	) dispatch.Status
}
