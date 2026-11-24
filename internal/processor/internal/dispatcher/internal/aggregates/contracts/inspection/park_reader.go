package inspection

import (
	"github.com/char2cs/asynx/v2/models/dispatch"
)

// ParkReader reads the parks of aggregates.
type ParkReader interface {
	// Inspect reports, under one hold of the mutex, whether the aggregate is
	// parked, the park's generation, its cause and the versions it left
	// undelivered, in ascending order. An aggregate that is not parked, or
	// that the registry holds no record of, has the zero dispatch.Status.
	Inspect(
		aggregateID string,
	) dispatch.Status

	// Unrun reports why no handler ran for the event at (aggregateID, version):
	// ErrDeliveryParked while the aggregate is parked and the event was left
	// undelivered by it, nil otherwise.
	Unrun(
		aggregateID string,
		version int64,
	) error
}
