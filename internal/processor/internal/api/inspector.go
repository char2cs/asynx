package api

import (
	asynxmd "github.com/char2cs/asynx/models"
)

// Inspector reports the delivery state of the aggregates.
type Inspector interface {
	// Paused returns the aggregates whose deliveries are parked.
	Paused() []string
	// Inspect reports the delivery status of an aggregate.
	Inspect(
		aggregateID string,
	) asynxmd.DeliveryStatus
}
