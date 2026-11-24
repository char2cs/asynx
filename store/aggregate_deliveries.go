package store

import (
	"github.com/char2cs/asynx/models/event"
)

type aggregateDeliveries string

func (a aggregateDeliveries) belong(
	d event.Delivery,
) bool {
	return d.AggregateID == string(a)
}
