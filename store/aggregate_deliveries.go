package store

import "github.com/char2cs/asynx/models"

type aggregateDeliveries string

func (a aggregateDeliveries) belong(
	d models.Delivery,
) bool {
	return d.AggregateID == string(a)
}
