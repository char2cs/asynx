package write

import (
	"context"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/storage"
)

// Delivery returns an Op that enqueues d in the delivery log.
func Delivery(
	d event.Delivery,
) Op {
	return func(
		ctx context.Context,
		tx storage.Tx,
	) error {
		return tx.EnqueueDelivery(
			ctx,
			d.AggregateID,
			d.Version,
		)
	}
}

// Deliveries returns one Op per version that enqueues deliveries of
// aggregateID at versions 1 through count, in ascending order.
func Deliveries(
	aggregateID string,
	count int,
) []Op {
	ops := make(
		[]Op,
		count,
	)
	for i := range count {
		ops[i] = Delivery(event.Delivery{AggregateID: aggregateID, Version: int64(i + 1)})
	}

	return ops
}

// EraseDeliveries returns an Op that drops every pending delivery of
// aggregateID.
func EraseDeliveries(
	aggregateID string,
) Op {
	return func(
		ctx context.Context,
		tx storage.Tx,
	) error {
		return tx.DropDeliveries(
			ctx,
			aggregateID,
		)
	}
}
