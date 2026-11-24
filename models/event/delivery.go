package event

// Delivery is a pending hand-off of one committed event to durable
// subscribers. It is written in the same transaction as the event and removed
// by AckDelivery once every handler for the event has finished, accepted by the
// dead-letter handler, or superseded: Forget drops an aggregate's entries
// (storage.TxDeliveryWriter.DropDeliveries), and the erase drops what is left.
//
// The pair (AggregateID, Version) identifies the delivery and is its only
// content: the event itself is read back from the stream. A forgotten
// aggregate restarts at version 1, so a key may be enqueued again after it was
// acknowledged or dropped.
type Delivery struct {
	AggregateID string
	Version     int64
}
