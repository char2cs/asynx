package models

// Delivery is a pending hand-off of one committed event to durable
// subscribers. It is written in the same transaction as the event and removed
// by AckDelivery once every handler for the event has finished, accepted by the
// dead-letter handler, or superseded: Forget drops an aggregate's entries
// (Tx.DropDeliveries), and the erase drops what is left.
type Delivery struct {
	AggregateID string
	Version     int64
}
