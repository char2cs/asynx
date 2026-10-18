package aggregates

// Item is what the Registry queues: one delivery of one event of an aggregate.
// The dispatcher's job type satisfies it.
type Item interface {
	// AggregateID names the aggregate whose record the item is queued in.
	AggregateID() string
	// Version is the version of the item's event, the key under which a park
	// records it as undelivered.
	Version() int64
	// Durable reports whether a park holds the item back. It must be true
	// exactly for a delivery that carries an Ack: the old dispatcher refused,
	// dropped and settled as parked only those (a tombstone or redelivery with a
	// nil Ack still flows through a park).
	Durable() bool
	// MarkUnrun settles the item as one whose handlers never ran, with err as
	// the reason. It is called under the registry mutex and must not call into
	// the Registry; a second settle is a no-op.
	MarkUnrun(err error)
	// Complete ends the item's delivery with err. It may be called under the
	// registry mutex and must not call into the Registry; a second completion is
	// a no-op.
	Complete(err error)
}
