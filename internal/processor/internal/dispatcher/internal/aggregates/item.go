package aggregates

// Item is what the Registry queues: one delivery of one event of an aggregate.
// The dispatcher's job type satisfies it. The registry finds the item's record
// through Keyed and settles and completes it through Settler.
type Item interface {
	Keyed
	Settler
	// Acked reports whether the item carries an Ack, which is exactly when a
	// park holds it back: a park refuses, drops and settles as parked only
	// those, while a live event, or a tombstone or redelivery with a nil Ack,
	// still flows through it.
	Acked() bool
}
