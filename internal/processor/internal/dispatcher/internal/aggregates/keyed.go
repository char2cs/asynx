package aggregates

// Keyed names the aggregate and the version of an item's event.
type Keyed interface {
	// AggregateID names the aggregate whose record the item is queued in.
	AggregateID() string
	// Version is the version of the item's event, the key under which a park
	// records it as undelivered.
	Version() int64
}
