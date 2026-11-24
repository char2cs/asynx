package command

// Meta identifies the command's target and event and says whether the
// resulting state is snapshotted. It must be pure and stable: asynx may read
// it at any point of the command's life and expects the same answers.
type Meta interface {
	// AggregateID returns the id of the aggregate the command applies to. It
	// selects the event stream and the per-aggregate order the command is
	// handled in, so it must not be empty and must not change.
	AggregateID() string

	// EventName returns the name of the event the command emits. Subscribers
	// are matched against it, so it should be stable across versions of the
	// program: it is persisted with the event.
	EventName() string

	// ShouldSnapshot reports whether asynx should store a snapshot of the
	// aggregate after this command's event. A snapshot is only a cache that
	// shortens replay; answering false or true never changes the aggregate's
	// state.
	ShouldSnapshot() bool
}
