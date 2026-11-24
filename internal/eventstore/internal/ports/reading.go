package ports

import "context"

// Reading is the read side of an event store: it answers what an aggregate's
// current state is without changing any event. Implementations are safe for
// concurrent use and do not take the aggregate's command lock, so a read may
// observe a state that a command is about to supersede.
type Reading[T any] interface {
	// Get returns the current aggregate state, using the latest snapshot plus
	// the events after it when a usable snapshot exists and a full replay
	// otherwise. It returns models.ErrNotFound for an aggregate that has no
	// events and the storage error when the stores fail. A snapshot that cannot
	// be decoded is not an error: it is reported to the corruption callback
	// given to New and the state is rebuilt from the events. When replay had to
	// upcast an old event, Get also tries to store the upcast state as a
	// snapshot; that write is best effort and its failure is not reported.
	Get(
		ctx context.Context,
		aggregateID string,
	) (T, error)
	// Exists reports whether the aggregate has at least one event. It reads a
	// single event rather than rebuilding state, so it is much cheaper than Get.
	Exists(
		ctx context.Context,
		aggregateID string,
	) (bool, error)
	// Preload warms the aggregate by performing a full Get and discarding the
	// state, so the first real read does not pay the cold-path cost. An
	// aggregate that does not exist yet is not an error.
	Preload(
		ctx context.Context,
		aggregateID string,
	) error
}
