package aggregates

// Hooks are the callbacks Enqueue runs under the registry mutex, so that what
// they do is atomic with the enqueue itself: with the closed check (no worker
// is started, and no job is tracked, once the registry is closed) and with the
// append (a job is tracked before its worker can pop it).
type Hooks[J Item] struct {
	// Spawn is called when an aggregate without a worker gets a job: the caller
	// starts the worker goroutine for aggregateID (and counts it, for Close).
	// The worker owns the record's wake and halt channels until it retires.
	Spawn func(
		aggregateID string,
	)
	// Admit is called for every job Enqueue queues, before it is appended.
	// A job refused because its aggregate is parked is not admitted.
	Admit func(
		job J,
	)
}
