package aggregates

// Hooks are the callbacks Enqueue runs under the registry mutex, so that what
// they do is atomic with the enqueue itself: with the closed check (no job is
// tracked or counted, once the registry is closed) and with the append (a job
// is tracked before its turn can begin).
type Hooks[J Item] struct {
	// Admit is called for every job Enqueue queues, before it is appended.
	// A job refused because its aggregate is parked is not admitted.
	Admit func(
		job J,
	)
}
