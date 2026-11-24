package aggregates

// Admission tells the caller of Enqueue what became of the job.
type Admission struct {
	// Spawned is true when the job's aggregate had no worker: Enqueue has called
	// Hooks.Spawn for it.
	Spawned bool
	// Parked is true when the job was refused because its aggregate is parked:
	// it was not queued, its version is recorded as undelivered, and it has
	// been marked unrun and completed with ErrDeliveryParked.
	Parked bool
}
