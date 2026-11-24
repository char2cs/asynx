package event

// Receipt records the outcome of an idempotent command (see command.Idempotent).
type Receipt struct {
	// Accepted is true when the command produced an event.
	Accepted bool
	// AggregateID, EventID and Version identify the event an accepted command
	// produced.
	AggregateID string
	EventID     string
	Version     int64
	// Rejection holds the validation error text of a rejected command.
	Rejection string
}
