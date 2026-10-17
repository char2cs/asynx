package models

// Command defines the contract for aggregate mutations.
// Implementations must be pure — no IO, side effects, or randomness.
type Command[T any] interface {
	AggregateID() string
	EventName() string
	ShouldSnapshot() bool

	// Validate receives nil current when the aggregate has never existed.
	Validate(
		current *T,
	) error

	// EmitEvent receives nil current when the aggregate has never existed.
	EmitEvent(
		current *T,
	) T
}

// IdempotentCommand is optionally implemented by a Command that carries a
// client-minted id. When CommandID returns a non-empty string, asynx records a
// receipt for the command in the same transaction as its event: retrying the
// same id is a no-op that returns the original event, and a command that was
// rejected stays rejected.
type IdempotentCommand interface {
	CommandID() string
}
