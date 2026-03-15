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
