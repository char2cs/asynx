package models

// Command defines the contract for aggregate mutations.
// Implementations must be pure — no IO, side effects, or randomness.
type Command[T any] interface {
	CommandMeta
	CommandBehavior[T]
}
