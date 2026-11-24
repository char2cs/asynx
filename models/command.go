package models

// Command defines the contract for aggregate mutations: a request to change
// the aggregate of type T. asynx loads the aggregate's current state,
// validates the command against it and turns it into one event.
//
// A Command is the union of CommandMeta (which aggregate and event it is, and
// whether it snapshots) and CommandBehavior (how it validates and what it
// emits). Implementations must be pure, with no IO, side effects or
// randomness: the same command applied to the same state must always give the
// same event, which is what makes replaying and retrying commands safe.
//
// A command may additionally implement IdempotentCommand to make retries by
// the same client a no-op.
type Command[T any] interface {
	CommandMeta
	CommandBehavior[T]
}
