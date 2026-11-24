package command

// Behavior validates a command against the aggregate and emits the
// state the aggregate takes after it. Both methods must be pure: no IO, no
// side effects, no randomness, and no mutation of current.
type Behavior[T any] interface {
	// Validate decides whether the command may be applied to current. It
	// receives nil current when the aggregate has never existed. A non-nil
	// error rejects the command: asynx returns it to the sender unchanged
	// (conventionally an error wrapping failure.ErrValidation) and writes no event.
	Validate(
		current *T,
	) error

	// EmitEvent returns the aggregate's new state, which becomes the event's
	// Aggregate. It receives nil current when the aggregate has never existed,
	// and is called only after Validate returned nil for the same current.
	EmitEvent(
		current *T,
	) T
}
