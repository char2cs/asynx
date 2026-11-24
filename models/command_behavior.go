package models

// CommandBehavior validates a command and emits its event.
type CommandBehavior[T any] interface {
	// Validate receives nil current when the aggregate has never existed.
	Validate(
		current *T,
	) error

	// EmitEvent receives nil current when the aggregate has never existed.
	EmitEvent(
		current *T,
	) T
}
