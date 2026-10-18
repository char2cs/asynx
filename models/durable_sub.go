package models

import "time"

// DurableSub is a durable subscriber the dispatcher runs itself, with retry,
// instead of publishing through the bus.
type DurableSub[T any] struct {
	// Name identifies the subscriber in the errors reported for it.
	Name string
	// Match reports whether the subscriber wants an event name.
	Match func(eventName string) bool
	// Handler delivers an event. A non-nil error, a panic, or a timeout then
	// error keeps the delivery pending and retries.
	Handler DurableHandler[T]
	// Timeout, when positive, reports a slow handler as timed out. The handler
	// is still waited for.
	Timeout time.Duration
}
