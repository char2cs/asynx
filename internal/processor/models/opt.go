package models

// Opt is a functional option for the processor constructor: it receives the
// Config being built and sets one field. Options apply in the order given, so
// a later option overrides an earlier one.
type Opt[T any] func(
	*Config[T],
)
