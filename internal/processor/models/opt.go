package models

// Opt is a functional option for the processor constructor.
type Opt[T any] func(
	*Config[T],
)
