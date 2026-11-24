package models

// Opt is a functional option for the bus constructor.
type Opt[T any] func(
	*Config[T],
)
