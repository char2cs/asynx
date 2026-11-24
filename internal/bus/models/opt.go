package models

// Opt is a functional option for the bus constructor: it adjusts the Config the
// constructor is assembling. Options are applied in the order given, so a later
// option overrides an earlier one that sets the same field.
type Opt[T any] func(
	*Config[T],
)
