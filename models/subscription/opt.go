package subscription

// Opt customises a subscription by writing into its
// Config. Options are applied in order when the subscription is
// registered, so a later option overrides an earlier one for the same field.
// WithFallback and WithHandlerTimeout are the options asynx provides.
type Opt[T any] func(
	*Config[T],
)
