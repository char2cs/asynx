package models

// SubscriptionOpt customises a subscription.
type SubscriptionOpt[T any] func(
	*SubscriptionConfig[T],
)
