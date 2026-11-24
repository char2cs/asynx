package models

type SubscriptionOpt[T any] func(
	*SubscriptionConfig[T],
)
