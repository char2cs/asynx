package models

// BusSubscriber registers and removes subscriptions.
type BusSubscriber[T any] interface {
	Subscribe(
		pattern string,
		handler ProjectionHandler[T],
		opts ...SubscriptionOpt[T],
	) (string, error)

	Unsubscribe(
		id string,
	) error
}
