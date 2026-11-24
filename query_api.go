package asynx

// QueryAPI groups the operations that read state and observe events.
type QueryAPI[T any] interface {
	AggregateReader[T]
	SubscriptionManager[T]
	EventListener[T]
}
