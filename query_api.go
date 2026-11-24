package asynx

// QueryAPI is the half of Asynx that reads state and observes events: loading
// aggregates (AggregateReader), registering and removing handlers
// (SubscriptionManager), and consuming events through channels, blocking waits
// and replays (EventListener).
//
// None of these operations changes an aggregate. Registering a subscription
// changes what the instance delivers to the caller, not what it stores, and
// Replay never writes snapshots. A read-only component can be given QueryAPI
// without being able to send commands or shut the instance down.
type QueryAPI[T any] interface {
	AggregateReader[T]
	SubscriptionManager[T]
	EventListener[T]
}
