package models

// PublishIndex is an immutable snapshot of the subscription indexes: ExactSubs
// maps an event name to the subscriptions registered for exactly that name, and
// RegexSubs lists the regular-expression subscriptions in registration order.
// Once stored via atomic.Pointer it is never modified; Subscribe and
// Unsubscribe always build and store a brand-new instance (copy-on-write), so
// Publish can read it without a lock.
type PublishIndex[T any] struct {
	ExactSubs map[string][]*Subscription[T]
	RegexSubs []*Subscription[T]
}
