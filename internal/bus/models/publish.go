package models

// PublishIndex is an immutable snapshot of the subscription indexes.
// Once stored via atomic.Pointer it is never modified; Subscribe and
// Unsubscribe always build and store a brand-new instance (copy-on-write).
type PublishIndex[T any] struct {
	ExactSubs map[string][]*Subscription[T] // pattern → subs (exact-match index)
	RegexSubs []*Subscription[T]            // subs whose pattern has metacharacters
}
