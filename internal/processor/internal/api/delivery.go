package api

// Delivery inspects the delivery state of the aggregates and redelivers what
// is pending.
type Delivery interface {
	Inspector
	Recoverer
}
