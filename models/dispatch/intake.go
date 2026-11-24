package dispatch

// Intake is the delivery side of a dispatcher.Dispatcher: what is queued for
// an aggregate and what became of it. Queue changes the queue and
// Inspector reports on it.
type Intake[T any] interface {
	Queue[T]
	Inspector
}
