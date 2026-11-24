package models

// DispatcherDelivery is the delivery side of a Dispatcher: what is queued for
// an aggregate and what became of it. DispatcherQueue changes the queue and
// DispatcherInspector reports on it.
type DispatcherDelivery[T any] interface {
	DispatcherQueue[T]
	DispatcherInspector
}
