package models

// Bus delivers events to subscribed handlers: the live, fire-and-forget side
// of asynx. A handler registered on a Bus sees events published while it is
// subscribed, is not retried and is not remembered across restarts; durable
// subscribers, which are, are run by the Dispatcher instead.
//
// A Bus is the union of BusPublisher (hand an event to the matching handlers),
// BusSubscriber (choose which handlers those are) and BusLifecycle (stop the
// bus and wait for its handlers). All methods are safe for concurrent use.
// Handler panics and timeouts are reported through the bus's panic and
// timeout handlers (PanicHandler, TimeoutHandler), never to the publisher.
type Bus[T any] interface {
	BusPublisher[T]
	BusSubscriber[T]
	BusLifecycle
}
