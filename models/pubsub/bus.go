package pubsub

// Bus delivers events to subscribed handlers: the live, fire-and-forget side
// of asynx. A handler registered on a Bus sees events published while it is
// subscribed, is not retried and is not remembered across restarts; durable
// subscribers, which are, are run by the dispatcher.Dispatcher instead.
//
// A Bus is the union of Publisher (hand an event to the matching handlers),
// Subscriber (choose which handlers those are) and Lifecycle (stop the
// bus and wait for its handlers). All methods are safe for concurrent use.
// Handler panics and timeouts are reported through the bus's panic and
// timeout handlers (handler.Panic, handler.Timeout), never to the publisher.
type Bus[T any] interface {
	Publisher[T]
	Subscriber[T]
	Lifecycle
}
