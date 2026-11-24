package models

// Bus delivers events to subscribed handlers.
type Bus[T any] interface {
	BusPublisher[T]
	BusSubscriber[T]
	BusLifecycle
}
