package models

// DispatcherFactory builds the Dispatcher an instance delivers its events
// through, from the configuration the instance was built with. A non-nil error
// fails the build. The factory is called once per instance, and the returned
// Dispatcher is owned by that instance, which closes it on shutdown.
type DispatcherFactory[T any] func(
	config DispatcherConfig[T],
) (Dispatcher[T], error)
