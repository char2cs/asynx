package models

// DispatcherLifecycle groups the interruption and shutdown of a Dispatcher.
type DispatcherLifecycle interface {
	DispatcherInterrupt
	DispatcherShutdown
}
