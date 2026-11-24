package models

// DispatcherLifecycle ends a Dispatcher's work: DispatcherInterrupt makes
// retries give up, and DispatcherShutdown stops the dispatcher and waits for
// its workers. Interrupting is the first step of shutdown, so that callers
// waiting on a retrying event are released before the workers drain.
type DispatcherLifecycle interface {
	DispatcherInterrupt
	DispatcherShutdown
}
