package dispatcher

// Lifecycle ends a Dispatcher's work: Interrupt makes
// retries give up, and Shutdown stops the dispatcher and waits for
// its workers. Interrupting is the first step of shutdown, so that callers
// waiting on a retrying event are released before the workers drain.
type Lifecycle interface {
	Interrupt
	Shutdown
}
