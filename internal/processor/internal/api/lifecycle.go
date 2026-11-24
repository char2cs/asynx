package api

import (
	"context"
)

// Lifecycle is how a processor is stopped and how a test waits for its
// deliveries. Shutdown is the production entry point; WaitPublish is a testing
// aid that is part of the interface because the root package's tests drive the
// processor only through it.
type Lifecycle interface {
	// Shutdown stops accepting work (new calls get ErrShuttingDown), waits
	// for the commands and Forgets in flight, drains the shards and closes the
	// dispatcher and the bus, within ctx. If ctx ends first it returns the
	// error and may be called again to finish; once complete, or while another
	// call runs, it returns ErrAlreadyShuttingDown.
	Shutdown(
		ctx context.Context,
	) error
	// WaitPublish blocks until all dispatched events have been delivered. It
	// is meant for tests; production code should use SendWait or Shutdown.
	WaitPublish()
}
