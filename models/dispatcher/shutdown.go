package dispatcher

import (
	"context"
)

// Shutdown stops the dispatcher and waits for its workers.
type Shutdown interface {
	// Close stops accepting requests, interrupts retries and waits for every
	// worker to drain its queue. If ctx ends first the context error is
	// returned, the handlers' contexts are cancelled, and Close may be called
	// again to finish waiting.
	Close(
		ctx context.Context,
	) error

	// WaitIdle blocks until every event queued when it was called has been
	// handled. Events queued while it waits are not waited for, and are not
	// refused.
	WaitIdle()
}
