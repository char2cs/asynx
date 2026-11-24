package api

import (
	"context"
)

// Lifecycle stops the processor and waits for its deliveries.
type Lifecycle interface {
	// Shutdown stops accepting work, drains it and closes the dispatcher and bus.
	Shutdown(
		ctx context.Context,
	) error
	// WaitPublish blocks until all dispatched events have been delivered.
	WaitPublish()
}
