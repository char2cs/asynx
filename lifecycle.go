package asynx

import (
	"context"
)

// Lifecycle controls the running life of an instance: stopping it and draining
// the events it has queued.
type Lifecycle[T any] interface {
	// Shutdown stops the instance. It first refuses new commands, with
	// models.ErrShuttingDown, then waits for the commands and Forgets already
	// running, and finally closes the dispatcher and the bus, which stops live
	// delivery. Durable deliveries that were interrupted stay pending in the
	// store and are redelivered by the next Build.
	//
	// If ctx ends before the work is done Shutdown returns its error and may
	// be called again to finish. Once it has completed, or while another call
	// is running, it returns models.ErrAlreadyShuttingDown.
	Shutdown(
		ctx context.Context,
	) error

	// WaitPublish blocks until every event queued when it was called has been
	// handled. It is for tests: under live traffic it still returns, but what it
	// waited for is only a moment's worth of events. Commands sent while it waits
	// are neither refused nor waited for. Production code that needs its
	// projections up to date uses SendWait.
	WaitPublish()
}
