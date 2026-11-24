package models

import (
	"context"
)

// BusLifecycle closes the bus and waits for in-flight handlers.
type BusLifecycle interface {
	Close(
		ctx context.Context,
	) error

	// WaitForHandlers blocks until all in-flight handler executions complete.
	// Only for use in tests; do not call in production code.
	WaitForHandlers()
}
