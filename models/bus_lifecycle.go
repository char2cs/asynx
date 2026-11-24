package models

import (
	"context"
)

// BusLifecycle stops a Bus and waits for the handlers it has started.
type BusLifecycle interface {
	// Close marks the bus closed, so every later Subscribe, Publish and
	// PublishSync returns ErrBusClosed, and waits for the handlers already in
	// flight. It returns nil once they have all finished and ctx.Err() if ctx
	// ends first; the bus stays closed and the remaining handlers keep running
	// in that case. Calling Close again is safe and waits again.
	Close(
		ctx context.Context,
	) error

	// WaitForHandlers blocks until every handler started by Publish has
	// finished, without closing the bus or bounding the wait. Handlers started
	// by PublishSync are not counted: that call waits for its own. Only for
	// use in tests and in shutdown paths that cannot pass a context; code that
	// can should call Close with a deadline instead.
	WaitForHandlers()
}
