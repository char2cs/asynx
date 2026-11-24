package exec

import (
	asynxmd "github.com/char2cs/asynx/models"
	"time"
)

// HandlerJob carries all the data needed to execute one subscription handler
// for a single event.
type HandlerJob[T any] struct {
	Handler         asynxmd.ProjectionHandler[T]
	FallbackHandler asynxmd.ProjectionHandler[T]
	Timeout         time.Duration
	Event           asynxmd.Event[T]

	// Optional per-job callback overrides. When nil, the package-level
	// OnHandlerPanic / OnHandlerTimeout functions are used instead.
	OnPanic   asynxmd.PanicHandler[T]
	OnTimeout asynxmd.TimeoutHandler[T]

	// WaitAfterTimeout makes a timed-out handler still be waited for: the
	// timeout is reported but Handler returns only once the handler has
	// finished. The dispatcher path needs this so the next event of an
	// aggregate never starts while the previous one is still being handled.
	WaitAfterTimeout bool
}
