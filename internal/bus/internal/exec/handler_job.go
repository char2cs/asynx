package exec

import (
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/handler"
	"time"
)

// HandlerJob carries everything Handler needs to execute one subscription's
// handler for a single event: the handler and its optional fallback, the
// timeout (zero means none), the event, and the observers that are told about
// panics and timeouts. A job is built per event and delivery by MakeJob, owned
// by the one Handler call that runs it, and not reused or shared between
// goroutines.
type HandlerJob[T any] struct {
	Handler         handler.Projection[T]
	FallbackHandler handler.Projection[T]
	Timeout         time.Duration
	Event           event.Event[T]

	// Optional per-job callback overrides. When nil, the package-level
	// OnHandlerPanic / OnHandlerTimeout functions are used instead.
	OnPanic   handler.Panic[T]
	OnTimeout handler.Timeout[T]

	// WaitAfterTimeout makes a timed-out handler still be waited for: the
	// timeout is reported but Handler returns only once the handler has
	// finished. The dispatcher path needs this so the next event of an
	// aggregate never starts while the previous one is still being handled.
	WaitAfterTimeout bool
}
