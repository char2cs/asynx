package bus

import (
	"github.com/char2cs/asynx/internal/bus/internal/channel"
	"github.com/char2cs/asynx/internal/bus/internal/routing"
	"github.com/char2cs/asynx/internal/bus/models"
	asynxmd "github.com/char2cs/asynx/models"
)

// Bus is the in-process event bus the asynx runtime publishes projection events
// on: handlers are registered under a name or regular-expression pattern and
// each published event is delivered to every matching handler on its own
// goroutine. The interface is exactly the public models.Bus contract; the
// implementation behind New stays unexported to this package tree so the
// runtime depends only on that contract.
type Bus[T any] interface {
	asynxmd.Bus[T]
}

// New builds an empty in-process bus. opts are applied in order and may set the
// panic and timeout callbacks (WithPanicHandler, WithTimeoutHandler); a callback
// left unset reports to stderr. The returned bus is safe for concurrent use and
// is shut down with Close, which waits for in-flight handlers.
func New[T any](
	opts ...models.Opt[T],
) Bus[T] {
	return channel.New(opts...)
}

// WithPanicHandler returns the option that makes the bus call fn whenever a
// subscription handler (or its fallback) panics, with the handler's context,
// the event being delivered and the recovered value. fn runs on the goroutine
// that recovered the panic, concurrently with other handlers, and a panic inside
// fn is contained and reported on stderr so a faulty observer cannot crash the
// process.
// When the option is absent, panics are written to stderr.
func WithPanicHandler[T any](
	fn asynxmd.PanicHandler[T],
) models.Opt[T] {
	return func(
		cfg *models.Config[T],
	) {
		cfg.OnPanic = fn
	}
}

// WithTimeoutHandler returns the option that makes the bus call fn whenever a
// subscription handler exceeds its configured timeout, with the handler's
// context, the event and the timeout that elapsed. fn runs on the goroutine
// waiting for the handler, concurrently with other handlers, and a panic inside
// fn is contained and reported on stderr. A timed-out handler is abandoned and keeps
// running, except under PublishSync, which still waits for it. When the option
// is absent, timeouts are written to stderr.
func WithTimeoutHandler[T any](
	fn asynxmd.TimeoutHandler[T],
) models.Opt[T] {
	return func(
		cfg *models.Config[T],
	) {
		cfg.OnTimeout = fn
	}
}

// IsExactPattern reports whether pattern names a single event rather than a
// regular expression: true when it contains no regexp metacharacter, so the bus
// matches it by plain equality with the event name. Callers use it to decide
// whether a subscription pattern can be looked up directly or must be tested
// against every event. It is pure and safe for concurrent use.
func IsExactPattern(
	pattern string,
) bool {
	return routing.IsExactPattern(pattern)
}
