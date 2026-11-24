package subscription

import (
	"time"

	"github.com/char2cs/asynx/v2/models/handler"
)

// WithFallback registers a secondary handler invoked, with the same context and
// event, when the primary handler panics or exceeds its handler timeout, so a
// subscriber can degrade to a simpler action instead of losing the event. The
// panic or timeout is still reported to the bus's handler.Panic or timeout
// handler. The fallback is not retried. Durable subscribers
// retry instead of falling back, and Build rejects the option on them with
// failure.ErrFallbackUnsupported.
func WithFallback[T any](
	callback handler.Projection[T],
) Opt[T] {
	return func(
		cfg *Config[T],
	) {
		cfg.Fallback = callback
	}
}

// WithHandlerTimeout sets the duration after which a subscription handler is
// reported as timed out. On the dispatcher path (every event committed through
// asynx) the handler is still waited for, so the aggregate's next event never
// overlaps it; the timeout is an alarm, not a cancellation.
func WithHandlerTimeout[T any](
	d time.Duration,
) Opt[T] {
	return func(
		cfg *Config[T],
	) {
		cfg.Timeout = d
	}
}
