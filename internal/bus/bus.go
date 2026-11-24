package bus

import (
	"github.com/char2cs/asynx/internal/bus/internal/channel"
	"github.com/char2cs/asynx/internal/bus/internal/routing"
	"github.com/char2cs/asynx/internal/bus/models"
	asynxmd "github.com/char2cs/asynx/models"
)

// Bus is the in-process event bus.
type Bus[T any] interface {
	asynxmd.Bus[T]
}

// New returns the in-process channel bus.
func New[T any](
	opts ...models.Opt[T],
) Bus[T] {
	return channel.New(opts...)
}

// WithPanicHandler sets a custom callback invoked when a subscription handler
// panics. When not set, the package-level handler is used.
func WithPanicHandler[T any](
	fn asynxmd.PanicHandler[T],
) models.Opt[T] {
	return func(
		cfg *models.Config[T],
	) {
		cfg.OnPanic = fn
	}
}

// WithTimeoutHandler sets a custom callback invoked when a subscription handler
// exceeds its timeout. When not set, the package-level handler is used.
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
// regular expression.
func IsExactPattern(
	pattern string,
) bool {
	return routing.IsExactPattern(pattern)
}
