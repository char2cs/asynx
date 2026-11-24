package models

import (
	"time"

	"github.com/char2cs/asynx/v2/models/handler"
)

// Subscription is one registered handler together with its routing pattern, the
// optional fallback run when the handler panics, and the timeout (zero for none)
// applied to each delivery. It is created by Subscribe and shared read-only with
// every in-flight delivery, so it must not be mutated after registration.
type Subscription[T any] struct {
	Pattern         string
	Handler         handler.Projection[T]
	FallbackHandler handler.Projection[T]
	Timeout         time.Duration
}
