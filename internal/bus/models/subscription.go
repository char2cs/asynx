package models

import (
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

// Subscription is one registered handler together with its routing pattern, the
// optional fallback run when the handler panics, and the timeout (zero for none)
// applied to each delivery. It is created by Subscribe and shared read-only with
// every in-flight delivery, so it must not be mutated after registration.
type Subscription[T any] struct {
	Pattern         string
	Handler         asynxmd.ProjectionHandler[T]
	FallbackHandler asynxmd.ProjectionHandler[T]
	Timeout         time.Duration
}
