package models

import (
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

// Subscription is one registered handler together with its routing pattern.
type Subscription[T any] struct {
	Pattern         string
	Handler         asynxmd.ProjectionHandler[T]
	FallbackHandler asynxmd.ProjectionHandler[T]
	Timeout         time.Duration
}
