package models

import (
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

type Subscription[T any] struct {
	Pattern         string
	Handler         asynxmd.ProjectionHandler[T]
	FallbackHandler asynxmd.ProjectionHandler[T]
	Timeout         time.Duration
}
