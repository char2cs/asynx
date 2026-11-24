package models

import (
	"time"
)

type SubscriptionConfig[T any] struct {
	Fallback ProjectionHandler[T]
	Timeout  time.Duration
}
