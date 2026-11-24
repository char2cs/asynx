package models

import (
	"time"
)

// SubscriptionConfig holds the options of one subscription.
type SubscriptionConfig[T any] struct {
	Fallback ProjectionHandler[T]
	Timeout  time.Duration
}
