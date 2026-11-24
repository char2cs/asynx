package models

import (
	"time"
)

// SubscriptionConfig holds the options of one subscription. It is what a
// SubscriptionOpt writes into; Bus implementations read it when they register
// the subscription.
//
// Fallback, when set, is invoked with the same event after the primary handler
// panicked. Timeout, when positive, is the
// duration after which a still-running handler is reported to the
// TimeoutHandler; zero means no timeout.
type SubscriptionConfig[T any] struct {
	Fallback ProjectionHandler[T]
	Timeout  time.Duration
}
