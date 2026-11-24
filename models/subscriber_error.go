package models

import (
	"fmt"
)

// SubscriberError is the failure of one durable subscriber. The delivery error
// handler and the dead-letter handler receive their errors wrapped in it, so a
// handler can tell which subscriber failed (errors.As) while errors.Is still
// sees the cause. When several subscribers fail for one event the dead-letter
// handler receives them all, joined.
type SubscriberError struct {
	// Subscriber names the subscriber: its pattern and registration order, such
	// as "OrderUpdated#1".
	Subscriber string
	Err        error
}

func (e *SubscriberError) Error() string {
	return fmt.Sprintf(
		"subscriber %s: %v",
		e.Subscriber,
		e.Err,
	)
}

func (e *SubscriberError) Unwrap() error { return e.Err }
