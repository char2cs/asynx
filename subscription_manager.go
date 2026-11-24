package asynx

import (
	"github.com/char2cs/asynx/models"
)

// SubscriptionManager registers and removes event subscriptions.
type SubscriptionManager[T any] interface {
	Subscribe(
		pattern string,
		handler models.ProjectionHandler[T],
		opts ...models.SubscriptionOpt[T],
	) (string, error)

	Unsubscribe(
		id string,
	) error
}
