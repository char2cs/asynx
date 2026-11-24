package asynx

import (
	"github.com/char2cs/asynx/v2/models/subscription"
)

type eventName[T any] string

func (n eventName[T]) matchedBy(
	sub subscription.Durable[T],
) bool {
	return sub.Match(string(n))
}
