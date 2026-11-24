package asynx

import (
	"slices"

	"github.com/char2cs/asynx/v2/models/subscription"
)

type durableEvents[T any] []subscription.Durable[T]

func (d durableEvents[T]) wants(
	name string,
) bool {
	return slices.ContainsFunc(
		d,
		eventName[T](name).matchedBy,
	)
}
