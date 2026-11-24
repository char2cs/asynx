package asynx

import (
	"slices"

	"github.com/char2cs/asynx/models"
)

type durableEvents[T any] []models.DurableSub[T]

func (d durableEvents[T]) wants(
	name string,
) bool {
	return slices.ContainsFunc(
		d,
		eventName[T](name).matchedBy,
	)
}
