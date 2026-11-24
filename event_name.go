package asynx

import (
	"github.com/char2cs/asynx/models"
)

type eventName[T any] string

func (n eventName[T]) matchedBy(
	sub models.DurableSub[T],
) bool {
	return sub.Match(string(n))
}
