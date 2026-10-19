package asynx

import "github.com/char2cs/asynx/models"

type subscriberDef[T any] struct {
	pattern string
	handler models.DurableHandler[T]
	opts    []models.SubscriptionOpt[T]
}
