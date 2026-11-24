package asynx

import (
	"github.com/char2cs/asynx/models/handler"
	"github.com/char2cs/asynx/models/subscription"
)

type subscriberDef[T any] struct {
	pattern string
	handler handler.Durable[T]
	opts    []subscription.Opt[T]
}
