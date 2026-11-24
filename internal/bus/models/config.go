package models

import (
	"github.com/char2cs/asynx/models/handler"
)

// Config is the mutable settings record the bus constructor builds up by
// applying its options in order before the bus copies the result. A nil
// callback selects the default reporter, which writes to stderr. The zero
// value is valid and means "all defaults".
type Config[T any] struct {
	OnPanic   handler.Panic[T]
	OnTimeout handler.Timeout[T]
}
