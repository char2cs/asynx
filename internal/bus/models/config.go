package models

import asynxmd "github.com/char2cs/asynx/models"

// Config holds the callbacks a bus is built with. A nil callback selects the
// package-level default.
type Config[T any] struct {
	OnPanic   asynxmd.PanicHandler[T]
	OnTimeout asynxmd.TimeoutHandler[T]
}
