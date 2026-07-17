// Package models defines data structures shared across processor sub-packages.
//
// CommandEnvelope[T] and CommandJob[T] are simple wrappers with no embedded logic.
//   - CommandEnvelope[T] — Command + context + result channel; sender creates, waits on result
//   - CommandJob[T]      — Wraps an envelope for the worker pool; dispatcher creates, worker consumes
//
// ResultChan is buffered size 1 to ensure non-blocking sends; slow readers are ignored.
// All fields are immutable after creation.
package models

import (
	"context"

	asynxmd "github.com/char2cs/asynx/models"
)

type CommandResult[T any] struct {
	Event asynxmd.Event[T]
	Err   error
}

type CommandEnvelope[T any] struct {
	Cmd          asynxmd.Command[T]
	Ctx          context.Context
	ResultChan   chan CommandResult[T]
	WaitHandlers bool
}

type CommandJob[T any] struct {
	Envelope *CommandEnvelope[T]
}
