// Package models defines data structures shared across processor sub-packages.
//
// CommandEnvelope[T] and CommandJob[T] are simple wrappers with no embedded logic.
//   - CommandEnvelope[T] — Command + context + result channel; sender creates, waits on result
//   - CommandJob[T]      — Envelope + version number; dispatcher creates, worker consumes
//
// ResultChan is buffered size 1 to ensure non-blocking sends; slow readers are ignored.
// All fields are immutable after creation.
package models

import (
	"context"

	asynxmd "github.com/char2cs/asynx/models"
)

type CommandEnvelope[T any] struct {
	Cmd        asynxmd.Command[T]
	Ctx        context.Context
	ResultChan chan error
}

type CommandJob[T any] struct {
	Envelope    *CommandEnvelope[T]
	NextVersion int64
}
