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
	// Wait, when non-nil, blocks until the event's handlers have run (set only
	// for envelopes with WaitHandlers). The sender calls it, not the worker, so a
	// slow or failing handler never occupies a shard worker.
	Wait func(context.Context) error
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
