package models

import (
	"context"
	"github.com/char2cs/asynx/models/event"
)

// CommandResult is the single outcome of an envelope: the committed event and
// error of a command that ran, or only an error for a command the shard
// rejected. It is written once to the envelope's ResultChan and read by the
// sender.
type CommandResult[T any] struct {
	Event event.Event[T]
	Err   error
	// Wait, when non-nil, blocks until the event's handlers have run (set only
	// for envelopes with WaitHandlers). The sender calls it, not the worker, so a
	// slow or failing handler never occupies a shard worker.
	Wait func(context.Context) error
}
