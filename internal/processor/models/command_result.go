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
