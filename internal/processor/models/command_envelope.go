package models

import (
	"context"
	asynxmd "github.com/char2cs/asynx/models"
)

// CommandEnvelope carries a command and its caller context through a shard queue.
type CommandEnvelope[T any] struct {
	Cmd          asynxmd.Command[T]
	Ctx          context.Context
	ResultChan   chan CommandResult[T]
	WaitHandlers bool
}
