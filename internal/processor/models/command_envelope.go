package models

import (
	"context"
	asynxmd "github.com/char2cs/asynx/models"
)

type CommandEnvelope[T any] struct {
	Cmd          asynxmd.Command[T]
	Ctx          context.Context
	ResultChan   chan CommandResult[T]
	WaitHandlers bool
}
