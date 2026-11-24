package replayer

import esmodels "github.com/char2cs/asynx/internal/eventstore/models"

type replayFrame[T any] struct {
	JSON  []byte
	State T
	Event esmodels.InternalEvent
}
