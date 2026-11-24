package replayer

import esmodels "github.com/char2cs/asynx/v2/internal/eventstore/models"

type replayFrame[T any] struct {
	JSON  []byte
	State T
	Event esmodels.InternalEvent
}
