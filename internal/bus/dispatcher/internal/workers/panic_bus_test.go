package workers_test

import (
	"context"

	asynxmd "github.com/char2cs/asynx/models"
)

type panicBus struct{ recordingBus }

func (*panicBus) PublishSync(
	context.Context,
	asynxmd.Event[int],
) error {
	panic("bus exploded")
}
