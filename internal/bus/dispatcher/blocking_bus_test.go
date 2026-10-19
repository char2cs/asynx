package dispatcher

import (
	"context"

	asynxmd "github.com/char2cs/asynx/models"
)

type blockingBus struct {
	recordingBus
	entered chan struct{}
	release chan struct{}
}

func (b *blockingBus) PublishSync(
	ctx context.Context,
	e asynxmd.Event[int],
) error {
	b.entered <- struct{}{}
	<-b.release
	return b.recordingBus.PublishSync(ctx, e)
}
