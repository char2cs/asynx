package workers_test

import (
	"context"
	"sync"

	asynxmd "github.com/char2cs/asynx/models"
)

type gateBus struct {
	recordingBus
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *gateBus) PublishSync(
	ctx context.Context,
	e asynxmd.Event[int],
) error {
	b.once.Do(func() {
		close(b.entered)
		<-b.release
	})
	return nil
}
