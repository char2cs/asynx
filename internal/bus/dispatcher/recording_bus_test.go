package dispatcher

import (
	"context"
	"sync"
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

type recordingBus struct {
	mu    sync.Mutex
	calls []asynxmd.Event[int]
	delay time.Duration
}

func (b *recordingBus) PublishSync(
	_ context.Context,
	e asynxmd.Event[int],
) error {
	if b.delay > 0 {
		time.Sleep(b.delay)
	}
	b.mu.Lock()
	b.calls = append(b.calls, e)
	b.mu.Unlock()
	return nil
}

func (b *recordingBus) Publish(
	context.Context,
	asynxmd.Event[int],
) error {
	return nil
}

func (b *recordingBus) Subscribe(
	string,
	asynxmd.ProjectionHandler[int],
	...asynxmd.SubscriptionOpt[int],
) (string, error) {
	return "", nil
}

func (b *recordingBus) Unsubscribe(string) error { return nil }

func (b *recordingBus) Close(context.Context) error { return nil }

func (b *recordingBus) WaitForHandlers() {}

func (b *recordingBus) snapshot() []asynxmd.Event[int] {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]asynxmd.Event[int], len(b.calls))
	copy(out, b.calls)
	return out
}
