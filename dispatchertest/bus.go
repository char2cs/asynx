package dispatchertest

import (
	"context"
	"sync"

	"github.com/char2cs/asynx/models"
)

type bus struct {
	fn func(
		context.Context,
		models.Event[Aggregate],
	) error
	mu    sync.Mutex
	calls []models.Event[Aggregate]
}

func (b *bus) PublishSync(
	ctx context.Context,
	e models.Event[Aggregate],
) error {
	err := b.call(ctx, e)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls = append(b.calls, e)
	return err
}

func (b *bus) call(
	ctx context.Context,
	e models.Event[Aggregate],
) error {
	if b.fn == nil {
		return nil
	}
	return b.fn(ctx, e)
}

func (b *bus) published() []models.Event[Aggregate] {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]models.Event[Aggregate](nil), b.calls...)
}

func (b *bus) Publish(
	context.Context,
	models.Event[Aggregate],
) error {
	return nil
}

func (b *bus) Subscribe(
	string,
	models.ProjectionHandler[Aggregate],
	...models.SubscriptionOpt[Aggregate],
) (string, error) {
	return "", nil
}

func (b *bus) Unsubscribe(
	string,
) error {
	return nil
}

func (b *bus) Close(
	context.Context,
) error {
	return nil
}

func (b *bus) WaitForHandlers() {}
