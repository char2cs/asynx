package mockbuses

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/v2/models/event"
	"testing"
)

func TestErrBus_ReturnsItsErrorForPublish(
	t *testing.T,
) {
	sentinel := errors.New("down")
	b := &ErrBus[fakeOrder]{Err: sentinel}
	if err := b.Publish(
		context.Background(),
		event.Event[fakeOrder]{},
	); !errors.Is(
		err,
		sentinel,
	) {
		t.Fatalf(
			"Publish = %v, want %v",
			err,
			sentinel,
		)
	}

	if err := b.PublishSync(
		context.Background(),
		event.Event[fakeOrder]{},
	); !errors.Is(
		err,
		sentinel,
	) {
		t.Fatalf(
			"PublishSync = %v, want %v",
			err,
			sentinel,
		)
	}
}

func TestErrBus_SucceedsForEverythingElse(
	t *testing.T,
) {
	b := &ErrBus[fakeOrder]{Err: errors.New("down")}
	ctx := context.Background()
	if _, err := b.Subscribe(
		"OrderCreated",
		nil,
	); err != nil {
		t.Fatalf(
			"Subscribe = %v, want nil",
			err,
		)
	}

	if err := b.Unsubscribe("x"); err != nil {
		t.Fatalf(
			"Unsubscribe = %v, want nil",
			err,
		)
	}

	if err := b.Close(ctx); err != nil {
		t.Fatalf(
			"Close = %v, want nil",
			err,
		)
	}

	b.WaitForHandlers()
}
