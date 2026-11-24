package mockbuses

import (
	"context"
	"github.com/char2cs/asynx/models/event"
	"testing"
)

func TestBus_SucceedsSilently(
	t *testing.T,
) {
	var b Bus[fakeOrder]
	ctx := context.Background()
	if err := b.Publish(
		ctx,
		event.Event[fakeOrder]{},
	); err != nil {
		t.Fatalf(
			"Publish = %v, want nil",
			err,
		)
	}

	if err := b.PublishSync(
		ctx,
		event.Event[fakeOrder]{},
	); err != nil {
		t.Fatalf(
			"PublishSync = %v, want nil",
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

func TestBus_SubscribeReturnsAnID(
	t *testing.T,
) {
	var b Bus[fakeOrder]
	id, err := b.Subscribe(
		"OrderCreated",
		nil,
	)
	if err != nil ||
		id != "id" {
		t.Fatalf(
			"Subscribe = (%q, %v), want (\"id\", nil)",
			id,
			err,
		)
	}
}
