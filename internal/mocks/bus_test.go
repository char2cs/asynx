package mocks

import (
	"context"
	"errors"
	"testing"

	"github.com/char2cs/asynx/models"
)

func TestBus_SucceedsSilently(t *testing.T) {
	var b Bus[Order]
	ctx := context.Background()
	if err := b.Publish(ctx, models.Event[Order]{}); err != nil {
		t.Fatalf("Publish = %v, want nil", err)
	}
	if err := b.PublishSync(ctx, models.Event[Order]{}); err != nil {
		t.Fatalf("PublishSync = %v, want nil", err)
	}
	if err := b.Unsubscribe("x"); err != nil {
		t.Fatalf("Unsubscribe = %v, want nil", err)
	}
	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}
	b.WaitForHandlers()
}

func TestBus_SubscribeReturnsAnID(t *testing.T) {
	var b Bus[Order]
	id, err := b.Subscribe("OrderCreated", nil)
	if err != nil || id != "id" {
		t.Fatalf("Subscribe = (%q, %v), want (\"id\", nil)", id, err)
	}
}

func TestErrBus_ReturnsItsErrorForPublish(t *testing.T) {
	sentinel := errors.New("down")
	b := &ErrBus[Order]{Err: sentinel}
	if err := b.Publish(context.Background(), models.Event[Order]{}); !errors.Is(err, sentinel) {
		t.Fatalf("Publish = %v, want %v", err, sentinel)
	}
	if err := b.PublishSync(context.Background(), models.Event[Order]{}); !errors.Is(err, sentinel) {
		t.Fatalf("PublishSync = %v, want %v", err, sentinel)
	}
}

func TestErrBus_SucceedsForEverythingElse(t *testing.T) {
	b := &ErrBus[Order]{Err: errors.New("down")}
	ctx := context.Background()
	if _, err := b.Subscribe("OrderCreated", nil); err != nil {
		t.Fatalf("Subscribe = %v, want nil", err)
	}
	if err := b.Unsubscribe("x"); err != nil {
		t.Fatalf("Unsubscribe = %v, want nil", err)
	}
	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}
	b.WaitForHandlers()
}
