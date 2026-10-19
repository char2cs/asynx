package dispatchertest

import (
	"context"
	"errors"
	"testing"

	"github.com/char2cs/asynx/models"
)

func TestBus_RecordsEveryPublishAndReturnsTheCallbackError(t *testing.T) {
	b := &bus{fn: func(
		context.Context,
		models.Event[Aggregate],
	) error {
		return errDown
	}}
	if err := b.PublishSync(context.Background(), event("a", 1)); !errors.Is(err, errDown) {
		t.Fatalf("PublishSync = %v, want the callback error", err)
	}
	if got := versionsOf(b.published()); len(got) != 1 || got[0] != 1 {
		t.Fatalf("published = %v, want [1]", got)
	}
}

func TestBus_TheRestOfTheBusIsANoOp(t *testing.T) {
	b := &bus{}
	id, subscribeErr := b.Subscribe("*", nil)
	errs := errors.Join(
		b.Publish(context.Background(), event("a", 1)),
		subscribeErr,
		b.Unsubscribe(id),
		b.Close(context.Background()),
	)
	b.WaitForHandlers()
	rejected := errs != nil || id != ""
	if rejected || len(b.published()) != 0 {
		t.Fatalf("errors %v, id %q, published %d; want a silent no-op", errs, id, len(b.published()))
	}
}
