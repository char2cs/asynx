package asynx_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

func TestAsynx_InspectDelivery_ShowsWhyAnAggregateIsParked(t *testing.T) {
	ctx := context.Background()
	readModelDown := errors.New("read model down")
	parked := make(chan error, 8)
	inst := build(t, eight(store.New()).
		WithDeliveryRetry(time.Millisecond, 2*time.Millisecond).
		WithDeliveryMaxAttempts(3).
		WithDeliveryErrorHandler(func(
			_ context.Context,
			_ models.Event[mocks.Order],
			err error,
			_ int,
		) {
			if errors.Is(err, models.ErrDeliveryParked) {
				parked <- err
			}
		}).
		WithSubscriber("OrderUpdated", func(
			_ context.Context,
			e models.Event[mocks.Order],
		) error {
			if e.AggregateID == "bad" {
				return readModelDown
			}
			return nil
		}))

	if _, err := inst.Send(ctx, upd("bad", 1)); err != nil {
		t.Fatal(err)
	}
	within(t, parked, "park report")
	if _, err := inst.SendWait(ctx, upd("bad", 2)); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("SendWait behind the park = %v, want ErrDeliveryParked", err)
	}

	got := inst.InspectDelivery("bad")
	if !got.Parked || got.Generation == 0 {
		t.Fatalf("InspectDelivery = %+v, want parked with a generation", got)
	}
	if !errors.Is(got.Cause, models.ErrDeliveryParked) || !errors.Is(got.Cause, readModelDown) {
		t.Fatalf("Cause = %v, want ErrDeliveryParked wrapping %v", got.Cause, readModelDown)
	}
	if !slices.Contains(got.Undelivered, 2) {
		t.Fatalf("Undelivered = %v, want v2, which no handler took", got.Undelivered)
	}
	if other := inst.InspectDelivery("other"); !reflect.DeepEqual(other, models.DeliveryStatus{}) {
		t.Fatalf("InspectDelivery of an unknown aggregate = %+v, want the zero status", other)
	}
}
