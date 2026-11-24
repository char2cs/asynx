package options_test

import (
	"context"
	"errors"
	"testing"

	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/eventstore/options"
	"github.com/char2cs/asynx/internal/mocks"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

var errSnapshotPut = errors.New("snapshot put failed")

func upcastedStore(
	t *testing.T,
	handler func(context.Context, asynxmd.Event[mocks.Order], error),
) eventstore.EventStore[mocks.Order] {
	t.Helper()

	events := store.New()
	snapshots := store.NewSnapshots()
	v1 := eventstore.New[mocks.Order](
		events,
		snapshots,
		nil,
		1,
		nil,
	)
	_, err := v1.Write(
		context.Background(),
		mocks.UpdateOrderCmd{
			ID:       "agg1",
			NewState: mocks.Order{ID: "1", Status: "Pending"},
		},
	)
	if err != nil {
		t.Fatalf(
			"Write: %v",
			err,
		)
	}

	snapshots.SetError(
		"agg1",
		errSnapshotPut,
	)
	identity := func(
		_ context.Context,
		_ string,
		payload []byte,
	) ([]byte, error) {
		return payload,
			nil
	}
	return eventstore.New[mocks.Order](
		events,
		snapshots,
		map[int]asynxmd.Upcaster{1: identity},
		2,
		nil,
		options.WithSnapshotErrorHandler(handler),
	)
}

func TestWithSnapshotErrorHandler_ReportsAFailedAutoSnapshotOnRead(
	t *testing.T,
) {
	var calls []asynxmd.Event[mocks.Order]
	var errs []error
	handler := func(
		_ context.Context,
		event asynxmd.Event[mocks.Order],
		err error,
	) {
		calls = append(
			calls,
			event,
		)
		errs = append(
			errs,
			err,
		)
	}
	es := upcastedStore(
		t,
		handler,
	)

	got, err := es.Get(
		context.Background(),
		"agg1",
	)
	if err != nil ||
		got.Status != "Pending" {
		t.Fatalf(
			"Get = %+v, %v; want Pending, nil",
			got,
			err,
		)
	}

	if len(calls) != 1 ||
		!errors.Is(
			errs[0],
			errSnapshotPut,
		) {
		t.Fatalf(
			"handler calls = %d, errs = %v; want 1 call with the put error",
			len(calls),
			errs,
		)
	}

	if calls[0].AggregateID != "agg1" ||
		calls[0].Version != 1 ||
		calls[0].Aggregate.Status != "Pending" {
		t.Errorf(
			"event = %+v, want agg1 v1 Pending",
			calls[0],
		)
	}
}

func TestWithSnapshotErrorHandler_NilHandlerKeepsDroppingTheFailure(
	t *testing.T,
) {
	es := upcastedStore(
		t,
		nil,
	)

	got, err := es.Get(
		context.Background(),
		"agg1",
	)
	if err != nil ||
		got.Status != "Pending" {
		t.Fatalf(
			"Get = %+v, %v; want Pending, nil",
			got,
			err,
		)
	}
}

func TestWithSnapshotErrorHandler_PanickingHandlerDoesNotBreakPreload(
	t *testing.T,
) {
	handler := func(
		context.Context,
		asynxmd.Event[mocks.Order],
		error,
	) {
		panic("handler blew up")
	}
	es := upcastedStore(
		t,
		handler,
	)

	err := es.Preload(
		context.Background(),
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Preload: %v",
			err,
		)
	}
}
