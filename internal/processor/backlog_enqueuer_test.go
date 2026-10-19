package processor

import (
	"context"
	"errors"
	"testing"

	"github.com/char2cs/asynx/internal/mocks"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

func newEnqueuer(
	d *mocks.Dispatcher[int],
	versions ...int64,
) *backlogEnqueuer[int] {
	vanished := make(map[int64]struct{}, len(versions))
	for _, v := range versions {
		vanished[v] = struct{}{}
	}
	return &backlogEnqueuer[int]{
		dispatcher: d,
		store:      store.New(),
		aggregate:  "a",
		vanished:   vanished,
	}
}

func TestBacklogEnqueuer_EnqueuesOnlyTheWantedVersionsAsRedeliveries(t *testing.T) {
	d := mocks.NewDispatcher[int]()
	e := newEnqueuer(d, 2)

	e.enqueue(context.Background(), asynxmd.Event[int]{AggregateID: "a", Version: 1})
	e.enqueue(context.Background(), asynxmd.Event[int]{AggregateID: "a", Version: 2})

	requests := d.Requests()
	wrongRequests := len(requests) != 1 || requests[0].Event.Version != 2 || requests[0].Kind != asynxmd.DispatchRedelivery
	if wrongRequests {
		t.Errorf("requests = %+v, want one redelivery of version 2", requests)
	}
	if len(e.vanished) != 0 || e.err != nil {
		t.Errorf("vanished = %v, err = %v, want none", e.vanished, e.err)
	}
}

func TestBacklogEnqueuer_StopsAfterTheFirstEnqueueFailure(t *testing.T) {
	sentinel := errors.New("enqueue failed")
	d := mocks.NewDispatcher[int]()
	d.FailEnqueue(sentinel)
	e := newEnqueuer(d, 1, 2)

	e.enqueue(context.Background(), asynxmd.Event[int]{AggregateID: "a", Version: 1})
	e.enqueue(context.Background(), asynxmd.Event[int]{AggregateID: "a", Version: 2})

	if !errors.Is(e.err, sentinel) || len(e.vanished) != 2 {
		t.Errorf("err = %v, vanished = %v, want the failure and both versions kept", e.err, e.vanished)
	}
	if calls := d.Calls(); len(calls) != 1 {
		t.Errorf("calls = %v, want a single Enqueue", calls)
	}
}

func TestBacklogEnqueuer_AckAcknowledgesTheDeliveryInTheStore(t *testing.T) {
	d := mocks.NewDispatcher[int]()
	e := newEnqueuer(d, 1)

	if err := e.ack(1)(context.Background()); err != nil {
		t.Errorf("ack = %v, want nil", err)
	}
}
