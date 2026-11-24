package dispatcher_test

import (
	"context"
	"testing"

	"github.com/char2cs/asynx/v2/internal/mocks"
	"github.com/char2cs/asynx/v2/internal/processor/internal/dispatcher"
	"github.com/char2cs/asynx/v2/models/dispatch"
	dispatchermodel "github.com/char2cs/asynx/v2/models/dispatcher"
	"github.com/char2cs/asynx/v2/models/event"
)

func TestDispatcher_EnqueueTracksTheTicketAndCloses(
	t *testing.T,
) {
	d := dispatcher.Dispatcher(dispatchermodel.Config[mocks.Order]{})
	defer d.Close(context.Background())

	ticket, err := d.Enqueue(
		context.Background(),
		dispatch.Request[mocks.Order]{
			Event: event.Event[mocks.Order]{
				AggregateID: "agg1",
				Version:     1,
			},
		},
	)
	if err != nil {
		t.Fatalf(
			"Enqueue: %v",
			err,
		)
	}

	<-ticket.Final
	got, ok := d.TicketFor(
		"agg1",
		1,
	)
	if ok {
		t.Fatalf(
			"TicketFor after completion = %v, want none",
			got,
		)
	}
}
