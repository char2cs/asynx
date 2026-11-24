package dispatcher_test

import (
	"context"
	"testing"

	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher"
	asynxmd "github.com/char2cs/asynx/models"
)

func TestDispatcher_EnqueueTracksTheTicketAndCloses(
	t *testing.T,
) {
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[mocks.Order]{})
	defer d.Close(context.Background())

	ticket, err := d.Enqueue(
		context.Background(),
		asynxmd.DispatchRequest[mocks.Order]{
			Event: asynxmd.Event[mocks.Order]{
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
