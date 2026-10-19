package dispatchertest

import (
	"context"
	"sync"
	"testing"

	"github.com/char2cs/asynx/models"
)

type ticketHoardingDispatcher struct {
	models.Dispatcher[Aggregate]
	mu      *sync.Mutex
	tickets map[string]map[int64]models.Ticket
}

func hoardTickets(
	inner models.Dispatcher[Aggregate],
) models.Dispatcher[Aggregate] {
	return ticketHoardingDispatcher{
		Dispatcher: inner,
		mu:         &sync.Mutex{},
		tickets:    map[string]map[int64]models.Ticket{},
	}
}

func (d ticketHoardingDispatcher) Enqueue(
	ctx context.Context,
	r models.DispatchRequest[Aggregate],
) (models.Ticket, error) {
	tk, err := d.Dispatcher.Enqueue(ctx, r)
	if err != nil {
		return tk, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	id := r.Event.AggregateID
	if d.tickets[id] == nil {
		d.tickets[id] = map[int64]models.Ticket{}
	}
	d.tickets[id][r.Event.Version] = tk
	return tk, nil
}

func (d ticketHoardingDispatcher) TicketFor(
	aggregateID string,
	version int64,
) (models.Ticket, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	tk, found := d.tickets[aggregateID][version]
	return tk, found
}

func TestSuite_FlagsADispatcherWhoseTicketForNeverForgets(t *testing.T) {
	brokenCase{brokenBy(hoardTickets), []string{
		"TestTicketFor_FindsAnEventWhileItsDeliveryIsUnderWay",
		"TestTicketFor_FindsAnEventWhileItRetries",
		"TestTicketFor_ForgetsEventsLeftBehindByAPark",
		"TestTicketFor_ForgetsQueuedEventsWhenTheyAreParked",
	}}.check(t)
}
