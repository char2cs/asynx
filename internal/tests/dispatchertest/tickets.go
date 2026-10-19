package dispatchertest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/char2cs/asynx/models"
)

func ticketScenarios() map[string]scenario {
	return map[string]scenario{
		"TestTicketFor_FindsAnEventWhileItsDeliveryIsUnderWay":           ticketForFindsAnEventUnderWay,
		"TestTicketFor_FindsAnEventQueuedBehindARunningOne":              ticketForFindsAQueuedEvent,
		"TestTicketFor_FindsAnEventWhileItRetries":                       ticketForFindsAnEventWhileItRetries,
		"TestTicketFor_ForgetsEventsLeftBehindByAPark":                   ticketForForgetsParkedEvents,
		"TestTicketFor_ForgetsQueuedEventsWhenTheyAreParked":             ticketForForgetsQueuedParkedEvents,
		"TestUnrun_RemembersEventsLeftUndeliveredByAPark":                unrunRemembersParkedEvents,
		"TestUnrun_RemembersQueuedEventsThatParkingTookOutOfMemory":      unrunRemembersQueuedParkedEvents,
		"TestUnrun_DoesNotCountAnEventWhoseHandlersRanAndFailed":         unrunIgnoresEventsWhoseHandlersRan,
		"TestTicket_UnrunSaysWhyNoHandlerRan":                            ticketUnrunSaysWhy,
		"TestTicket_AParkedDurableEventSettlesAndEndsWithDeliveryParked": ticketOfAParkedEvent,
	}
}

func gatedBus(
	entered chan struct{},
	release <-chan struct{},
) *bus {
	return &bus{fn: func(
		context.Context,
		models.Event[Aggregate],
	) error {
		entered <- struct{}{}
		<-release
		return nil
	}}
}

func ticketForFindsAnEventUnderWay(
	t testing.TB,
	h harness,
) {
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	d := h.build(t, config(gatedBus(entered, release)))
	t.Cleanup(func() { closeOnce(release) })
	if _, found := d.TicketFor("a", 1); found {
		t.Fatal("TicketFor found an event that was never queued")
	}

	enqueued := mustEnqueue(t, d, live(event("a", 1)))
	h.wait(t, entered, "the handler to start")
	ticket, found := d.TicketFor("a", 1)
	sameTicket := ticket.Settled == enqueued.Settled && ticket.Final == enqueued.Final
	if !found || !sameTicket {
		t.Fatalf("TicketFor = found %v; want the ticket Enqueue returned while its handler runs", found)
	}
	_, otherVersion := d.TicketFor("a", 2)
	_, otherAggregate := d.TicketFor("b", 1)
	if otherVersion || otherAggregate {
		t.Fatal("TicketFor matched another version or aggregate")
	}
	closeOnce(release)
	h.wait(t, ticket.Final, "the delivery")
	h.awaitForgotten(t, d, "a", 1, "TicketFor to forget an event whose delivery is over")
}

func ticketForFindsAQueuedEvent(
	t testing.TB,
	h harness,
) {
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	d := h.build(t, config(gatedBus(entered, release)))
	t.Cleanup(func() { closeOnce(release) })

	mustEnqueue(t, d, live(event("a", 1)))
	h.wait(t, entered, "v1 to start")
	queued := mustEnqueue(t, d, durable(event("a", 2), acknowledge))
	ticket, found := d.TicketFor("a", 2)
	sameTicket := ticket.Settled == queued.Settled && ticket.Final == queued.Final
	if !found || !sameTicket {
		t.Fatalf("TicketFor(a, 2) = found %v; want the ticket of the event queued behind a running one", found)
	}
	closeOnce(release)
	h.wait(t, queued.Final, "the queued event")
}

func ticketForFindsAnEventWhileItRetries(
	t testing.TB,
	h harness,
) {
	cfg := config(&bus{}, subscriber(fail))
	cfg.RetryInitial, cfg.RetryMax = time.Hour, time.Hour
	d := h.build(t, cfg)

	enqueued := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, enqueued.Settled, "the first attempt")
	if _, found := d.TicketFor("a", 1); !found {
		t.Fatal("TicketFor lost an event that is retrying")
	}
	d.Interrupt()
	h.wait(t, enqueued.Final, "the interrupted delivery")
	h.awaitForgotten(t, d, "a", 1, "TicketFor to forget an event whose delivery gave up")
}

func ticketForForgetsParkedEvents(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}, subscriber(succeed)))
	d.Park("a", errImposed)
	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, tk.Final, "the parked event")
	h.awaitForgotten(t, d, "a", 1, "TicketFor to forget an event that was never queued because its aggregate is parked")
}

func ticketForForgetsQueuedParkedEvents(
	t testing.TB,
	h harness,
) {
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	d := h.build(t, config(gatedBus(entered, release)))
	t.Cleanup(func() { closeOnce(release) })

	mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, entered, "v1 to start")
	queued := mustEnqueue(t, d, durable(event("a", 2), acknowledge))
	d.Park("a", errImposed)
	h.wait(t, queued.Final, "the queued event to leave memory")
	h.awaitForgotten(t, d, "a", 2, "TicketFor to forget a queued event that parking took out of memory")
}

func unrunRemembersParkedEvents(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}, subscriber(succeed)))
	d.Park("a", errImposed)
	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, tk.Final, "the parked event")

	if err := d.Unrun("a", 1); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("Unrun(a, 1) = %v, want ErrDeliveryParked", err)
	}
	if d.Unrun("a", 2) != nil || d.Unrun("b", 1) != nil {
		t.Fatalf("Unrun(a, 2) = %v, Unrun(b, 1) = %v; want nil for events nobody enqueued", d.Unrun("a", 2), d.Unrun("b", 1))
	}
	resume(d, "a")
	if err := d.Unrun("a", 1); err != nil {
		t.Fatalf("Unrun after the park lifted = %v, want nil", err)
	}
}

func unrunRemembersQueuedParkedEvents(
	t testing.TB,
	h harness,
) {
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	d := h.build(t, config(gatedBus(entered, release)))
	t.Cleanup(func() { closeOnce(release) })

	first := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, entered, "v1 to start")
	queued := mustEnqueue(t, d, durable(event("a", 2), acknowledge))
	d.Park("a", errImposed)
	h.wait(t, queued.Final, "the queued event to leave memory")
	closeOnce(release)
	h.wait(t, first.Final, "v1")

	if err := d.Unrun("a", 2); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("Unrun(a, 2) = %v, want ErrDeliveryParked", err)
	}
	if err := d.Unrun("a", 1); err != nil {
		t.Fatalf("Unrun of the event whose handlers ran = %v, want nil", err)
	}
}

func unrunIgnoresEventsWhoseHandlersRan(
	t testing.TB,
	h harness,
) {
	cfg := config(&bus{}, subscriber(fail))
	cfg.MaxAttempts = 1
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, tk.Final, "the exhausted delivery")
	h.waitFor(t, "the aggregate to park", func() bool { return len(parked(d)) == 1 })
	if err := d.Unrun("a", 1); err != nil {
		t.Fatalf("Unrun = %v, want nil: the handler ran", err)
	}
	if err := tk.Unrun(); err != nil {
		t.Fatalf("ticket Unrun = %v, want nil: the handler ran", err)
	}
}

func ticketUnrunSaysWhy(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}, subscriber(succeed)))
	ran := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, ran.Final, "the delivered event")
	if err := ran.Unrun(); err != nil {
		t.Fatalf("Unrun = %v for a delivered event", err)
	}
	d.Park("a", errImposed)
	parkedTicket := mustEnqueue(t, d, durable(event("a", 2), acknowledge))
	h.wait(t, parkedTicket.Settled, "the parked event to settle")
	if err := parkedTicket.Unrun(); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("Unrun = %v, want ErrDeliveryParked", err)
	}
}

func ticketOfAParkedEvent(
	t testing.TB,
	h harness,
) {
	var runs journal
	b := &bus{}
	d := h.build(t, config(b, subscriber(func(
		_ context.Context,
		e models.Event[Aggregate],
	) error {
		runs.add(e)
		return nil
	})))
	d.Park("a", errImposed)

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	if err := h.final(t, tk, "the parked event"); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
	if !errors.Is(tk.Unrun(), models.ErrDeliveryParked) || len(runs.list()) != 0 {
		t.Fatalf("Unrun = %v, durable runs = %v; want ErrDeliveryParked and none", tk.Unrun(), runs.list())
	}
	liveTicket := mustEnqueue(t, d, live(event("a", 2)))
	if err := h.final(t, liveTicket, "a live event of the parked aggregate"); err != nil || len(b.published()) != 1 {
		t.Fatalf("live final = %v, published %d; want live events to flow while parked", err, len(b.published()))
	}
}
