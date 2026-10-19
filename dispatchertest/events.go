package dispatchertest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/char2cs/asynx/models"
)

const eventName = "test.event"

func event(
	aggregateID string,
	version int64,
) models.Event[Aggregate] {
	return models.Event[Aggregate]{
		ID:          fmt.Sprintf("%s-v%d", aggregateID, version),
		AggregateID: aggregateID,
		EventName:   eventName,
		Version:     version,
		OccurredAt:  time.Now(),
	}
}

func marker(
	aggregateID string,
	version int64,
) models.Event[Aggregate] {
	e := event(aggregateID, version)
	e.EventName = models.ForgetEventName
	return e
}

func live(
	e models.Event[Aggregate],
) models.DispatchRequest[Aggregate] {
	return models.DispatchRequest[Aggregate]{Event: e}
}

func durable(
	e models.Event[Aggregate],
	ack func(
		context.Context,
	) error,
) models.DispatchRequest[Aggregate] {
	return models.DispatchRequest[Aggregate]{Event: e, Ack: ack}
}

func redelivery(
	e models.Event[Aggregate],
	ack func(
		context.Context,
	) error,
) models.DispatchRequest[Aggregate] {
	return models.DispatchRequest[Aggregate]{Event: e, Ack: ack, Kind: models.DispatchRedelivery}
}

func tombstone(
	e models.Event[Aggregate],
	ack func(
		context.Context,
	) error,
) models.DispatchRequest[Aggregate] {
	return models.DispatchRequest[Aggregate]{Event: e, Ack: ack, Kind: models.DispatchTombstone}
}

func mustEnqueue(
	t testing.TB,
	d models.Dispatcher[Aggregate],
	r models.DispatchRequest[Aggregate],
) models.Ticket {
	t.Helper()
	tk, err := d.Enqueue(context.Background(), r)
	if err != nil {
		t.Fatalf("Enqueue %s/%d: %v", r.Event.AggregateID, r.Event.Version, err)
	}
	return tk
}

func acknowledge(
	context.Context,
) error {
	return nil
}

func ackInto(
	acked *sync.Map,
	version int64,
) func(
	context.Context,
) error {
	return func(
		context.Context,
	) error {
		acked.Store(version, true)
		return nil
	}
}

func failAck(
	err error,
) func(
	context.Context,
) error {
	return func(
		context.Context,
	) error {
		return err
	}
}

func config(
	b models.Bus[Aggregate],
	subs ...models.DurableSub[Aggregate],
) models.DispatcherConfig[Aggregate] {
	return models.DispatcherConfig[Aggregate]{
		Bus:          b,
		DurableSubs:  subs,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
		MaxAttempts:  20,
	}
}

func waitIdle(
	d models.Dispatcher[Aggregate],
	done chan<- struct{},
) {
	d.WaitIdle()
	close(done)
}

func waitIdleAfter(
	d models.Dispatcher[Aggregate],
	started chan<- struct{},
	done chan<- struct{},
) {
	close(started)
	waitIdle(d, done)
}

func enqueueAll(
	d models.Dispatcher[Aggregate],
	aggregateID string,
	n int64,
	refused *journal,
	done chan<- struct{},
) {
	for v := int64(1); v <= n; v++ {
		_, err := d.Enqueue(context.Background(), live(event(aggregateID, v)))
		noteError(refused, err)
	}
	close(done)
}

func enqueueOne(
	wg *sync.WaitGroup,
	d models.Dispatcher[Aggregate],
	r models.DispatchRequest[Aggregate],
	refused *journal,
) {
	defer wg.Done()
	_, err := d.Enqueue(context.Background(), r)
	noteError(refused, err)
}

func enqueueRound(
	t testing.TB,
	d models.Dispatcher[Aggregate],
	version int64,
	aggregateIDs ...string,
) {
	t.Helper()
	for _, id := range aggregateIDs {
		mustEnqueue(t, d, live(event(id, version)))
	}
}

func noteError(
	j *journal,
	err error,
) {
	if err == nil {
		return
	}
	j.note(err.Error())
}
