package dispatchertest

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/char2cs/asynx/models"
)

func TestEvents_RequestsCarryTheirKindAndAck(t *testing.T) {
	requests := []models.DispatchRequest[Aggregate]{
		live(event("a", 1)),
		durable(event("a", 2), acknowledge),
		redelivery(event("a", 3), nil),
		tombstone(marker("a", 4), acknowledge),
	}
	kinds := []models.DispatchKind{}
	for _, r := range requests {
		kinds = append(kinds, r.Kind)
	}
	want := []models.DispatchKind{models.DispatchEvent, models.DispatchEvent, models.DispatchRedelivery, models.DispatchTombstone}
	if !slices.Equal(kinds, want) {
		t.Fatalf("kinds = %v, want %v", kinds, want)
	}
	if requests[0].Ack != nil || requests[3].Event.EventName != models.ForgetEventName {
		t.Fatalf("want a live request without ack and a tombstone named %q", models.ForgetEventName)
	}
}

func TestEvents_MustEnqueueFailsTheScenarioOnARefusal(t *testing.T) {
	d, _ := refusing(config(nil))
	if !fails(func(
		t testing.TB,
	) {
		mustEnqueue(t, d, live(event("a", 1)))
	}) {
		t.Fatal("a refused Enqueue did not fail the scenario")
	}
}

func TestEvents_AcksRecordOrFail(t *testing.T) {
	var acked sync.Map
	errs := errors.Join(acknowledge(context.Background()), ackInto(&acked, 7)(context.Background()))
	_, stored := acked.Load(int64(7))
	failsAsTold := errors.Is(failAck(errDown)(context.Background()), errDown)
	acksBroken := errs != nil || !stored
	if acksBroken || !failsAsTold {
		t.Fatalf("errors %v, stored %v; want acks that succeed, record, or fail as told", errs, stored)
	}
}

func TestEvents_EnqueueHelpersNoteRefusals(t *testing.T) {
	d, _ := refusing(config(nil))
	var refused journal
	done := make(chan struct{})
	enqueueAll(d, "a", 2, &refused, done)
	var wg sync.WaitGroup
	wg.Add(1)
	enqueueOne(&wg, d, live(event("b", 1)), &refused)
	wg.Wait()
	if got := len(refused.list()); got != 3 || !isClosed(done) {
		t.Fatalf("noted %d refusals, done closed %v; want 3 and closed", got, isClosed(done))
	}
}
