package dispatchertest

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/char2cs/asynx/models"
)

var (
	errImposed = errors.New("dispatchertest: parked by the test")
	errLater   = errors.New("dispatchertest: a later cause")
	errAck     = errors.New("dispatchertest: ack failed")
)

func inspectScenarios() map[string]scenario {
	return map[string]scenario{
		"TestInspect_UnknownAggregateIsTheZeroStatus":                  inspectUnknownAggregate,
		"TestInspect_ReportsParkGenerationCauseAndUndelivered":         inspectReportsThePark,
		"TestInspect_ParkedAgainKeepsTheGenerationAndTheFirstCause":    inspectKeepsTheFirstCause,
		"TestInspect_ALaterCauseReplacesANilOne":                       inspectReplacesANilCause,
		"TestInspect_GenerationChangesWhenAParkIsImposedAgain":         inspectNewGeneration,
		"TestInspect_IsClearAfterTheParkLifts":                         inspectClearAfterTheLift,
		"TestInspect_GiveUpCauseNamesTheLastHandlerError":              inspectGiveUpCause,
		"TestInspect_AckFailureCauseIsTheAckError":                     inspectAckFailureCause,
		"TestInspect_InterruptCauseIsDispatcherClosed":                 inspectInterruptCause,
		"TestInspect_ADurableEventRefusedByTheParkIsListedUndelivered": inspectListsARefusedEvent,
	}
}

func inspectUnknownAggregate(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}, subscriber(succeed)))
	h.final(t, mustEnqueue(t, d, durable(event("busy", 1), acknowledge)), "a delivered event")
	if got := d.Inspect("ghost"); !reflect.DeepEqual(got, models.DeliveryStatus{}) {
		t.Fatalf("Inspect(ghost) = %+v, want the zero status", got)
	}
	if got := d.Inspect("busy"); !reflect.DeepEqual(got, models.DeliveryStatus{}) {
		t.Fatalf("Inspect(busy) = %+v, want the zero status for an aggregate that is not parked", got)
	}
}

func inspectReportsThePark(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	d.Park("a", errImposed, 5, 3)
	got := d.Inspect("a")
	if !got.Parked || got.Generation == 0 || got.Generation != d.Parks()["a"] {
		t.Fatalf("Inspect = %+v, want parked with the generation Parks lists (%d)", got, d.Parks()["a"])
	}
	if !errors.Is(got.Cause, errImposed) {
		t.Fatalf("Cause = %v, want %v", got.Cause, errImposed)
	}
	if !slices.Equal(got.Undelivered, []int64{3, 5}) {
		t.Fatalf("Undelivered = %v, want [3 5] ascending", got.Undelivered)
	}
}

func inspectKeepsTheFirstCause(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	d.Park("a", errImposed, 3)
	first := d.Inspect("a").Generation
	d.Park("a", errLater, 1)
	d.Park("a", nil)
	got := d.Inspect("a")
	if !got.Parked || got.Generation != first {
		t.Fatalf("Inspect = %+v, want still parked with generation %d", got, first)
	}
	if !errors.Is(got.Cause, errImposed) || errors.Is(got.Cause, errLater) {
		t.Fatalf("Cause = %v, want the first one, %v", got.Cause, errImposed)
	}
	if !slices.Equal(got.Undelivered, []int64{1, 3}) {
		t.Fatalf("Undelivered = %v, want the merged [1 3]", got.Undelivered)
	}
}

func inspectReplacesANilCause(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	d.Park("a", nil)
	if got := d.Inspect("a"); !got.Parked || got.Cause != nil {
		t.Fatalf("Inspect = %+v, want parked with no cause", got)
	}
	d.Park("a", errLater)
	if got := d.Inspect("a"); !errors.Is(got.Cause, errLater) {
		t.Fatalf("Cause = %v, want %v replacing the nil one", got.Cause, errLater)
	}
}

func inspectNewGeneration(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	d.Park("a", errImposed)
	first := d.Inspect("a").Generation
	resume(d, "a")
	d.Park("a", errLater)
	got := d.Inspect("a")
	if !got.Parked || got.Generation == first {
		t.Fatalf("Inspect = %+v, want parked with a generation other than %d", got, first)
	}
	if got.Generation != d.Parks()["a"] {
		t.Fatalf("Generation = %d, want the one Parks lists, %d", got.Generation, d.Parks()["a"])
	}
	if !errors.Is(got.Cause, errLater) {
		t.Fatalf("Cause = %v, want the new park's %v", got.Cause, errLater)
	}
}

func inspectClearAfterTheLift(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	d.Park("a", errImposed, 3)
	resume(d, "a")
	if got := d.Inspect("a"); !reflect.DeepEqual(got, models.DeliveryStatus{}) {
		t.Fatalf("Inspect after the lift = %+v, want the zero status", got)
	}
	d.Park("a", nil)
	got := d.Inspect("a")
	if !got.Parked || got.Cause != nil {
		t.Fatalf("Inspect = %+v, want a fresh park with no cause", got)
	}
	if len(got.Undelivered) != 0 {
		t.Fatalf("Undelivered = %v, want nothing after the lift", got.Undelivered)
	}
}

func inspectGiveUpCause(
	t testing.TB,
	h harness,
) {
	cfg := config(&bus{}, subscriber(func(
		context.Context,
		models.Event[Aggregate],
	) error {
		return errPoison
	}))
	cfg.MaxAttempts = 3
	d := h.build(t, cfg)

	h.final(t, mustEnqueue(t, d, durable(event("a", 1), acknowledge)), "the delivery to give up")
	got := d.Inspect("a")
	if !got.Parked || got.Generation != d.Parks()["a"] {
		t.Fatalf("Inspect = %+v, want parked with the generation Parks lists", got)
	}
	if !errors.Is(got.Cause, models.ErrDeliveryParked) || !errors.Is(got.Cause, errPoison) {
		t.Fatalf("Cause = %v, want ErrDeliveryParked wrapping %v", got.Cause, errPoison)
	}
}

func inspectAckFailureCause(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}, subscriber(succeed)))
	h.final(t, mustEnqueue(t, d, durable(event("a", 1), failAck(errAck))), "the failed ack")
	got := d.Inspect("a")
	if !got.Parked || !errors.Is(got.Cause, errAck) {
		t.Fatalf("Inspect = %+v, want parked with %v", got, errAck)
	}
}

func inspectInterruptCause(
	t testing.TB,
	h harness,
) {
	attempted := make(chan struct{}, 8)
	cfg := config(&bus{}, subscriber(func(
		context.Context,
		models.Event[Aggregate],
	) error {
		sendIf(true, attempted, struct{}{})
		return errPoison
	}))
	cfg.RetryInitial, cfg.RetryMax = time.Hour, time.Hour
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, attempted, "the first attempt")
	d.Interrupt()
	h.wait(t, tk.Final, "the interrupted delivery")
	got := d.Inspect("a")
	if !got.Parked || !errors.Is(got.Cause, models.ErrDispatcherClosed) {
		t.Fatalf("Inspect = %+v, want parked with ErrDispatcherClosed", got)
	}
}

func inspectListsARefusedEvent(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}, subscriber(succeed)))
	d.Park("a", errImposed)
	h.final(t, mustEnqueue(t, d, durable(event("a", 4), acknowledge)), "the refused event")
	if got := d.Inspect("a"); !got.Parked || !slices.Equal(got.Undelivered, []int64{4}) {
		t.Fatalf("Inspect = %+v, want parked with [4] undelivered", got)
	}
}
