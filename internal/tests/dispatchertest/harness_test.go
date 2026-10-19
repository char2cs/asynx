package dispatchertest

import (
	"testing"
	"time"

	"github.com/char2cs/asynx/models"
)

func failingFactory(
	models.DispatcherConfig[Aggregate],
) (models.Dispatcher[Aggregate], error) {
	return nil, errRefused
}

func TestHarness_BuildFailsTheScenarioWhenTheFactoryFails(t *testing.T) {
	h := harness{factory: failingFactory, patience: time.Second}
	if !fails(func(
		t testing.TB,
	) {
		h.build(t, config(&bus{}))
	}) {
		t.Fatal("a factory error did not fail the scenario")
	}
}

func TestHarness_ACloseErrorAtCleanupFailsTheScenario(t *testing.T) {
	h := harness{factory: refusing, patience: time.Second}
	if !fails(func(
		t testing.TB,
	) {
		h.build(t, config(&bus{}))
	}) {
		t.Fatal("a Close error at cleanup did not fail the scenario")
	}
}

func neverTrue() bool {
	return false
}

func TestHarness_BoundedWaitsFailInsteadOfHanging(t *testing.T) {
	h := harness{factory: defaultFactory, patience: time.Millisecond}
	never := make(chan struct{})
	waitFails := fails(func(
		t testing.TB,
	) {
		h.wait(t, never, "nothing")
	})
	waitForFails := fails(func(
		t testing.TB,
	) {
		h.waitFor(t, "nothing", neverTrue)
	})
	finalFails := fails(func(
		t testing.TB,
	) {
		h.final(t, models.Ticket{Final: never}, "nothing")
	})
	waitsFail := waitFails && waitForFails
	if !waitsFail || !finalFails {
		t.Fatalf("wait %v, waitFor %v, final %v: each must fail on what never happens", waitFails, waitForFails, finalFails)
	}
}

func TestHarness_IdleAndEnqueueBoundedReturnOnTheDefault(t *testing.T) {
	h := harness{factory: defaultFactory, patience: patience}
	d := h.build(t, config(&bus{}))
	tk := h.enqueueBounded(t, d, live(event("a", 1)))
	h.idle(t, d)
	if err := h.final(t, tk, "the live event"); err != nil {
		t.Fatalf("final = %v", err)
	}
}
