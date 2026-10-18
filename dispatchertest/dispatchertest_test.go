package dispatchertest

import (
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/bus/dispatcher"
	"github.com/char2cs/asynx/models"
)

const brokenPatience = 2 * time.Second

func defaultFactory(
	cfg models.DispatcherConfig[Aggregate],
) (models.Dispatcher[Aggregate], error) {
	return dispatcher.New(cfg), nil
}

func brokenBy(
	wrap func(
		models.Dispatcher[Aggregate],
	) models.Dispatcher[Aggregate],
) models.DispatcherFactory[Aggregate] {
	return func(
		cfg models.DispatcherConfig[Aggregate],
	) (models.Dispatcher[Aggregate], error) {
		inner, err := defaultFactory(cfg)
		return wrap(inner), err
	}
}

func failures(
	h harness,
) map[string]bool {
	t := &tally{failed: map[string]bool{}}
	for name, check := range scenarios() {
		t.wg.Add(1)
		go t.run(name, check, h)
	}
	t.wg.Wait()
	return t.failed
}

func failedNames(
	failed map[string]bool,
) []string {
	names := slices.Collect(maps.Keys(failed))
	names = slices.DeleteFunc(names, func(
		name string,
	) bool {
		return !failed[name]
	})
	slices.Sort(names)
	return names
}

func runGroup(
	t *testing.T,
	group map[string]scenario,
) {
	h := harness{factory: defaultFactory, patience: patience}
	for _, name := range slices.Sorted(maps.Keys(group)) {
		t.Run(name, h.subtest(group[name]))
	}
}

func TestDispatcherDefault(t *testing.T) {
	Run(t, defaultFactory)
}

func TestFailures_TheDefaultFailsNoScenarioUnderTheRecorder(t *testing.T) {
	failed := failures(harness{factory: defaultFactory, patience: patience})
	if names := failedNames(failed); len(names) != 0 {
		t.Fatalf("the default failed %v under the recorder", names)
	}
	if len(failed) != len(scenarios()) {
		t.Fatalf("ran %d scenarios, want %d", len(failed), len(scenarios()))
	}
}

func TestSuite_FlagsADispatcherThatDeliversNothing(t *testing.T) {
	failed := failures(harness{factory: hollow, patience: brokenPatience})
	if got, total := len(failedNames(failed)), len(failed); got < total/2 {
		t.Fatalf("only %d of %d scenarios failed a dispatcher that delivers nothing: %v", got, total, failedNames(failed))
	}
}

func TestSuite_FlagsADispatcherThatParksEverything(t *testing.T) {
	failed := failures(harness{factory: parkingEverything, patience: brokenPatience})
	if got, total := len(failedNames(failed)), len(failed); got < total/2 {
		t.Fatalf("only %d of %d scenarios failed a dispatcher that parks everything: %v", got, total, failedNames(failed))
	}
}

func TestSuite_FlagsADispatcherThatRefusesEverything(t *testing.T) {
	failed := failures(harness{factory: refusing, patience: brokenPatience})
	if got, total := len(failedNames(failed)), len(failed); got < total*3/4 {
		t.Fatalf("only %d of %d scenarios failed a dispatcher that refuses every request: %v", got, total, failedNames(failed))
	}
}

func TestSuite_FlagsEveryBrokenDispatcher(t *testing.T) {
	cases := map[string]brokenCase{
		"reorders an aggregate's events": {reorder, []string{
			"TestDispatch_OrderingGuarantee",
			"TestDispatch_MultipleAggregatesOrdered",
			"TestClose_DrainsAllEvents",
		}},
		"acks before the handlers succeed": {ackEarly, []string{
			"TestEnqueue_AckRunsAfterHandlersFinish",
			"TestDurable_DoneClosesAfterTheFirstAttemptEvenWhenItFails",
			"TestDurable_CloseInterruptsRetriesAndLeavesTheDeliveryUnacked",
			"TestShutdownDuringRetry_NeverDeliversALaterEventPastTheFailingOne",
		}},
		"ignores the generation in ResumeUndelivered": {ignoreGeneration, []string{
			"TestResumeUndelivered_LeavesAParkImposedAgainSinceItWasListed",
		}},
		"queues durable events while parked": {queueWhileParked, []string{
			"TestPark_OfAnAggregateWithoutAQueueStillBlocksItsDurableEvents",
			"TestTicket_AParkedDurableEventSettlesAndEndsWithDeliveryParked",
			"TestTicket_UnrunSaysWhyNoHandlerRan",
			"TestUnrun_RemembersEventsLeftUndeliveredByAPark",
		}},
		"never reports ErrDeliveryParked": {hideParks, []string{
			"TestExhaustedAttempts_ParkTheAggregateAndLeaveOthersAlone",
			"TestUnrun_RemembersEventsLeftUndeliveredByAPark",
			"TestTicket_UnrunSaysWhyNoHandlerRan",
			"TestParkUndelivered_RemembersTheNamedVersionsUntilTheParkLifts",
		}},
		"drops Close's drain": {skipDrain, []string{
			"TestClose_DrainsJobsQueuedBehindASlowHandler",
			"TestClose_CalledAgainStillWaitsForTheWorkers",
		}},
		"dead-letters tombstones": {deadLetterTombstones, []string{
			"TestEnqueueForget_IsNeverDeadLettered",
		}},
	}
	for name, c := range cases {
		t.Run(name, c.check)
	}
}
