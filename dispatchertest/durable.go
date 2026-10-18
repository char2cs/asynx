package dispatchertest

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx/models"
)

func durableScenarios() map[string]scenario {
	return map[string]scenario{
		"TestEnqueue_AckRunsAfterHandlersFinish":                                enqueueAckRunsAfterHandlersFinish,
		"TestEnqueue_AckErrorIsReported":                                        enqueueAckErrorIsReported,
		"TestEnqueue_DurableDeliveryStillAcksWhenBusRejectsEvent":               durableStillAcksWhenBusRejects,
		"TestDurable_FailedHandlerRetriesAndHoldsBackLaterEventsOfTheAggregate": durableFailedHandlerRetriesAndHoldsBack,
		"TestDurable_DoneClosesAfterTheFirstAttemptEvenWhenItFails":             durableSettledAfterTheFirstAttempt,
		"TestDurable_PanicIsAFailureThatRetriesAndReachesThePanicHandler":       durablePanicIsAFailure,
		"TestDurable_APanickingPanicHandlerCannotKillTheWorker":                 durablePanickingPanicHandler,
		"TestDurable_OnlyFailedHandlersAreRetried":                              durableOnlyFailedHandlersAreRetried,
		"TestDurable_NonDurableJobsSkipDurableSubscribers":                      durableNonDurableJobsSkipDurableSubscribers,
		"TestDurable_DeliveryWithNoMatchingSubscriberIsStillAcked":              durableNoMatchingSubscriberIsStillAcked,
		"TestDurable_HandlerFinishingBeforeItsTimeoutReportsNothing":            durableHandlerWithinTimeoutReportsNothing,
		"TestDurable_ATimedOutHandlerThatThenFailsIsRetried":                    durableTimedOutThenFailedIsRetried,
		"TestDurable_FailuresNameTheSubscriberAndDeadLetterGetsThemAll":         durableFailuresNameTheSubscriber,
		"TestDurable_ATimeoutNamesItsSubscriber":                                durableTimeoutNamesItsSubscriber,
		"TestBusErrorDoesNotSkipTheDurableDelivery":                             busErrorDoesNotSkipTheDurableDelivery,
		"TestBusPanicDoesNotSkipTheDurableDelivery":                             busPanicDoesNotSkipTheDurableDelivery,
		"TestDispatch_WorkerSurvivesPanic":                                      dispatchWorkerSurvivesPanic,
		"TestHandle_DispatcherPanicIsReportedNotSwallowed":                      handleBusPanicIsReported,
		"TestDeliveryErrorHandlerPanicDoesNotReleaseTheHold":                    deliveryErrorHandlerPanicKeepsTheHold,
		"TestPublishErrorHandlerPanicDoesNotCrashTheWorker":                     publishErrorHandlerPanicKeepsTheWorker,
		"TestEnqueueRedelivery_GoesToTheDurableSubscribersOnly":                 redeliveryGoesToTheDurableSubscribersOnly,
		"TestTombstone_ReachesEveryDurableSubscriber":                           tombstoneReachesEveryDurableSubscriber,
		"TestTombstone_WithANilAckRunsTheHandlersAndCallsNoAck":                 tombstoneWithANilAck,
		"TestRedelivery_OfAForgetMarkerWithANilAckRunsTheHandlers":              redeliveredMarkerWithANilAck,
		"TestDeadLetter_TheDeadLetteredEventIsAckedAndItsFinalClosesNil":        deadLetteredEventIsAcked,
	}
}

func enqueueAckRunsAfterHandlersFinish(
	t testing.TB,
	h harness,
) {
	var handled, acked, ackedEarly atomic.Bool
	gate := make(chan struct{})
	b := &bus{fn: func(
		context.Context,
		models.Event[Aggregate],
	) error {
		<-gate
		handled.Store(true)
		return nil
	}}
	d := h.build(t, config(b))
	ack := func(
		context.Context,
	) error {
		ackedEarly.Store(!handled.Load())
		acked.Store(true)
		return nil
	}

	tk := mustEnqueue(t, d, durable(event("a", 1), ack))
	ranWhileBlocked := acked.Load()
	close(gate)
	h.wait(t, tk.Settled, "the delivery to settle")
	if ranWhileBlocked || ackedEarly.Load() {
		t.Fatal("ack ran before the handlers finished")
	}
	if err := h.final(t, tk, "the delivery"); err != nil || !acked.Load() {
		t.Fatalf("final = %v, acked = %v; want nil and acked", err, acked.Load())
	}
}

func enqueueAckErrorIsReported(
	t testing.TB,
	h harness,
) {
	ackErr := errors.New("ack failed")
	reported := make(chan error, 4)
	cfg := config(&bus{})
	cfg.OnPublishError = func(
		_ context.Context,
		_ models.Event[Aggregate],
		err error,
	) {
		sendIf(true, reported, err)
	}
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), failAck(ackErr)))
	if got := receive(t, h.patience, reported, "the ack failure report"); !errors.Is(got, ackErr) {
		t.Fatalf("reported %v, want the ack error", got)
	}
	if err := h.final(t, tk, "the delivery"); !errors.Is(err, ackErr) {
		t.Fatalf("final = %v, want the ack error", err)
	}
}

func durableStillAcksWhenBusRejects(
	t testing.TB,
	h harness,
) {
	var acked atomic.Bool
	var reported atomic.Int32
	b := &bus{fn: func(
		context.Context,
		models.Event[Aggregate],
	) error {
		return models.ErrBusClosed
	}}
	cfg := config(b)
	cfg.OnPublishError = func(
		context.Context,
		models.Event[Aggregate],
		error,
	) {
		reported.Add(1)
	}
	d := h.build(t, cfg)
	ack := func(
		context.Context,
	) error {
		acked.Store(true)
		return nil
	}

	tk := mustEnqueue(t, d, durable(event("a", 1), ack))
	if err := h.final(t, tk, "the delivery"); err != nil {
		t.Fatalf("final = %v, want nil", err)
	}
	if !acked.Load() || reported.Load() != 1 {
		t.Fatalf("acked = %v, reported = %d; want acked and one report", acked.Load(), reported.Load())
	}
}

func durableFailedHandlerRetriesAndHoldsBack(
	t testing.TB,
	h harness,
) {
	var order, attempts journal
	var failuresLeft, acks atomic.Int32
	failuresLeft.Store(3)
	sub := subscriber(func(
		_ context.Context,
		e models.Event[Aggregate],
	) error {
		failing := e.AggregateID == "a" && e.Version == 1 && failuresLeft.Add(-1) >= 0
		order.note(label(e, failing))
		return errorIf(failing, errDown)
	})
	cfg := config(&bus{}, sub)
	cfg.OnDeliveryError = func(
		_ context.Context,
		_ models.Event[Aggregate],
		_ error,
		attempt int,
	) {
		attempts.note(strconv.Itoa(attempt))
	}
	d := h.build(t, cfg)
	ack := func(
		context.Context,
	) error {
		acks.Add(1)
		return nil
	}

	mustEnqueue(t, d, durable(event("a", 1), ack))
	mustEnqueue(t, d, durable(event("a", 2), ack))
	b1 := mustEnqueue(t, d, durable(event("b", 1), ack))
	h.wait(t, b1.Final, "aggregate b, which a's failure must not hold back")
	h.waitFor(t, "all three deliveries to be acked", func() bool { return acks.Load() == 3 })

	want := []string{"a/1-fail", "a/1-fail", "a/1-fail", "a/1", "a/2"}
	if got := without(order.list(), "b/"); !slices.Equal(got, want) {
		t.Fatalf("deliveries of a = %v, want %v", got, want)
	}
	if got := attempts.list(); !slices.Equal(got, []string{"1", "2", "3"}) {
		t.Fatalf("delivery error attempts = %v, want [1 2 3]", got)
	}
}

func label(
	e models.Event[Aggregate],
	failing bool,
) string {
	name := e.AggregateID + "/" + strconv.FormatInt(e.Version, 10)
	if !failing {
		return name
	}
	return name + "-fail"
}

func durableSettledAfterTheFirstAttempt(
	t testing.TB,
	h harness,
) {
	release := make(chan struct{})
	var acked atomic.Bool
	d := h.build(t, config(&bus{}, subscriber(func(
		context.Context,
		models.Event[Aggregate],
	) error {
		return errorIf(!isClosed(release), errDown)
	})))
	ack := func(
		context.Context,
	) error {
		acked.Store(true)
		return nil
	}

	tk := mustEnqueue(t, d, durable(event("a", 1), ack))
	h.wait(t, tk.Settled, "Settled after the first, failed, attempt")
	if acked.Load() {
		close(release)
		t.Fatal("acked a delivery whose handler failed")
	}
	close(release)
	h.waitFor(t, "the ack after the retry succeeded", acked.Load)
}

func durablePanicIsAFailure(
	t testing.TB,
	h harness,
) {
	var attempts, panics atomic.Int32
	var acked atomic.Bool
	reported := make(chan error, 8)
	cfg := config(&bus{}, subscriber(func(
		context.Context,
		models.Event[Aggregate],
	) error {
		panicIf(attempts.Add(1) == 1, "boom")
		return nil
	}))
	cfg.OnPanic = func(
		context.Context,
		models.Event[Aggregate],
		any,
	) {
		panics.Add(1)
	}
	cfg.OnDeliveryError = func(
		_ context.Context,
		_ models.Event[Aggregate],
		err error,
		_ int,
	) {
		sendIf(true, reported, err)
	}
	d := h.build(t, cfg)
	ack := func(
		context.Context,
	) error {
		acked.Store(true)
		return nil
	}

	mustEnqueue(t, d, durable(event("a", 1), ack))
	h.waitFor(t, "the ack after a panic then a success", acked.Load)
	got := receive(t, h.patience, reported, "the panic report")
	if panics.Load() != 1 || !errors.Is(got, models.ErrHandlerPanic) {
		t.Fatalf("panic handler calls = %d, reported %v; want 1 and ErrHandlerPanic", panics.Load(), got)
	}
}

func durablePanickingPanicHandler(
	t testing.TB,
	h harness,
) {
	var attempts atomic.Int32
	var acked atomic.Bool
	cfg := config(&bus{}, subscriber(func(
		context.Context,
		models.Event[Aggregate],
	) error {
		panicIf(attempts.Add(1) == 1, "boom")
		return nil
	}))
	cfg.OnPanic = func(
		context.Context,
		models.Event[Aggregate],
		any,
	) {
		panic("handler of handlers")
	}
	d := h.build(t, cfg)
	ack := func(
		context.Context,
	) error {
		acked.Store(true)
		return nil
	}

	mustEnqueue(t, d, durable(event("a", 1), ack))
	h.waitFor(t, "recovery from a panicking panic handler", acked.Load)
}

func durableOnlyFailedHandlersAreRetried(
	t testing.TB,
	h harness,
) {
	var goodRuns, flakyRuns, strayRuns atomic.Int32
	var acked atomic.Bool
	good := subscriber(func(
		context.Context,
		models.Event[Aggregate],
	) error {
		goodRuns.Add(1)
		return nil
	})
	flaky := subscriber(func(
		context.Context,
		models.Event[Aggregate],
	) error {
		return errorIf(flakyRuns.Add(1) < 3, errDown)
	})
	stray := subscriber(func(
		context.Context,
		models.Event[Aggregate],
	) error {
		strayRuns.Add(1)
		return nil
	})
	stray.Match = never
	d := h.build(t, config(&bus{}, good, flaky, stray))
	ack := func(
		context.Context,
	) error {
		acked.Store(true)
		return nil
	}

	mustEnqueue(t, d, durable(event("a", 1), ack))
	h.waitFor(t, "the ack", acked.Load)
	if goodRuns.Load() != 1 || flakyRuns.Load() != 3 || strayRuns.Load() != 0 {
		t.Fatalf("good ran %d, flaky %d, non-matching %d; want 1, 3, 0", goodRuns.Load(), flakyRuns.Load(), strayRuns.Load())
	}
}

func durableNonDurableJobsSkipDurableSubscribers(
	t testing.TB,
	h harness,
) {
	var runs atomic.Int32
	d := h.build(t, config(&bus{}, subscriber(func(
		context.Context,
		models.Event[Aggregate],
	) error {
		runs.Add(1)
		return nil
	})))

	tk := mustEnqueue(t, d, live(event("a", 1)))
	if err := h.final(t, tk, "the live event"); err != nil || runs.Load() != 0 {
		t.Fatalf("final = %v, durable runs = %d; want nil and 0", err, runs.Load())
	}
}

func durableNoMatchingSubscriberIsStillAcked(
	t testing.TB,
	h harness,
) {
	var acked atomic.Bool
	d := h.build(t, config(&bus{}))
	ack := func(
		context.Context,
	) error {
		acked.Store(true)
		return nil
	}

	tk := mustEnqueue(t, d, durable(event("a", 1), ack))
	if err := h.final(t, tk, "the delivery"); err != nil || !acked.Load() {
		t.Fatalf("final = %v, acked = %v; want nil and acked", err, acked.Load())
	}
}

func durableHandlerWithinTimeoutReportsNothing(
	t testing.TB,
	h harness,
) {
	var reports atomic.Int32
	sub := subscriber(succeed)
	sub.Timeout = time.Minute
	cfg := config(&bus{}, sub)
	cfg.OnDeliveryError = func(
		context.Context,
		models.Event[Aggregate],
		error,
		int,
	) {
		reports.Add(1)
	}
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	if err := h.final(t, tk, "the delivery"); err != nil || reports.Load() != 0 {
		t.Fatalf("final = %v, reports = %d; want nil and 0", err, reports.Load())
	}
}

func durableTimedOutThenFailedIsRetried(
	t testing.TB,
	h harness,
) {
	var attempts atomic.Int32
	timedOut := make(chan struct{}, 4)
	sub := subscriber(func(
		context.Context,
		models.Event[Aggregate],
	) error {
		first := attempts.Add(1) == 1
		holdIf(first, timedOut)
		return errorIf(first, errDown)
	})
	sub.Timeout = 5 * time.Millisecond
	cfg := config(&bus{}, sub)
	cfg.OnDeliveryError = func(
		_ context.Context,
		_ models.Event[Aggregate],
		err error,
		_ int,
	) {
		sendIf(errors.Is(err, models.ErrHandlerTimeout), timedOut, struct{}{})
	}
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	if err := h.final(t, tk, "the delivery"); err != nil || attempts.Load() != 2 {
		t.Fatalf("final = %v, attempts = %d; want nil and 2", err, attempts.Load())
	}
}

func durableFailuresNameTheSubscriber(
	t testing.TB,
	h harness,
) {
	billing := errors.New("billing down")
	search := errors.New("search down")
	reported := make(chan error, 16)
	deadLettered := make(chan error, 1)
	first := models.DurableSub[Aggregate]{Name: "OrderUpdated#1", Match: always, Handler: func(
		context.Context,
		models.Event[Aggregate],
	) error {
		return billing
	}}
	second := models.DurableSub[Aggregate]{Name: "OrderUpdated#2", Match: always, Handler: func(
		context.Context,
		models.Event[Aggregate],
	) error {
		return search
	}}
	cfg := config(&bus{}, first, second)
	cfg.MaxAttempts = 2
	cfg.OnDeliveryError = func(
		_ context.Context,
		_ models.Event[Aggregate],
		err error,
		_ int,
	) {
		sendIf(true, reported, err)
	}
	cfg.DeadLetter = func(
		_ context.Context,
		_ models.Event[Aggregate],
		err error,
	) error {
		sendIf(true, deadLettered, err)
		return nil
	}
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	if err := h.final(t, tk, "the dead-lettered delivery"); err != nil {
		t.Fatalf("final = %v, want nil", err)
	}
	got := receive(t, h.patience, deadLettered, "the dead-letter handler")
	if !errors.Is(got, billing) || !errors.Is(got, search) {
		t.Fatalf("dead-letter handler learned of %q, want both failures", got)
	}
	if names := subscriberNames(got); !slices.Equal(names, []string{"OrderUpdated#1", "OrderUpdated#2"}) {
		t.Fatalf("subscribers named in the dead-letter error = %v", names)
	}
	firstReport := receive(t, h.patience, reported, "the first failure report")
	if names := subscriberNames(firstReport); !slices.Equal(names, []string{"OrderUpdated#1"}) {
		t.Fatalf("first delivery error = %v, want it to name OrderUpdated#1", firstReport)
	}
}

func durableTimeoutNamesItsSubscriber(
	t testing.TB,
	h harness,
) {
	release := make(chan struct{})
	reported := make(chan error, 4)
	sub := models.DurableSub[Aggregate]{Name: "slow#1", Match: always, Timeout: time.Millisecond, Handler: func(
		context.Context,
		models.Event[Aggregate],
	) error {
		<-release
		return nil
	}}
	cfg := config(&bus{}, sub)
	cfg.OnDeliveryError = func(
		_ context.Context,
		_ models.Event[Aggregate],
		err error,
		_ int,
	) {
		sendIf(true, reported, err)
	}
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	got := receive(t, h.patience, reported, "the timeout report")
	close(release)
	h.wait(t, tk.Final, "the delivery")
	if names := subscriberNames(got); !slices.Equal(names, []string{"slow#1"}) || !errors.Is(got, models.ErrHandlerTimeout) {
		t.Fatalf("timeout reported as %q, want ErrHandlerTimeout from slow#1", got)
	}
}

func busErrorDoesNotSkipTheDurableDelivery(
	t testing.TB,
	h harness,
) {
	var rec journal
	var acked sync.Map
	b := &bus{fn: func(
		context.Context,
		models.Event[Aggregate],
	) error {
		return errDown
	}}
	d := h.build(t, config(b, subscriber(func(
		_ context.Context,
		e models.Event[Aggregate],
	) error {
		rec.add(e)
		return nil
	})))

	mustEnqueue(t, d, durable(event("a", 1), ackInto(&acked, 1)))
	mustEnqueue(t, d, durable(event("a", 2), ackInto(&acked, 2)))
	last := mustEnqueue(t, d, durable(event("a", 3), ackInto(&acked, 3)))
	if err := h.final(t, last, "the last delivery"); err != nil {
		t.Fatalf("final = %v", err)
	}
	if got := rec.list(); !slices.Equal(got, []string{"a/1", "a/2", "a/3"}) {
		t.Fatalf("durable deliveries = %v, want a/1 a/2 a/3 in order", got)
	}
}

func busPanicDoesNotSkipTheDurableDelivery(
	t testing.TB,
	h harness,
) {
	var acked atomic.Bool
	var reported atomic.Int32
	b := &bus{fn: func(
		context.Context,
		models.Event[Aggregate],
	) error {
		panic("bus exploded")
	}}
	cfg := config(b, subscriber(succeed))
	cfg.OnPublishError = func(
		context.Context,
		models.Event[Aggregate],
		error,
	) {
		reported.Add(1)
	}
	d := h.build(t, cfg)
	ack := func(
		context.Context,
	) error {
		acked.Store(true)
		return nil
	}

	tk := mustEnqueue(t, d, durable(event("a", 1), ack))
	if err := h.final(t, tk, "the delivery"); err != nil {
		t.Fatalf("final = %v", err)
	}
	if !acked.Load() || reported.Load() != 1 {
		t.Fatalf("acked = %v, reported = %d; want the panic reported and the delivery completed", acked.Load(), reported.Load())
	}
}

func dispatchWorkerSurvivesPanic(
	t testing.TB,
	h harness,
) {
	var calls atomic.Int32
	b := &bus{fn: func(
		context.Context,
		models.Event[Aggregate],
	) error {
		panicIf(calls.Add(1) == 1, "boom")
		return nil
	}}
	d := h.build(t, config(b))

	first := mustEnqueue(t, d, live(event("agg-1", 1)))
	h.wait(t, first.Settled, "the panicking event")
	second := mustEnqueue(t, d, live(event("agg-1", 2)))
	h.wait(t, second.Final, "the event after the panic")
	if calls.Load() != 2 {
		t.Fatalf("PublishSync calls = %d, want 2", calls.Load())
	}
}

func handleBusPanicIsReported(
	t testing.TB,
	h harness,
) {
	reported := make(chan error, 4)
	b := &bus{fn: func(
		context.Context,
		models.Event[Aggregate],
	) error {
		panic("bus exploded")
	}}
	cfg := config(b)
	cfg.OnPublishError = func(
		_ context.Context,
		_ models.Event[Aggregate],
		err error,
	) {
		sendIf(true, reported, err)
	}
	d := h.build(t, cfg)

	mustEnqueue(t, d, live(event("a", 1)))
	if got := receive(t, h.patience, reported, "the bus panic report"); got == nil {
		t.Fatal("a panic in the bus was reported as a nil error")
	}
	next := mustEnqueue(t, d, live(event("a", 2)))
	h.wait(t, next.Final, "the worker to keep delivering")
}

func deliveryErrorHandlerPanicKeepsTheHold(
	t testing.TB,
	h harness,
) {
	var rec journal
	var failures atomic.Int32
	failures.Store(2)
	cfg := config(&bus{}, subscriber(func(
		_ context.Context,
		e models.Event[Aggregate],
	) error {
		rec.add(e)
		return errorIf(e.Version == 1 && failures.Add(-1) >= 0, errDown)
	}))
	cfg.OnDeliveryError = func(
		context.Context,
		models.Event[Aggregate],
		error,
		int,
	) {
		panic("boom")
	}
	d := h.build(t, cfg)

	mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	last := mustEnqueue(t, d, durable(event("a", 2), acknowledge))
	if err := h.final(t, last, "v2"); err != nil {
		t.Fatalf("v2 = %v", err)
	}
	if got := rec.list(); !slices.Equal(got, []string{"a/1", "a/1", "a/1", "a/2"}) {
		t.Fatalf("delivered %v: a panicking error handler released the hold", got)
	}
}

func publishErrorHandlerPanicKeepsTheWorker(
	t testing.TB,
	h harness,
) {
	b := &bus{fn: func(
		context.Context,
		models.Event[Aggregate],
	) error {
		return errDown
	}}
	cfg := config(b)
	cfg.OnPublishError = func(
		context.Context,
		models.Event[Aggregate],
		error,
	) {
		panic("boom")
	}
	d := h.build(t, cfg)

	first := mustEnqueue(t, d, live(event("a", 1)))
	h.wait(t, first.Settled, "the first event")
	second := mustEnqueue(t, d, live(event("a", 2)))
	h.wait(t, second.Settled, "the worker to survive")
}

func redeliveryGoesToTheDurableSubscribersOnly(
	t testing.TB,
	h harness,
) {
	b := &bus{}
	var seen journal
	var acked atomic.Int32
	sub := subscriber(func(
		_ context.Context,
		e models.Event[Aggregate],
	) error {
		seen.add(e)
		return nil
	})
	sub.Match = onlyTestEvents
	d := h.build(t, config(b, sub))
	ack := func(
		context.Context,
	) error {
		acked.Add(1)
		return nil
	}

	h.final(t, mustEnqueue(t, d, redelivery(event("a", 1), ack)), "the redelivery")
	h.final(t, mustEnqueue(t, d, redelivery(marker("a", 2), ack)), "the redelivered marker")
	if got := len(b.published()); got != 0 {
		t.Fatalf("live subscribers got %d redelivered events", got)
	}
	if got := seen.list(); !slices.Equal(got, []string{"a/1", "a/2"}) || acked.Load() != 2 {
		t.Fatalf("durable subscriber saw %v, acked %d; want the event and the marker, both acked", got, acked.Load())
	}
}

func tombstoneReachesEveryDurableSubscriber(
	t testing.TB,
	h harness,
) {
	b := &bus{}
	var matching, other journal
	first := subscriber(func(
		_ context.Context,
		e models.Event[Aggregate],
	) error {
		matching.add(e)
		return nil
	})
	first.Match = onlyTestEvents
	second := subscriber(func(
		_ context.Context,
		e models.Event[Aggregate],
	) error {
		other.add(e)
		return nil
	})
	second.Match = never
	d := h.build(t, config(b, first, second))

	if err := h.final(t, mustEnqueue(t, d, tombstone(marker("a", 1), acknowledge)), "the tombstone"); err != nil {
		t.Fatalf("tombstone final = %v", err)
	}
	if !slices.Equal(matching.list(), []string{"a/1"}) || !slices.Equal(other.list(), []string{"a/1"}) {
		t.Fatalf("subscribers saw %v and %v, want the tombstone in both", matching.list(), other.list())
	}
	if got := len(b.published()); got != 1 {
		t.Fatalf("live subscribers got %d events, want the tombstone", got)
	}
}

func tombstoneWithANilAck(
	t testing.TB,
	h harness,
) {
	var seen journal
	sub := subscriber(func(
		_ context.Context,
		e models.Event[Aggregate],
	) error {
		seen.add(e)
		return nil
	})
	sub.Match = never
	d := h.build(t, config(&bus{}, sub))

	if err := h.final(t, mustEnqueue(t, d, tombstone(marker("a", 1), nil)), "the tombstone"); err != nil {
		t.Fatalf("tombstone final = %v", err)
	}
	if got := seen.list(); !slices.Equal(got, []string{"a/1"}) {
		t.Fatalf("durable subscriber saw %v, want the tombstone", got)
	}
}

func redeliveredMarkerWithANilAck(
	t testing.TB,
	h harness,
) {
	b := &bus{}
	var seen journal
	sub := subscriber(func(
		_ context.Context,
		e models.Event[Aggregate],
	) error {
		seen.add(e)
		return nil
	})
	sub.Match = never
	d := h.build(t, config(b, sub))

	if err := h.final(t, mustEnqueue(t, d, redelivery(marker("a", 1), nil)), "the redelivered marker"); err != nil {
		t.Fatalf("marker final = %v", err)
	}
	if got := seen.list(); !slices.Equal(got, []string{"a/1"}) || len(b.published()) != 0 {
		t.Fatalf("durable subscriber saw %v, live %d; want the marker durably only", got, len(b.published()))
	}
}

func deadLetteredEventIsAcked(
	t testing.TB,
	h harness,
) {
	var acked sync.Map
	var dead atomic.Int32
	cfg := config(&bus{}, subscriber(fail))
	cfg.MaxAttempts = 2
	cfg.DeadLetter = func(
		context.Context,
		models.Event[Aggregate],
		error,
	) error {
		dead.Add(1)
		return nil
	}
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), ackInto(&acked, 1)))
	if err := h.final(t, tk, "the dead-lettered delivery"); err != nil {
		t.Fatalf("final = %v, want nil", err)
	}
	if _, ok := acked.Load(int64(1)); !ok || dead.Load() != 1 || len(parked(d)) != 0 {
		t.Fatalf("acked = %v, dead letters = %d, parked = %v; want acked once, unparked", ok, dead.Load(), parked(d))
	}
}
