package dispatchertest

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx/models"
)

func parkingScenarios() map[string]scenario {
	return map[string]scenario{
		"TestExhaustedAttempts_ParkTheAggregateAndLeaveOthersAlone":             exhaustedAttemptsPark,
		"TestResume_LiftsAParkAndDeliversAgain":                                 resumeLiftsAPark,
		"TestResume_ReportsWhetherItLiftedAPark":                                resumeReportsTheLift,
		"TestDeadLetter_AcceptingTheEventSkipsItAndLetsLaterEventsFlow":         deadLetterAcceptance,
		"TestDeadLetter_RefusalParksTheAggregate":                               deadLetterRefusal,
		"TestDeadLetter_AReturnedErrorParksTheAggregate":                        deadLetterReturnedError,
		"TestDeadLetter_TheAggregateIsParkedBeforeTheRefusalIsReported":         deadLetterRefusalParksBeforeReporting,
		"TestSupersede_LetsATombstoneWithAnAckThroughARetryingDelivery":         supersedeLetsATombstoneWithAnAckThrough,
		"TestSupersede_LetsTheTombstoneThroughAFailingDelivery":                 supersedeLetsTheTombstoneThrough,
		"TestSupersede_AfterThePopStillSkipsTheSupersededJob":                   supersedeAfterThePop,
		"TestSupersede_LeavesTheAggregateUnparkedWithNoRecord":                  supersedeLeavesNoRecord,
		"TestTombstone_FinalReportsADurableSubscriberThatKeepsFailing":          tombstoneFinalReportsAFailure,
		"TestEnqueueForget_IsNeverDeadLettered":                                 forgetIsNeverDeadLettered,
		"TestGiveUp_AfterASupersedeDoesNotParkTheAggregateAgain":                giveUpAfterASupersede,
		"TestInterrupt_AfterASupersedeDoesNotParkTheAggregate":                  interruptAfterASupersede,
		"TestPark_OfAnAggregateWithoutAQueueStillBlocksItsDurableEvents":        parkWithoutAQueue,
		"TestPark_KeepsLiveOnlyJobsQueuedAndDropsDurableOnes":                   parkKeepsLiveOnlyJobs,
		"TestPark_QueuedDurableJobsReportThemselvesUnrun":                       parkQueuedJobsReportUnrun,
		"TestPark_OnAParkedAggregateKeepsItsGenerationAndMergesTheVersions":     parkMergesIntoAPark,
		"TestPark_AfterALiftImposesANewGeneration":                              parkAfterALift,
		"TestParkUndelivered_RemembersTheNamedVersionsUntilTheParkLifts":        parkRemembersTheNamedVersions,
		"TestGiveUp_TheAggregateIsParkedBeforeTheErrorHandlerIsTold":            giveUpParksBeforeReporting,
		"TestAckFailure_TheAggregateIsParkedBeforeThePublishErrorHandlerIsTold": ackFailureParksBeforeReporting,
		"TestAckFailure_ParksTheAggregateSoALaterEventDoesNotOvertake":          ackFailureParks,
		"TestResumeUndelivered_ReturnsWhatTheParkLeftUndeliveredAndForgetsIt":   resumeUndeliveredReturnsTheRecord,
		"TestResumeUndelivered_LeavesAParkImposedAgainSinceItWasListed":         resumeUndeliveredLeavesANewerPark,
	}
}

func exhaustedAttemptsPark(
	t testing.TB,
	h harness,
) {
	var attempts, parkedReports atomic.Int32
	var rec journal
	var acked sync.Map
	var once sync.Once
	gate := make(chan struct{})
	cfg := config(&bus{}, subscriber(func(
		_ context.Context,
		e models.Event[Aggregate],
	) error {
		rec.add(e)
		poison := e.AggregateID == "a" && e.Version == 1
		holdIf(poison && firstTime(&once), gate)
		countIf(poison, &attempts)
		return errorIf(poison, errPoison)
	}))
	cfg.MaxAttempts = 4
	cfg.OnDeliveryError = func(
		_ context.Context,
		_ models.Event[Aggregate],
		err error,
		_ int,
	) {
		countIf(errors.Is(err, models.ErrDeliveryParked), &parkedReports)
	}
	d := h.build(t, cfg)
	t.Cleanup(func() { closeOnce(gate) })

	t1 := mustEnqueue(t, d, durable(event("a", 1), ackInto(&acked, 1)))
	t2 := mustEnqueue(t, d, durable(event("a", 2), ackInto(&acked, 2)))
	closeOnce(gate)
	if err := h.final(t, t1, "v1"); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("v1 final = %v, want ErrDeliveryParked", err)
	}
	if err := h.final(t, t2, "v2"); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("v2 final = %v, want ErrDeliveryParked", err)
	}
	if attempts.Load() != 4 || !slices.Equal(parked(d), []string{"a"}) {
		t.Fatalf("attempts = %d, parked = %v; want exactly the cap (4) and [a]", attempts.Load(), parked(d))
	}
	if slices.Contains(rec.list(), "a/2") {
		t.Fatal("v2 delivered although v1 was parked")
	}

	t3 := mustEnqueue(t, d, durable(event("a", 3), ackInto(&acked, 3)))
	if err := h.final(t, t3, "v3"); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("v3 final = %v, want ErrDeliveryParked", err)
	}
	tb := mustEnqueue(t, d, durable(event("b", 1), ackInto(&acked, 1)))
	if err := h.final(t, tb, "b"); err != nil {
		t.Fatalf("aggregate b held back: %v", err)
	}
	if parkedReports.Load() != 1 {
		t.Fatalf("parked reports = %d, want 1", parkedReports.Load())
	}
}

func resumeLiftsAPark(
	t testing.TB,
	h harness,
) {
	var healthy atomic.Bool
	var acked sync.Map
	cfg := config(&bus{}, subscriber(func(
		context.Context,
		models.Event[Aggregate],
	) error {
		return errorIf(!healthy.Load(), errDown)
	}))
	cfg.MaxAttempts = 2
	d := h.build(t, cfg)

	h.final(t, mustEnqueue(t, d, durable(event("a", 1), acknowledge)), "the parked delivery")
	healthy.Store(true)
	if !resume(d, "a") {
		t.Fatal("the park was not lifted")
	}
	tk := mustEnqueue(t, d, redelivery(event("a", 1), ackInto(&acked, 1)))
	if err := h.final(t, tk, "the redelivery"); err != nil {
		t.Fatalf("redelivery = %v", err)
	}
	if _, ok := acked.Load(int64(1)); !ok || len(parked(d)) != 0 {
		t.Fatalf("acked = %v, parked = %v; want the redelivery acked and nothing parked", ok, parked(d))
	}
}

func resumeReportsTheLift(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	if resume(d, "a") {
		t.Fatal("lifted a park on an aggregate that is not parked")
	}
	d.Park("a", errImposed)
	if !resume(d, "a") || resume(d, "a") {
		t.Fatal("ResumeUndelivered must report the lift once")
	}
}

func deadLetterAcceptance(
	t testing.TB,
	h harness,
) {
	var rec journal
	var dead atomic.Int64
	var acked sync.Map
	var once sync.Once
	gate := make(chan struct{})
	cfg := config(&bus{}, subscriber(func(
		_ context.Context,
		e models.Event[Aggregate],
	) error {
		rec.add(e)
		holdIf(e.Version == 1 && firstTime(&once), gate)
		return errorIf(e.Version == 1, errPoison)
	}))
	cfg.MaxAttempts = 3
	cfg.DeadLetter = func(
		_ context.Context,
		e models.Event[Aggregate],
		_ error,
	) error {
		dead.Store(e.Version)
		return nil
	}
	d := h.build(t, cfg)
	t.Cleanup(func() { closeOnce(gate) })

	t1 := mustEnqueue(t, d, durable(event("a", 1), ackInto(&acked, 1)))
	t2 := mustEnqueue(t, d, durable(event("a", 2), ackInto(&acked, 2)))
	closeOnce(gate)
	if err := h.final(t, t1, "v1"); err != nil {
		t.Fatalf("dead-lettered event final = %v, want nil", err)
	}
	if err := h.final(t, t2, "v2"); err != nil {
		t.Fatalf("v2 final = %v", err)
	}
	_, ok := acked.Load(int64(1))
	if dead.Load() != 1 || !ok || len(parked(d)) != 0 {
		t.Fatalf("dead letter saw v%d, v1 acked = %v, parked = %v; want v1, acked, nothing parked", dead.Load(), ok, parked(d))
	}
}

func deadLetterRefusal(
	t testing.TB,
	h harness,
) {
	cfg := config(&bus{}, subscriber(fail))
	cfg.MaxAttempts = 2
	cfg.DeadLetter = func(
		context.Context,
		models.Event[Aggregate],
		error,
	) error {
		panic("dead-letter store down")
	}
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	if err := h.final(t, tk, "the delivery"); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
	if !slices.Equal(parked(d), []string{"a"}) {
		t.Fatalf("parked = %v, want [a]", parked(d))
	}
}

func deadLetterReturnedError(
	t testing.TB,
	h harness,
) {
	var rec journal
	var acked sync.Map
	var dead atomic.Int32
	var once sync.Once
	gate := make(chan struct{})
	cfg := config(&bus{}, subscriber(func(
		_ context.Context,
		e models.Event[Aggregate],
	) error {
		rec.add(e)
		holdIf(e.Version == 1 && firstTime(&once), gate)
		return errorIf(e.Version == 1, errPoison)
	}))
	cfg.MaxAttempts = 2
	cfg.DeadLetter = func(
		context.Context,
		models.Event[Aggregate],
		error,
	) error {
		dead.Add(1)
		return errDown
	}
	d := h.build(t, cfg)
	t.Cleanup(func() { closeOnce(gate) })

	t1 := mustEnqueue(t, d, durable(event("a", 1), ackInto(&acked, 1)))
	t2 := mustEnqueue(t, d, durable(event("a", 2), ackInto(&acked, 2)))
	closeOnce(gate)
	if err := h.final(t, t1, "v1"); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("v1 final = %v, want ErrDeliveryParked", err)
	}
	if err := h.final(t, t2, "the queued v2"); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("v2 final = %v, want ErrDeliveryParked", err)
	}
	_, v1Acked := acked.Load(int64(1))
	if v1Acked || dead.Load() != 1 || !slices.Equal(parked(d), []string{"a"}) || slices.Contains(rec.list(), "a/2") {
		t.Fatalf("v1 acked %v, dead letters %d, parked %v, delivered %v; want unacked, 1, [a], no v2", v1Acked, dead.Load(), parked(d), rec.list())
	}
}

func deadLetterRefusalParksBeforeReporting(
	t testing.TB,
	h harness,
) {
	var d models.Dispatcher[Aggregate]
	parkedWhenTold := make(chan []string, 4)
	cfg := config(&bus{}, subscriber(fail))
	cfg.MaxAttempts = 1
	cfg.DeadLetter = func(
		context.Context,
		models.Event[Aggregate],
		error,
	) error {
		return errDown
	}
	cfg.OnDeliveryError = func(
		_ context.Context,
		_ models.Event[Aggregate],
		err error,
		_ int,
	) {
		sendIf(errors.Is(err, models.ErrDeliveryParked), parkedWhenTold, parked(d))
	}
	d = h.build(t, cfg)

	mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	if got := receive(t, h.patience, parkedWhenTold, "the park to be reported"); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("parked when the handler was told = %v, want [a]", got)
	}
}

func supersedeLetsATombstoneWithAnAckThrough(
	t testing.TB,
	h harness,
) {
	var acked sync.Map
	attempted := make(chan struct{}, 8)
	cfg := config(&bus{}, subscriber(func(
		_ context.Context,
		e models.Event[Aggregate],
	) error {
		poison := e.EventName == eventName
		sendIf(poison, attempted, struct{}{})
		return errorIf(poison, errPoison)
	}))
	cfg.RetryInitial, cfg.RetryMax = time.Hour, time.Hour
	d := h.build(t, cfg)

	retrying := mustEnqueue(t, d, durable(event("a", 1), ackInto(&acked, 1)))
	h.wait(t, attempted, "the first attempt")
	d.Supersede("a")
	tk := mustEnqueue(t, d, tombstone(marker("a", 2), ackInto(&acked, 2)))
	if err := h.final(t, tk, "the tombstone"); err != nil {
		t.Fatalf("tombstone final = %v, want nil", err)
	}
	h.wait(t, retrying.Final, "the superseded delivery")
	_, v1 := acked.Load(int64(1))
	_, tombstoneAcked := acked.Load(int64(2))
	if v1 || !tombstoneAcked || len(parked(d)) != 0 {
		t.Fatalf("v1 acked %v, tombstone acked %v, parked %v; want only the tombstone acked, unparked", v1, tombstoneAcked, parked(d))
	}
}

func supersedeLetsTheTombstoneThrough(
	t testing.TB,
	h harness,
) {
	var seen journal
	var acked sync.Map
	attempted := make(chan struct{}, 8)
	sub := subscriber(func(
		_ context.Context,
		e models.Event[Aggregate],
	) error {
		seen.add(e)
		poison := e.EventName == eventName
		sendIf(poison, attempted, struct{}{})
		return errorIf(poison, errPoison)
	})
	sub.Match = onlyTestEvents
	cfg := config(&bus{}, sub)
	cfg.RetryInitial, cfg.RetryMax = time.Hour, time.Hour
	d := h.build(t, cfg)

	mustEnqueue(t, d, durable(event("a", 1), ackInto(&acked, 1)))
	mustEnqueue(t, d, durable(event("a", 2), ackInto(&acked, 2)))
	h.wait(t, attempted, "the poison attempt")
	d.Supersede("a")
	tk := mustEnqueue(t, d, tombstone(marker("a", 3), nil))
	if err := h.final(t, tk, "the tombstone"); err != nil {
		t.Fatalf("tombstone final = %v", err)
	}
	if len(parked(d)) != 0 || !slices.Contains(seen.list(), "a/3") {
		t.Fatalf("parked = %v, seen = %v; want unparked and the tombstone seen", parked(d), seen.list())
	}
}

func supersedeAfterThePop(
	t testing.TB,
	h harness,
) {
	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { closeOnce(release) })
	var once sync.Once
	var oldRuns atomic.Int32
	b := &bus{fn: func(
		context.Context,
		models.Event[Aggregate],
	) error {
		holdFirst(&once, entered, release)
		return nil
	}}
	cfg := config(b, subscriber(func(
		_ context.Context,
		e models.Event[Aggregate],
	) error {
		old := e.EventName != models.ForgetEventName
		countIf(old, &oldRuns)
		return errorIf(old, errDown)
	}))
	cfg.RetryInitial, cfg.RetryMax = time.Hour, time.Hour
	cfg.MaxAttempts = 5
	d := h.build(t, cfg)

	mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, entered, "v1 popped and publishing")
	d.Supersede("a")
	tk := mustEnqueue(t, d, tombstone(marker("a", 2), acknowledge))
	closeOnce(release)
	if err := h.final(t, tk, "the marker"); err != nil {
		t.Fatalf("marker = %v", err)
	}
	if oldRuns.Load() != 0 || len(parked(d)) != 0 {
		t.Fatalf("the superseded job ran %d attempts, parked = %v; want 0 and none", oldRuns.Load(), parked(d))
	}
}

func supersedeLeavesNoRecord(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	d.Park("a", errImposed, 2)
	d.Supersede("a")
	if len(parked(d)) != 0 || d.Unrun("a", 2) != nil {
		t.Fatalf("parked = %v, Unrun = %v; want unparked and no record", parked(d), d.Unrun("a", 2))
	}
	tk := mustEnqueue(t, d, durable(event("a", 3), acknowledge))
	if err := h.final(t, tk, "a durable event after the supersede"); err != nil {
		t.Fatalf("final = %v, want delivered", err)
	}
}

func tombstoneFinalReportsAFailure(
	t testing.TB,
	h harness,
) {
	sub := subscriber(fail)
	sub.Match = never
	cfg := config(&bus{}, sub)
	cfg.MaxAttempts = 2
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, tombstone(marker("a", 2), nil))
	if err := h.final(t, tk, "the tombstone"); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
	if got := parked(d); len(got) != 0 {
		t.Fatalf("parked = %v, want none: a tombstone without an ack leaves parking to Forget", got)
	}
}

func forgetIsNeverDeadLettered(
	t testing.TB,
	h harness,
) {
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

	tk := mustEnqueue(t, d, tombstone(marker("a", 2), acknowledge))
	if err := h.final(t, tk, "the marker"); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked: a dead letter must not let Forget erase", err)
	}
	if dead.Load() != 0 || !slices.Equal(parked(d), []string{"a"}) {
		t.Fatalf("dead letters = %d, parked = %v; want none and [a]", dead.Load(), parked(d))
	}
}

func giveUpAfterASupersede(
	t testing.TB,
	h harness,
) {
	inHandler := make(chan struct{}, 4)
	release := make(chan struct{})
	var dead atomic.Int32
	cfg := config(&bus{}, subscriber(func(
		context.Context,
		models.Event[Aggregate],
	) error {
		inHandler <- struct{}{}
		<-release
		return errDown
	}))
	cfg.MaxAttempts = 1
	cfg.DeadLetter = func(
		context.Context,
		models.Event[Aggregate],
		error,
	) error {
		dead.Add(1)
		return nil
	}
	d := h.build(t, cfg)
	t.Cleanup(func() { closeOnce(release) })

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, inHandler, "the last attempt to run")
	d.Supersede("a")
	closeOnce(release)
	if err := h.final(t, tk, "the superseded delivery"); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
	if len(parked(d)) != 0 || dead.Load() != 0 {
		t.Fatalf("parked = %v, dead letters = %d: the exhausted attempt acted on a superseded delivery", parked(d), dead.Load())
	}
}

func interruptAfterASupersede(
	t testing.TB,
	h harness,
) {
	attempted := make(chan struct{}, 4)
	cfg := config(&bus{}, subscriber(func(
		context.Context,
		models.Event[Aggregate],
	) error {
		sendIf(true, attempted, struct{}{})
		return errDown
	}))
	cfg.RetryInitial, cfg.RetryMax = time.Hour, time.Hour
	cfg.MaxAttempts = 100
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, attempted, "the first attempt")
	d.Supersede("a")
	h.final(t, tk, "the superseded delivery")
	d.Interrupt()
	if got := parked(d); len(got) != 0 {
		t.Fatalf("parked = %v, want none", got)
	}
}

func parkWithoutAQueue(
	t testing.TB,
	h harness,
) {
	var acked sync.Map
	d := h.build(t, config(&bus{}))
	d.Park("ghost", errImposed)
	d.Park("ghost", errImposed)
	resume(d, "never-parked")

	tk := mustEnqueue(t, d, durable(event("ghost", 1), ackInto(&acked, 1)))
	if err := h.final(t, tk, "the parked event"); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
	if _, ok := acked.Load(int64(1)); ok {
		t.Fatal("an event of a parked aggregate was acknowledged")
	}
	resume(d, "ghost")
	tk = mustEnqueue(t, d, durable(event("ghost", 1), ackInto(&acked, 1)))
	if err := h.final(t, tk, "the event after the lift"); err != nil {
		t.Fatalf("final after the lift = %v", err)
	}
}

func parkKeepsLiveOnlyJobs(
	t testing.TB,
	h harness,
) {
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	d := h.build(t, config(&bus{}, subscriber(func(
		_ context.Context,
		e models.Event[Aggregate],
	) error {
		sendIf(e.Version == 1, started, struct{}{})
		holdIf(e.Version == 1, gate)
		return nil
	})))
	t.Cleanup(func() { closeOnce(gate) })

	mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, started, "v1 running")
	queued := mustEnqueue(t, d, durable(event("a", 2), acknowledge))
	liveOnly := mustEnqueue(t, d, live(event("a", 3)))
	d.Park("a", errImposed)
	if err := h.final(t, queued, "the queued durable event"); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("durable final = %v, want ErrDeliveryParked", err)
	}
	closeOnce(gate)
	if err := h.final(t, liveOnly, "the live-only event"); err != nil {
		t.Fatalf("live-only final = %v, want delivered", err)
	}
}

func closeOnce(
	ch chan struct{},
) {
	if isClosed(ch) {
		return
	}
	close(ch)
}

func parkQueuedJobsReportUnrun(
	t testing.TB,
	h harness,
) {
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	d := h.build(t, config(&bus{}, subscriber(func(
		_ context.Context,
		e models.Event[Aggregate],
	) error {
		sendIf(e.Version == 1, started, struct{}{})
		holdIf(e.Version == 1, gate)
		return nil
	})))
	t.Cleanup(func() { closeOnce(gate) })

	mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, started, "v1 running")
	queued := mustEnqueue(t, d, durable(event("a", 2), acknowledge))
	d.Park("a", errImposed)
	h.wait(t, queued.Settled, "the queued event to settle")
	if err := queued.Unrun(); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("Unrun = %v, want ErrDeliveryParked", err)
	}
}

func parkMergesIntoAPark(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	d.Park("a", errImposed, 3)
	generation := d.Parks()["a"]
	d.Park("a", errImposed, 5)
	if again := d.Parks()["a"]; again != generation {
		t.Fatalf("generation = %d after parking again, want the first park's %d", again, generation)
	}
	lifted, undelivered := d.ResumeUndelivered("a", generation)
	if !lifted || !slices.Equal(undelivered, []int64{3, 5}) {
		t.Fatalf("ResumeUndelivered = %v, %v; want true, [3 5]", lifted, undelivered)
	}
}

func parkAfterALift(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	d.Park("a", errImposed)
	first := d.Parks()["a"]
	resume(d, "a")
	d.Park("a", errImposed)
	if again, isParked := d.Parks()["a"]; !isParked || again == first {
		t.Fatalf("generation after a lift and a new park = %d (parked %v), want parked with a generation other than %d", again, isParked, first)
	}
}

func parkRemembersTheNamedVersions(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	d.Park("a", errImposed, 3, 5)
	if !slices.Equal(parked(d), []string{"a"}) {
		t.Fatalf("parked = %v, want [a]", parked(d))
	}
	if !errors.Is(d.Unrun("a", 3), models.ErrDeliveryParked) || !errors.Is(d.Unrun("a", 5), models.ErrDeliveryParked) {
		t.Fatalf("Unrun(a, 3) = %v, Unrun(a, 5) = %v; want ErrDeliveryParked", d.Unrun("a", 3), d.Unrun("a", 5))
	}
	if err := d.Unrun("a", 4); err != nil {
		t.Fatalf("Unrun(a, 4) = %v, want nil", err)
	}
	resume(d, "a")
	if err := d.Unrun("a", 3); err != nil {
		t.Fatalf("Unrun after the lift = %v, want nil", err)
	}
}

func giveUpParksBeforeReporting(
	t testing.TB,
	h harness,
) {
	var d models.Dispatcher[Aggregate]
	parkedWhenTold := make(chan []string, 4)
	cfg := config(&bus{}, subscriber(fail))
	cfg.MaxAttempts = 1
	cfg.OnDeliveryError = func(
		_ context.Context,
		_ models.Event[Aggregate],
		err error,
		_ int,
	) {
		sendIf(errors.Is(err, models.ErrDeliveryParked), parkedWhenTold, parked(d))
	}
	d = h.build(t, cfg)

	mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	if got := receive(t, h.patience, parkedWhenTold, "the park to be reported"); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("parked when the handler was told = %v, want [a]", got)
	}
}

func ackFailureParksBeforeReporting(
	t testing.TB,
	h harness,
) {
	var d models.Dispatcher[Aggregate]
	parkedWhenTold := make(chan []string, 4)
	cfg := config(&bus{}, subscriber(succeed))
	cfg.OnPublishError = func(
		context.Context,
		models.Event[Aggregate],
		error,
	) {
		sendIf(true, parkedWhenTold, parked(d))
	}
	d = h.build(t, cfg)

	mustEnqueue(t, d, durable(event("a", 1), failAck(errDown)))
	if got := receive(t, h.patience, parkedWhenTold, "the ack failure to be reported"); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("parked when the handler was told = %v, want [a]", got)
	}
}

func ackFailureParks(
	t testing.TB,
	h harness,
) {
	var rec journal
	var reported atomic.Int32
	cfg := config(&bus{}, subscriber(func(
		_ context.Context,
		e models.Event[Aggregate],
	) error {
		rec.add(e)
		return nil
	}))
	cfg.OnPublishError = func(
		context.Context,
		models.Event[Aggregate],
		error,
	) {
		reported.Add(1)
	}
	d := h.build(t, cfg)
	gate := make(chan struct{})
	t.Cleanup(func() { closeOnce(gate) })
	blockedFailingAck := func(
		context.Context,
	) error {
		holdUpTo(gate, h.patience)
		return errDown
	}

	t1 := mustEnqueue(t, d, durable(event("a", 1), blockedFailingAck))
	t2 := mustEnqueue(t, d, durable(event("a", 2), acknowledge))
	closeOnce(gate)
	if err := h.final(t, t1, "v1"); err == nil {
		t.Fatal("v1 ended as acknowledged")
	}
	if err := h.final(t, t2, "v2"); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("v2 = %v, want ErrDeliveryParked", err)
	}
	if !slices.Equal(rec.list(), []string{"a/1"}) || !slices.Equal(parked(d), []string{"a"}) || reported.Load() != 1 {
		t.Fatalf("delivered %v, parked %v, reported %d; want only v1, [a], 1", rec.list(), parked(d), reported.Load())
	}
}

func resumeUndeliveredReturnsTheRecord(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	if lifted, undelivered := d.ResumeUndelivered("a", 1); lifted || undelivered != nil {
		t.Fatalf("not parked: got %v, %v", lifted, undelivered)
	}
	d.Park("a", errImposed, 7, 3, 5)
	lifted, undelivered := d.ResumeUndelivered("a", d.Parks()["a"])
	if !lifted || !slices.Equal(undelivered, []int64{3, 5, 7}) {
		t.Fatalf("ResumeUndelivered = %v, %v; want true, [3 5 7]", lifted, undelivered)
	}
	if err := d.Unrun("a", 3); err != nil {
		t.Fatalf("Unrun after the lift = %v, want nil", err)
	}
}

func resumeUndeliveredLeavesANewerPark(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	d.Park("a", errImposed)
	listed := d.Parks()["a"]
	resume(d, "a")
	d.Park("a", errImposed, 4)

	if lifted, _ := d.ResumeUndelivered("a", listed); lifted {
		t.Fatal("lifted a park other than the one listed")
	}
	if !slices.Equal(parked(d), []string{"a"}) {
		t.Fatalf("parked = %v, want a still parked", parked(d))
	}
	if err := d.Unrun("a", 4); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("Unrun = %v, want what the new park left undelivered kept", err)
	}
	if again := d.Parks()["a"]; again == listed {
		t.Fatal("a new park reused the generation of the old one")
	}
}
