package aggregates_test

import (
	"errors"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/aggregates"
	asynxmd "github.com/char2cs/asynx/models"
)

func enqueue(
	t *testing.T,
	r aggregates.Registry[*fakeItem],
	job *fakeItem,
) aggregates.Admission {
	t.Helper()
	admission, err := r.Enqueue(job)
	if err != nil {
		t.Fatalf("Enqueue(%s/%d) = %v", job.aggregateID, job.version, err)
	}
	return admission
}

func pop(
	t *testing.T,
	r aggregates.Registry[*fakeItem],
	aggregateID string,
) (*fakeItem, <-chan struct{}) {
	t.Helper()
	job, halt, ok := r.Pop(aggregateID)
	if !ok {
		t.Fatalf("Pop(%s) found nothing queued", aggregateID)
	}
	return job, halt
}

func closed(
	ch <-chan struct{},
) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func generation(
	t *testing.T,
	r aggregates.Registry[*fakeItem],
	aggregateID string,
) uint64 {
	t.Helper()
	g, ok := r.Parks()[aggregateID]
	if !ok {
		t.Fatalf("%s is not parked", aggregateID)
	}
	return g
}

func assertDroppedAsParked(
	t *testing.T,
	r aggregates.Registry[*fakeItem],
	job *fakeItem,
) {
	t.Helper()
	unrun, completions, err := job.state()
	if !errors.Is(unrun, asynxmd.ErrDeliveryParked) {
		t.Fatalf("%d: unrun = %v, want ErrDeliveryParked", job.version, unrun)
	}
	if completions != 1 || !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("%d: completed %d times with %v, want once with ErrDeliveryParked", job.version, completions, err)
	}
	if got := r.Unrun(job.aggregateID, job.version); !errors.Is(got, asynxmd.ErrDeliveryParked) {
		t.Fatalf("Unrun(%d) = %v, want ErrDeliveryParked", job.version, got)
	}
}

func assertUntouched(
	t *testing.T,
	job *fakeItem,
) {
	t.Helper()
	unrun, completions, _ := job.state()
	if unrun != nil || completions != 0 {
		t.Fatalf("%d: unrun = %v, completions = %d, want untouched", job.version, unrun, completions)
	}
}

func TestEnqueue_SpawnsOneWorkerPerAggregateAndAdmitsEveryQueuedJob(t *testing.T) {
	r, hooks := newRegistry()
	if !enqueue(t, r, durableItem("a", 1)).Spawned {
		t.Fatal("first job of a did not spawn a worker")
	}
	if enqueue(t, r, liveItem("a", 2)).Spawned {
		t.Fatal("second job of a spawned another worker")
	}
	if !enqueue(t, r, durableItem("b", 1)).Spawned {
		t.Fatal("first job of b did not spawn a worker")
	}
	if hooks.spawns() != 2 || hooks.admits() != 3 {
		t.Fatalf("spawns = %d, admits = %d, want 2 and 3", hooks.spawns(), hooks.admits())
	}
}

func TestEnqueue_SignalsTheWakeChannel(t *testing.T) {
	r, _ := newRegistry()
	enqueue(t, r, liveItem("a", 1))
	enqueue(t, r, liveItem("a", 2))
	wake := r.Wake("a")
	if !closed(wake) {
		t.Fatal("wake channel not signalled by Enqueue")
	}
	if closed(wake) {
		t.Fatal("wake channel holds more than one signal")
	}
	enqueue(t, r, liveItem("a", 3))
	if !closed(wake) {
		t.Fatal("wake channel not signalled again")
	}
}

func TestPop_ReturnsJobsInEnqueueOrderWithTheirHalt(t *testing.T) {
	r, _ := newRegistry()
	first, second := durableItem("a", 1), liveItem("a", 2)
	enqueue(t, r, first)
	enqueue(t, r, second)
	got, halt := pop(t, r, "a")
	wrongPop := got != first || halt == nil || closed(halt)
	if wrongPop {
		t.Fatalf("Pop = %d with halt closed=%v, want 1 under an open halt", got.version, closed(halt))
	}
	if got, _ = pop(t, r, "a"); got != second {
		t.Fatalf("Pop = %d, want 2", got.version)
	}
	if _, _, ok := r.Pop("a"); ok {
		t.Fatal("Pop of a drained queue found a job")
	}
}

func TestEnqueue_RefusedAfterClose(t *testing.T) {
	r, hooks := newRegistry()
	if !r.Close() {
		t.Fatal("first Close reported already closed")
	}
	if r.Close() {
		t.Fatal("second Close reported it closed the registry")
	}
	job := durableItem("a", 1)
	if _, err := r.Enqueue(job); !errors.Is(err, asynxmd.ErrDispatcherClosed) {
		t.Fatalf("Enqueue after Close = %v, want ErrDispatcherClosed", err)
	}
	if hooks.spawns() != 0 || hooks.admits() != 0 {
		t.Fatal("a refused job spawned a worker or was admitted")
	}
	assertUntouched(t, job)
}

func TestPark_KeepsLiveOnlyJobsQueuedAndDropsDurableOnes(t *testing.T) {
	r, _ := newRegistry()
	running, durable, live := durableItem("a", 1), durableItem("a", 2), liveItem("a", 3)
	enqueue(t, r, running)
	_, halt := pop(t, r, "a")
	enqueue(t, r, durable)
	enqueue(t, r, live)

	r.Park("a", errHalted)

	if !closed(halt) {
		t.Fatal("Park left the halt channel of the running delivery open")
	}
	assertDroppedAsParked(t, r, durable)
	assertUntouched(t, live)
	assertUntouched(t, running)
	if got, _ := pop(t, r, "a"); got != live {
		t.Fatalf("Pop = %d, want the live job", got.version)
	}
	if _, _, ok := r.Pop("a"); ok {
		t.Fatal("a durable job stayed queued through the park")
	}
	generation(t, r, "a")
}

func TestPark_QueuedDurableJobsReportThemselvesUnrun(t *testing.T) {
	r, _ := newRegistry()
	jobs := []*fakeItem{durableItem("a", 1), durableItem("a", 2), durableItem("a", 3)}
	for _, job := range jobs {
		enqueue(t, r, job)
	}
	r.Park("a", errHalted)
	for _, job := range jobs {
		assertDroppedAsParked(t, r, job)
	}
}

func TestPark_OfAParkedAggregateMergesTheVersionsAndKeepsTheGeneration(t *testing.T) {
	r, _ := newRegistry()
	r.Park("a", errHalted, 1)
	g := generation(t, r, "a")
	r.Park("a", errHalted, 2)
	if again := generation(t, r, "a"); again != g {
		t.Fatalf("generation %d became %d on a second Park", g, again)
	}
	if r.Unrun("a", 1) == nil || r.Unrun("a", 2) == nil {
		t.Fatal("a second Park dropped or did not record versions")
	}
	if _, undelivered := r.Lift("a", g); !slices.Equal(undelivered, []int64{1, 2}) {
		t.Fatalf("undelivered = %v, want [1 2]", undelivered)
	}
}

func TestPark_OfAnAggregateWithoutAQueueStillBlocksItsDurableEvents(t *testing.T) {
	r, hooks := newRegistry()
	r.Park("ghost", errHalted)
	job := durableItem("ghost", 1)
	if !enqueue(t, r, job).Parked {
		t.Fatal("a durable job of a parked aggregate was admitted")
	}
	assertDroppedAsParked(t, r, job)
	if hooks.admits() != 0 || hooks.spawns() != 0 {
		t.Fatal("a refused job was admitted or spawned a worker")
	}
}

func TestPark_ADurableEnqueueOnAParkedAggregateIsSettledUnrun(t *testing.T) {
	r, hooks := newRegistry()
	enqueue(t, r, liveItem("a", 1))
	r.Park("a", errHalted)
	g := generation(t, r, "a")

	job := durableItem("a", 2)
	if !enqueue(t, r, job).Parked {
		t.Fatal("Admission.Parked is false")
	}
	assertDroppedAsParked(t, r, job)
	if again := generation(t, r, "a"); again != g {
		t.Fatalf("refusing a job changed the generation %d to %d", g, again)
	}
	live := liveItem("a", 3)
	if enqueue(t, r, live).Parked {
		t.Fatal("a live job was refused by the park")
	}
	pop(t, r, "a")
	if got, _ := pop(t, r, "a"); got != live {
		t.Fatalf("Pop = %d, want the live job queued after the park", got.version)
	}
	if hooks.admits() != 2 {
		t.Fatalf("admits = %d, want only the two live jobs", hooks.admits())
	}
}

func TestPark_TakesANewGenerationEachTimeItIsImposed(t *testing.T) {
	r, _ := newRegistry()
	r.Park("a", errHalted)
	first := generation(t, r, "a")
	r.Park("b", errHalted)
	if generation(t, r, "b") == first {
		t.Fatal("two aggregates share a generation")
	}
	r.Lift("a", first)
	r.Park("a", errHalted)
	if generation(t, r, "a") == first {
		t.Fatal("a park imposed again kept its old generation")
	}
}

func TestResumeUndelivered_ReturnsWhatTheParkLeftUndeliveredAndForgetsIt(t *testing.T) {
	r, _ := newRegistry()
	enqueue(t, r, durableItem("a", 1))
	_, halt := pop(t, r, "a")
	r.Park("a", errHalted, 3, 1)
	g := generation(t, r, "a")

	lifted, undelivered := r.Lift("a", g)

	if !lifted || !slices.Equal(undelivered, []int64{1, 3}) {
		t.Fatalf("Lift = %v, %v, want true, [1 3]", lifted, undelivered)
	}
	if len(r.Parks()) != 0 {
		t.Fatalf("Parks = %v after the lift", r.Parks())
	}
	if r.Unrun("a", 1) != nil || r.Unrun("a", 3) != nil {
		t.Fatal("Unrun still answers after the lift")
	}
	if lifted, _ := r.Lift("a", g); lifted {
		t.Fatal("a second Lift lifted again")
	}
	assertFreshHalt(t, r, halt)
}

func assertFreshHalt(
	t *testing.T,
	r aggregates.Registry[*fakeItem],
	old <-chan struct{},
) {
	t.Helper()
	next := durableItem("a", 9)
	if enqueue(t, r, next).Parked {
		t.Fatal("a durable job was refused after the lift")
	}
	got, halt := pop(t, r, "a")
	notFresh := got != next || halt == old || closed(halt)
	if notFresh {
		t.Fatal("after the lift the job is not delivered under a fresh open halt")
	}
}

func TestResumeUndelivered_ReturnsTheVersionsAscending(t *testing.T) {
	r, _ := newRegistry()
	enqueue(t, r, durableItem("a", 4))
	r.Park("a", errHalted, 9, 2)
	enqueue(t, r, durableItem("a", 3))
	r.Park("a", errHalted, 5)
	_, undelivered := r.Lift("a", generation(t, r, "a"))
	if !slices.Equal(undelivered, []int64{2, 3, 4, 5, 9}) {
		t.Fatalf("undelivered = %v, want [2 3 4 5 9]", undelivered)
	}
}

func TestResumeUndelivered_LeavesAParkImposedAgainSinceItWasListed(t *testing.T) {
	r, _ := newRegistry()
	r.Park("a", errHalted, 1)
	stale := generation(t, r, "a")
	r.Lift("a", stale)
	r.Park("a", errHalted, 2)
	current := generation(t, r, "a")

	lifted, undelivered := r.Lift("a", stale)

	if lifted || undelivered != nil {
		t.Fatalf("Lift with a stale generation = %v, %v, want false, nil", lifted, undelivered)
	}
	if generation(t, r, "a") != current {
		t.Fatal("a stale Lift changed the generation")
	}
	if r.Unrun("a", 2) == nil {
		t.Fatal("a stale Lift forgot the undelivered versions")
	}
}

func TestResumeUndelivered_WithAStaleGenerationChangesNothing(t *testing.T) {
	r, _ := newRegistry()
	enqueue(t, r, liveItem("a", 1))
	_, halt := pop(t, r, "a")
	r.Park("a", errHalted, 7)
	g := generation(t, r, "a")

	if lifted, undelivered := r.Lift("a", g+1); lifted || undelivered != nil {
		t.Fatalf("Lift(g+1) = %v, %v, want false, nil", lifted, undelivered)
	}
	if generation(t, r, "a") != g || r.Unrun("a", 7) == nil {
		t.Fatal("a stale Lift changed the park")
	}
	job := durableItem("a", 8)
	if !enqueue(t, r, job).Parked {
		t.Fatal("a stale Lift let a durable job through")
	}
	if !closed(halt) {
		t.Fatal("a stale Lift reopened the halt channel")
	}
}

func TestResumeUndelivered_OfAnUnparkedAggregateDoesNothing(t *testing.T) {
	r, _ := newRegistry()
	enqueue(t, r, liveItem("a", 1))
	if lifted, undelivered := r.Lift("a", 0); lifted || undelivered != nil {
		t.Fatalf("Lift = %v, %v, want false, nil", lifted, undelivered)
	}
}

func TestSupersede_DropsQueuedDurableJobsAndLeavesNoPark(t *testing.T) {
	r, _ := newRegistry()
	running, durable, live := durableItem("a", 1), durableItem("a", 2), liveItem("a", 3)
	enqueue(t, r, running)
	_, halt := pop(t, r, "a")
	enqueue(t, r, durable)
	enqueue(t, r, live)

	r.Supersede("a")

	if !closed(halt) {
		t.Fatal("Supersede left the running delivery's halt open")
	}
	unrun, completions, err := durable.state()
	wrongSupersede := !errors.Is(unrun, asynxmd.ErrDeliveryParked) || completions != 1 || !errors.Is(err, asynxmd.ErrDeliveryParked)
	if wrongSupersede {
		t.Fatalf("superseded job: unrun %v, completed %d with %v", unrun, completions, err)
	}
	if len(r.Parks()) != 0 {
		t.Fatalf("Parks = %v after Supersede", r.Parks())
	}
	if r.Unrun("a", 2) != nil {
		t.Fatal("Supersede kept the undelivered record")
	}
	if got, _ := pop(t, r, "a"); got != live {
		t.Fatal("Supersede dropped a live job")
	}
	assertFreshHalt(t, r, halt)
}

func TestSupersede_OfAParkedAggregateClearsTheRecord(t *testing.T) {
	r, hooks := newRegistry()
	r.Park("a", errHalted, 1, 2)
	r.Supersede("a")
	if len(r.Parks()) != 0 || r.Unrun("a", 1) != nil {
		t.Fatal("Supersede left the park or its record")
	}
	if r.Wake("a") != nil {
		t.Fatal("Supersede left an empty record behind")
	}
	if !enqueue(t, r, durableItem("a", 3)).Spawned || hooks.spawns() != 1 {
		t.Fatal("a durable job after Supersede was not queued on a new worker")
	}
}

func TestParkIfCurrent_WithAnOpenHaltParksTheAggregate(t *testing.T) {
	r, _ := newRegistry()
	enqueue(t, r, durableItem("a", 1))
	_, halt := pop(t, r, "a")
	queued := durableItem("a", 2)
	enqueue(t, r, queued)

	if !r.ParkIfCurrent("a", halt, true, errHalted) {
		t.Fatal("ParkIfCurrent under an open halt did not park")
	}
	generation(t, r, "a")
	if !closed(halt) {
		t.Fatal("ParkIfCurrent left the halt open")
	}
	assertDroppedAsParked(t, r, queued)
}

func TestParkIfCurrent_OfALiveOnlyJobDoesNothing(t *testing.T) {
	r, _ := newRegistry()
	enqueue(t, r, liveItem("a", 1))
	_, halt := pop(t, r, "a")
	if r.ParkIfCurrent("a", halt, false, errHalted) {
		t.Fatal("ParkIfCurrent parked for a live-only job")
	}
	if len(r.Parks()) != 0 || closed(halt) {
		t.Fatal("ParkIfCurrent changed the aggregate for a live-only job")
	}
}

func TestParkIfCurrent_WithAClosedHaltDoesNothing(t *testing.T) {
	r, _ := newRegistry()
	enqueue(t, r, durableItem("a", 1))
	_, halt := pop(t, r, "a")
	r.Supersede("a")
	queued := durableItem("a", 2)
	enqueue(t, r, queued)

	if r.ParkIfCurrent("a", halt, true, errHalted) {
		t.Fatal("ParkIfCurrent with a closed halt reported a park")
	}
	if len(r.Parks()) != 0 {
		t.Fatalf("Parks = %v: a superseded delivery parked the aggregate again", r.Parks())
	}
	assertUntouched(t, queued)
}

func TestParkIfCurrent_AfterALiftDoesNotParkAgain(t *testing.T) {
	r, _ := newRegistry()
	enqueue(t, r, durableItem("a", 1))
	_, halt := pop(t, r, "a")
	r.Park("a", errHalted)
	r.Lift("a", generation(t, r, "a"))
	if r.ParkIfCurrent("a", halt, true, errHalted) || len(r.Parks()) != 0 {
		t.Fatal("ParkIfCurrent re-imposed a park that was lifted")
	}
}

func TestSettleIfParked_RecordsAParkStillInForce(t *testing.T) {
	r, _ := newRegistry()
	job := durableItem("a", 1)
	enqueue(t, r, job)
	popped, halt := pop(t, r, "a")
	r.Park("a", errHalted)
	if !r.SettleIfParked("a", halt, popped) {
		t.Fatal("SettleIfParked of a parked aggregate refused")
	}
	unrun, completions, _ := job.state()
	if !errors.Is(unrun, asynxmd.ErrDeliveryParked) || completions != 0 {
		t.Fatalf("unrun = %v, completions = %d, want ErrDeliveryParked and not completed", unrun, completions)
	}
	if !errors.Is(r.Unrun("a", 1), asynxmd.ErrDeliveryParked) {
		t.Fatal("a park in force did not record the version")
	}
}

func TestSettleIfParked_AfterTheLiftSettlesWithoutARecord(t *testing.T) {
	r, _ := newRegistry()
	job := durableItem("a", 1)
	enqueue(t, r, job)
	popped, halt := pop(t, r, "a")
	r.Park("a", errHalted)
	r.Lift("a", generation(t, r, "a"))

	if !r.SettleIfParked("a", halt, popped) {
		t.Fatal("SettleIfParked under a closed halt refused")
	}
	if unrun, _, _ := job.state(); !errors.Is(unrun, asynxmd.ErrDeliveryParked) {
		t.Fatalf("unrun = %v, want ErrDeliveryParked", unrun)
	}
	if r.Unrun("a", 1) != nil {
		t.Fatal("a lifted park left a record that outlives it")
	}
	if len(r.Parks()) != 0 {
		t.Fatal("SettleIfParked parked the aggregate")
	}
}

func TestSettleIfParked_UnderAnOpenHaltDoesNothing(t *testing.T) {
	r, _ := newRegistry()
	job := durableItem("a", 1)
	enqueue(t, r, job)
	popped, halt := pop(t, r, "a")
	if r.SettleIfParked("a", halt, popped) {
		t.Fatal("SettleIfParked of a running aggregate settled the job")
	}
	assertUntouched(t, job)
}

func TestRetire_NeverRemovesAQueueThatStillHasJobs(t *testing.T) {
	r, hooks := newRegistry()
	enqueue(t, r, liveItem("a", 1))
	second := liveItem("a", 2)
	enqueue(t, r, second)
	pop(t, r, "a")

	if r.Retire("a") {
		t.Fatal("Retire let the worker of a queue with jobs exit")
	}
	if got, _ := pop(t, r, "a"); got != second {
		t.Fatal("Retire lost a queued job")
	}
	if enqueue(t, r, liveItem("a", 3)).Spawned || hooks.spawns() != 1 {
		t.Fatal("a refused Retire released the worker")
	}
}

func TestRetire_ReleasesAnIdleWorker(t *testing.T) {
	r, hooks := newRegistry()
	enqueue(t, r, liveItem("a", 1))
	pop(t, r, "a")
	if !r.Retire("a") {
		t.Fatal("Retire of an idle aggregate refused")
	}
	if r.Wake("a") != nil {
		t.Fatal("Retire left an empty record behind")
	}
	if !enqueue(t, r, liveItem("a", 2)).Spawned || hooks.spawns() != 2 {
		t.Fatal("an Enqueue after Retire did not spawn a new worker")
	}
}

func TestRetire_KeepsTheRecordOfAParkedAggregate(t *testing.T) {
	r, hooks := newRegistry()
	enqueue(t, r, durableItem("a", 1))
	pop(t, r, "a")
	r.Park("a", errHalted, 7)
	g := generation(t, r, "a")

	if !r.Retire("a") {
		t.Fatal("Retire of an idle parked aggregate refused")
	}
	if generation(t, r, "a") != g {
		t.Fatal("Retire changed the park")
	}
	if r.Unrun("a", 7) == nil {
		t.Fatal("Retire dropped the undelivered versions")
	}
	if !enqueue(t, r, durableItem("a", 8)).Parked {
		t.Fatal("a durable job passed a park that survived Retire")
	}
	if !enqueue(t, r, liveItem("a", 9)).Spawned || hooks.spawns() != 2 {
		t.Fatal("a live job after Retire did not spawn a new worker")
	}
	if _, undelivered := r.Lift("a", g); !slices.Equal(undelivered, []int64{7, 8}) {
		t.Fatalf("undelivered = %v, want [7 8]", undelivered)
	}
}

func TestRetire_ThenLiftLeavesNoRecord(t *testing.T) {
	r, _ := newRegistry()
	enqueue(t, r, liveItem("a", 1))
	pop(t, r, "a")
	r.Park("a", errHalted, 1)
	r.Retire("a")
	r.Lift("a", generation(t, r, "a"))
	if r.Wake("a") != nil || r.Unrun("a", 1) != nil {
		t.Fatal("a lifted park of a retired aggregate left a record behind")
	}
}

func TestEnqueue_RacingLiftNeverQueuesADurableJobWhileParked(t *testing.T) {
	const jobs = 2000
	r, _ := newRegistry()
	done := make(chan struct{})
	var violations atomic.Int64
	var wg sync.WaitGroup
	wg.Add(2)
	go parkAndLift(&wg, r, done, &violations)
	go popAll(&wg, r, done)

	items := make([]*fakeItem, jobs)
	for i := range items {
		items[i] = durableItem("a", int64(i+1))
		enqueue(t, r, items[i])
	}
	close(done)
	wg.Wait()
	for job, _, ok := r.Pop("a"); ok; job, _, ok = r.Pop("a") {
		job.Complete(nil)
	}

	if violations.Load() != 0 {
		t.Fatalf("%d durable jobs were queued while the aggregate was parked", violations.Load())
	}
	for _, job := range items {
		assertSettledOnce(t, job)
	}
}

func TestInspect_RacingParkAndLiftNeverReportsAnInconsistentStatus(t *testing.T) {
	const reads = 20000
	r, _ := newRegistry()
	done := make(chan struct{})
	var violations atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go parkAndLift(&wg, r, done, &violations)

	statuses := make([]asynxmd.DeliveryStatus, reads)
	for i := range statuses {
		statuses[i] = r.Inspect("a")
	}
	inconsistent := len(slices.DeleteFunc(statuses, consistent))
	close(done)
	wg.Wait()

	if inconsistent != 0 {
		t.Fatalf("%d of %d statuses were inconsistent while the park raced its lift", inconsistent, reads)
	}
}

func consistent(
	status asynxmd.DeliveryStatus,
) bool {
	if status.Parked {
		return status.Generation != 0
	}
	return status.Cause == nil && len(status.Undelivered) == 0
}

func parkAndLift(
	wg *sync.WaitGroup,
	r aggregates.Registry[*fakeItem],
	done <-chan struct{},
	violations *atomic.Int64,
) {
	defer wg.Done()
	for !closed(done) {
		r.Park("a", errHalted)
		violations.Add(popWhileParked(r))
		r.Lift("a", r.Parks()["a"])
	}
}

func popWhileParked(
	r aggregates.Registry[*fakeItem],
) int64 {
	job, _, ok := r.Pop("a")
	if !ok {
		return 0
	}
	job.Complete(nil)
	return 1
}

func popAll(
	wg *sync.WaitGroup,
	r aggregates.Registry[*fakeItem],
	done <-chan struct{},
) {
	defer wg.Done()
	for !closed(done) {
		completePopped(r)
	}
}

func completePopped(
	r aggregates.Registry[*fakeItem],
) {
	job, halt, ok := r.Pop("a")
	if !ok {
		return
	}
	if r.SettleIfParked("a", halt, job) {
		job.Complete(asynxmd.ErrDeliveryParked)
		return
	}
	job.Complete(nil)
}

func assertSettledOnce(
	t *testing.T,
	job *fakeItem,
) {
	t.Helper()
	unrun, completions, err := job.state()
	if completions != 1 {
		t.Fatalf("%d: completed %d times, want once", job.version, completions)
	}
	if errors.Is(err, asynxmd.ErrDeliveryParked) != errors.Is(unrun, asynxmd.ErrDeliveryParked) {
		t.Fatalf("%d: completed with %v but unrun = %v", job.version, err, unrun)
	}
}

var (
	errHalted = errors.New("test: halted")
	errLater  = errors.New("test: a later cause")
)

func TestInspect_UnknownAggregateIsTheZeroStatus(t *testing.T) {
	r, _ := newRegistry()
	enqueue(t, r, durableItem("busy", 1))
	if got := r.Inspect("ghost"); !reflect.DeepEqual(got, asynxmd.DeliveryStatus{}) {
		t.Fatalf("Inspect(ghost) = %+v, want the zero status", got)
	}
	if got := r.Inspect("busy"); !reflect.DeepEqual(got, asynxmd.DeliveryStatus{}) {
		t.Fatalf("Inspect(busy) = %+v, want the zero status for a running aggregate", got)
	}
}

func TestInspect_ReportsParkGenerationCauseAndUndelivered(t *testing.T) {
	r, _ := newRegistry()
	enqueue(t, r, durableItem("a", 4))
	r.Park("a", errHalted, 9, 2)
	got := r.Inspect("a")
	if !got.Parked || got.Generation != generation(t, r, "a") {
		t.Fatalf("Inspect = %+v, want parked with the generation Parks lists", got)
	}
	if !errors.Is(got.Cause, errHalted) {
		t.Fatalf("Cause = %v, want %v", got.Cause, errHalted)
	}
	if !slices.Equal(got.Undelivered, []int64{2, 4, 9}) {
		t.Fatalf("Undelivered = %v, want [2 4 9] ascending", got.Undelivered)
	}
}

func TestPark_KeepsTheFirstNonNilCause(t *testing.T) {
	r, _ := newRegistry()
	r.Park("a", errHalted, 1)
	r.Park("a", errLater, 2)
	r.Park("a", nil, 3)
	got := r.Inspect("a")
	if !errors.Is(got.Cause, errHalted) || errors.Is(got.Cause, errLater) {
		t.Fatalf("Cause = %v, want the first one, %v", got.Cause, errHalted)
	}
	if !slices.Equal(got.Undelivered, []int64{1, 2, 3}) {
		t.Fatalf("Undelivered = %v, want [1 2 3]", got.Undelivered)
	}
}

func TestPark_ALaterCauseReplacesANilOne(t *testing.T) {
	r, _ := newRegistry()
	r.Park("a", nil)
	if got := r.Inspect("a"); !got.Parked || got.Cause != nil {
		t.Fatalf("Inspect = %+v, want parked with no cause", got)
	}
	r.Park("a", errLater)
	if got := r.Inspect("a"); !errors.Is(got.Cause, errLater) {
		t.Fatalf("Cause = %v, want %v", got.Cause, errLater)
	}
}

func TestParkIfCurrent_RecordsTheCause(t *testing.T) {
	r, _ := newRegistry()
	enqueue(t, r, durableItem("a", 1))
	_, halt := pop(t, r, "a")
	r.ParkIfCurrent("a", halt, true, errHalted)
	if got := r.Inspect("a"); !got.Parked || !errors.Is(got.Cause, errHalted) {
		t.Fatalf("Inspect = %+v, want parked with %v", got, errHalted)
	}
}

func TestInspect_GenerationChangesWhenAParkIsImposedAgain(t *testing.T) {
	r, _ := newRegistry()
	r.Park("a", errHalted)
	first := r.Inspect("a").Generation
	r.Park("a", errLater)
	if again := r.Inspect("a").Generation; again != first {
		t.Fatalf("generation %d became %d while still parked", first, again)
	}
	r.Lift("a", first)
	r.Park("a", errLater)
	got := r.Inspect("a")
	notReimposed := !got.Parked || got.Generation == first || got.Generation == 0
	if notReimposed {
		t.Fatalf("Inspect = %+v, want parked with a generation other than %d", got, first)
	}
	if !errors.Is(got.Cause, errLater) {
		t.Fatalf("Cause = %v, want the new park's %v", got.Cause, errLater)
	}
}

func TestInspect_IsClearAfterTheParkLifts(t *testing.T) {
	r, _ := newRegistry()
	enqueue(t, r, liveItem("a", 1))
	r.Park("a", errHalted, 3)
	r.Lift("a", generation(t, r, "a"))
	if got := r.Inspect("a"); !reflect.DeepEqual(got, asynxmd.DeliveryStatus{}) {
		t.Fatalf("Inspect after the lift = %+v, want the zero status", got)
	}
	r.Park("a", nil)
	if got := r.Inspect("a"); got.Cause != nil {
		t.Fatalf("Cause = %v after a lift and a nil-cause park, want nil", got.Cause)
	}
}
