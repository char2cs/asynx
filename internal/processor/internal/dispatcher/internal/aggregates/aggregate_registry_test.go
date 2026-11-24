package aggregates_test

import (
	"errors"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/aggregates"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/aggregates/contracts"
	asynxmd "github.com/char2cs/asynx/models"
)

type fakeItem struct {
	mu          sync.Mutex
	aggregateID string
	version     int64
	durable     bool
	unrun       error
	unrunCalls  int
	err         error
	completions int
}

func durableItem(
	aggregateID string,
	version int64,
) *fakeItem {
	return &fakeItem{aggregateID: aggregateID, version: version, durable: true}
}

func liveItem(
	aggregateID string,
	version int64,
) *fakeItem {
	return &fakeItem{aggregateID: aggregateID, version: version}
}

// AggregateID returns the aggregate the fake item was built for.
func (f *fakeItem) AggregateID() string { return f.aggregateID }

// Version returns the version the fake item was built with.
func (f *fakeItem) Version() int64 { return f.version }

// Acked reports whether the fake item was built as a durable job, which a
// park holds back.
func (f *fakeItem) Acked() bool { return f.durable }

// MarkUnrun counts the call and keeps the last reason, so a test can assert
// how often and why the registry settled the item as unrun.
func (f *fakeItem) MarkUnrun(
	err error,
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unrunCalls++
	f.unrun = err
}

// Complete counts the call and keeps the last error, so a test can assert
// that the registry completed the item once and with what.
func (f *fakeItem) Complete(
	err error,
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completions++
	f.err = err
}

func (f *fakeItem) state() (unrun error, completions int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unrun,
		f.completions,
		f.err
}

type hookRecorder struct {
	mu       sync.Mutex
	admitted []*fakeItem
}

type testRegistry struct {
	aggregates.Registry[*fakeItem]
	hooks *hookRecorder
	mu    sync.Mutex
	begun map[*fakeItem]bool
	dones map[*fakeItem]chan<- struct{}
}

func newRegistry() (*testRegistry, *hookRecorder) {
	rec := &hookRecorder{}
	return &testRegistry{
			Registry: aggregates.New(aggregates.Hooks[*fakeItem]{Admit: rec.admit}),
			hooks:    rec,
			begun:    make(map[*fakeItem]bool),
			dones:    make(map[*fakeItem]chan<- struct{}),
		},
		rec
}

// Enqueue enqueues job and remembers the channel it hands its turn on with.
func (r *testRegistry) Enqueue(
	job *fakeItem,
) (contracts.Admission, error) {
	admission, err := r.Registry.Enqueue(job)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dones[job] = admission.Done
	return admission,
		err
}

// Pop begins the oldest job of the aggregate that is neither begun nor
// dropped by a park, the way the next run does when its turn comes.
func (r *testRegistry) Pop(
	aggregateID string,
) (*fakeItem, <-chan struct{}, bool) {
	return r.beginFirst(r.hooks.queued(aggregateID))
}

func (r *testRegistry) beginFirst(
	candidates []*fakeItem,
) (job *fakeItem, halt <-chan struct{}, ok bool) {
	if len(candidates) == 0 {
		return nil,
			nil,
			false
	}

	halt, ok = r.beginOnce(candidates[0])
	if ok {
		return candidates[0],
			halt,
			true
	}

	return r.beginFirst(candidates[1:])
}

func (r *testRegistry) beginOnce(
	job *fakeItem,
) (halt <-chan struct{}, ok bool) {
	if r.take(job) {
		return nil,
			false
	}

	return r.Begin(job)
}

func (r *testRegistry) take(
	job *fakeItem,
) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	was := r.begun[job]
	r.begun[job] = true
	return was
}

func (r *testRegistry) finish(
	job *fakeItem,
) {
	r.mu.Lock()
	done := r.dones[job]
	r.mu.Unlock()
	r.Registry.Finish(
		job.aggregateID,
		done,
	)
}

func (h *hookRecorder) admit(
	job *fakeItem,
) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.admitted = append(
		h.admitted,
		job,
	)
}

func (h *hookRecorder) queued(
	aggregateID string,
) (jobs []*fakeItem) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, job := range h.admitted {
		jobs = appendOf(
			jobs,
			job,
			aggregateID,
		)
	}

	return jobs
}

func appendOf(
	jobs []*fakeItem,
	job *fakeItem,
	aggregateID string,
) []*fakeItem {
	if job.aggregateID != aggregateID {
		return jobs
	}

	return append(
		jobs,
		job,
	)
}

func (h *hookRecorder) admits() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.admitted)
}

func enqueue(
	t *testing.T,
	r *testRegistry,
	job *fakeItem,
) contracts.Admission {
	t.Helper()
	admission, err := r.Enqueue(job)
	if err != nil {
		t.Fatalf(
			"Enqueue(%s/%d) = %v",
			job.aggregateID,
			job.version,
			err,
		)
	}

	return admission
}

func pop(
	t *testing.T,
	r *testRegistry,
	aggregateID string,
) (*fakeItem, <-chan struct{}) {
	t.Helper()
	job, halt, ok := r.Pop(aggregateID)
	if !ok {
		t.Fatalf(
			"Pop(%s) found nothing queued",
			aggregateID,
		)
	}

	return job,
		halt
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
	r *testRegistry,
	aggregateID string,
) uint64 {
	t.Helper()
	g, ok := r.Parks()[aggregateID]
	if !ok {
		t.Fatalf(
			"%s is not parked",
			aggregateID,
		)
	}

	return g
}

func assertDroppedAsParked(
	t *testing.T,
	r *testRegistry,
	job *fakeItem,
) {
	t.Helper()
	unrun, completions, err := job.state()
	if !errors.Is(
		unrun,
		asynxmd.ErrDeliveryParked,
	) {
		t.Fatalf(
			"%d: unrun = %v, want ErrDeliveryParked",
			job.version,
			unrun,
		)
	}

	if completions != 1 ||
		!errors.Is(
			err,
			asynxmd.ErrDeliveryParked,
		) {
		t.Fatalf(
			"%d: completed %d times with %v, want once with ErrDeliveryParked",
			job.version,
			completions,
			err,
		)
	}

	if got := r.Unrun(
		job.aggregateID,
		job.version,
	); !errors.Is(
		got,
		asynxmd.ErrDeliveryParked,
	) {
		t.Fatalf(
			"Unrun(%d) = %v, want ErrDeliveryParked",
			job.version,
			got,
		)
	}
}

func assertUntouched(
	t *testing.T,
	job *fakeItem,
) {
	t.Helper()
	unrun, completions, _ := job.state()
	if unrun != nil ||
		completions != 0 {
		t.Fatalf(
			"%d: unrun = %v, completions = %d, want untouched",
			job.version,
			unrun,
			completions,
		)
	}
}

func TestEnqueue_ChainsEachJobBehindThePreviousJobOfItsAggregate(
	t *testing.T,
) {
	r, hooks := newRegistry()
	first := enqueue(
		t,
		r,
		durableItem(
			"a",
			1,
		),
	)
	second := enqueue(
		t,
		r,
		liveItem(
			"a",
			2,
		),
	)
	other := enqueue(
		t,
		r,
		durableItem(
			"b",
			1,
		),
	)

	if !closed(first.Turn) {
		t.Fatal("the first job of an idle aggregate has to wait")
	}

	if closed(second.Turn) {
		t.Fatal("the second job of a was let in before the first finished")
	}

	if !closed(other.Turn) {
		t.Fatal("the first job of b waits on another aggregate")
	}

	if hooks.admits() != 3 {
		t.Fatalf(
			"admits = %d, want 3",
			hooks.admits(),
		)
	}
}
func TestFinish_LetsTheNextJobOfTheAggregateIn(
	t *testing.T,
) {
	r, _ := newRegistry()
	first, second, third := liveItem(
		"a",
		1,
	), liveItem(
		"a",
		2,
	), liveItem(
		"a",
		3,
	)
	enqueue(
		t,
		r,
		first,
	)
	next := enqueue(
		t,
		r,
		second,
	)
	last := enqueue(
		t,
		r,
		third,
	)

	r.finish(first)
	if !closed(next.Turn) {
		t.Fatal("Finish did not open the turn of the next job")
	}

	if closed(last.Turn) {
		t.Fatal("Finish opened the turn of a job two places behind")
	}

	r.finish(second)
	if !closed(last.Turn) {
		t.Fatal("the turns did not pass on down the chain")
	}
}
func TestPop_ReturnsJobsInEnqueueOrderWithTheirHalt(
	t *testing.T,
) {
	r, _ := newRegistry()
	first, second := durableItem(
		"a",
		1,
	), liveItem(
		"a",
		2,
	)
	enqueue(
		t,
		r,
		first,
	)
	enqueue(
		t,
		r,
		second,
	)
	got, halt := pop(
		t,
		r,
		"a",
	)
	wrongPop := got != first || halt == nil || closed(halt)
	if wrongPop {
		t.Fatalf(
			"Pop = %d with halt closed=%v, want 1 under an open halt",
			got.version,
			closed(halt),
		)
	}

	if got, _ = pop(
		t,
		r,
		"a",
	); got != second {
		t.Fatalf(
			"Pop = %d, want 2",
			got.version,
		)
	}

	if _, _, ok := r.Pop("a"); ok {
		t.Fatal("Pop of a drained queue found a job")
	}
}

func TestEnqueue_RefusedAfterClose(
	t *testing.T,
) {
	r, hooks := newRegistry()
	r.Close()
	r.Close()

	job := durableItem(
		"a",
		1,
	)
	if _, err := r.Enqueue(job); !errors.Is(
		err,
		asynxmd.ErrDispatcherClosed,
	) {
		t.Fatalf(
			"Enqueue after Close = %v, want ErrDispatcherClosed",
			err,
		)
	}

	if hooks.admits() != 0 {
		t.Fatal("a refused job was admitted")
	}

	assertUntouched(
		t,
		job,
	)
}

func TestPark_KeepsLiveOnlyJobsQueuedAndDropsDurableOnes(
	t *testing.T,
) {
	r, _ := newRegistry()
	running, durable, live := durableItem(
		"a",
		1,
	), durableItem(
		"a",
		2,
	), liveItem(
		"a",
		3,
	)
	enqueue(
		t,
		r,
		running,
	)
	_, halt := pop(
		t,
		r,
		"a",
	)
	enqueue(
		t,
		r,
		durable,
	)
	enqueue(
		t,
		r,
		live,
	)

	r.Park(
		"a",
		errHalted,
	)

	if !closed(halt) {
		t.Fatal("Park left the halt channel of the running delivery open")
	}

	assertDroppedAsParked(
		t,
		r,
		durable,
	)
	assertUntouched(
		t,
		live,
	)
	assertUntouched(
		t,
		running,
	)
	if got, _ := pop(
		t,
		r,
		"a",
	); got != live {
		t.Fatalf(
			"Pop = %d, want the live job",
			got.version,
		)
	}

	if _, _, ok := r.Pop("a"); ok {
		t.Fatal("a durable job stayed queued through the park")
	}

	generation(
		t,
		r,
		"a",
	)
}

func TestPark_QueuedDurableJobsReportThemselvesUnrun(
	t *testing.T,
) {
	r, _ := newRegistry()
	jobs := []*fakeItem{durableItem(
		"a",
		1,
	), durableItem(
		"a",
		2,
	), durableItem(
		"a",
		3,
	)}
	for _, job := range jobs {
		enqueue(
			t,
			r,
			job,
		)
	}

	r.Park(
		"a",
		errHalted,
	)
	for _, job := range jobs {
		assertDroppedAsParked(
			t,
			r,
			job,
		)
	}
}

func TestPark_OfAParkedAggregateMergesTheVersionsAndKeepsTheGeneration(
	t *testing.T,
) {
	r, _ := newRegistry()
	r.Park(
		"a",
		errHalted,
		1,
	)
	g := generation(
		t,
		r,
		"a",
	)
	r.Park(
		"a",
		errHalted,
		2,
	)
	if again := generation(
		t,
		r,
		"a",
	); again != g {
		t.Fatalf(
			"generation %d became %d on a second Park",
			g,
			again,
		)
	}

	if r.Unrun(
		"a",
		1,
	) == nil ||
		r.Unrun(
			"a",
			2,
		) == nil {
		t.Fatal("a second Park dropped or did not record versions")
	}

	if _, undelivered := r.Lift(
		"a",
		g,
	); !slices.Equal(
		undelivered,
		[]int64{1, 2},
	) {
		t.Fatalf(
			"undelivered = %v, want [1 2]",
			undelivered,
		)
	}
}

func TestPark_OfAnAggregateWithoutAQueueStillBlocksItsDurableEvents(
	t *testing.T,
) {
	r, hooks := newRegistry()
	r.Park(
		"ghost",
		errHalted,
	)
	job := durableItem(
		"ghost",
		1,
	)
	if !enqueue(
		t,
		r,
		job,
	).Parked {
		t.Fatal("a durable job of a parked aggregate was admitted")
	}

	assertDroppedAsParked(
		t,
		r,
		job,
	)
	if hooks.admits() != 0 {
		t.Fatal("a refused job was admitted")
	}
}

func TestPark_ADurableEnqueueOnAParkedAggregateIsSettledUnrun(
	t *testing.T,
) {
	r, hooks := newRegistry()
	enqueue(
		t,
		r,
		liveItem(
			"a",
			1,
		),
	)
	r.Park(
		"a",
		errHalted,
	)
	g := generation(
		t,
		r,
		"a",
	)

	job := durableItem(
		"a",
		2,
	)
	if !enqueue(
		t,
		r,
		job,
	).Parked {
		t.Fatal("Admission.Parked is false")
	}

	assertDroppedAsParked(
		t,
		r,
		job,
	)
	if again := generation(
		t,
		r,
		"a",
	); again != g {
		t.Fatalf(
			"refusing a job changed the generation %d to %d",
			g,
			again,
		)
	}

	live := liveItem(
		"a",
		3,
	)
	if enqueue(
		t,
		r,
		live,
	).Parked {
		t.Fatal("a live job was refused by the park")
	}

	pop(
		t,
		r,
		"a",
	)
	if got, _ := pop(
		t,
		r,
		"a",
	); got != live {
		t.Fatalf(
			"Pop = %d, want the live job queued after the park",
			got.version,
		)
	}

	if hooks.admits() != 2 {
		t.Fatalf(
			"admits = %d, want only the two live jobs",
			hooks.admits(),
		)
	}
}

func TestPark_TakesANewGenerationEachTimeItIsImposed(
	t *testing.T,
) {
	r, _ := newRegistry()
	r.Park(
		"a",
		errHalted,
	)
	first := generation(
		t,
		r,
		"a",
	)
	r.Park(
		"b",
		errHalted,
	)
	if generation(
		t,
		r,
		"b",
	) == first {
		t.Fatal("two aggregates share a generation")
	}

	r.Lift(
		"a",
		first,
	)
	r.Park(
		"a",
		errHalted,
	)
	if generation(
		t,
		r,
		"a",
	) == first {
		t.Fatal("a park imposed again kept its old generation")
	}
}

func TestResumeUndelivered_ReturnsWhatTheParkLeftUndeliveredAndForgetsIt(
	t *testing.T,
) {
	r, _ := newRegistry()
	enqueue(
		t,
		r,
		durableItem(
			"a",
			1,
		),
	)
	_, halt := pop(
		t,
		r,
		"a",
	)
	r.Park(
		"a",
		errHalted,
		3,
		1,
	)
	g := generation(
		t,
		r,
		"a",
	)

	lifted, undelivered := r.Lift(
		"a",
		g,
	)
	if !lifted ||
		!slices.Equal(
			undelivered,
			[]int64{1, 3},
		) {
		t.Fatalf(
			"Lift = %v, %v, want true, [1 3]",
			lifted,
			undelivered,
		)
	}

	if len(r.Parks()) != 0 {
		t.Fatalf(
			"Parks = %v after the lift",
			r.Parks(),
		)
	}

	if r.Unrun(
		"a",
		1,
	) != nil ||
		r.Unrun(
			"a",
			3,
		) != nil {
		t.Fatal("Unrun still answers after the lift")
	}

	if lifted, _ := r.Lift(
		"a",
		g,
	); lifted {
		t.Fatal("a second Lift lifted again")
	}

	assertFreshHalt(
		t,
		r,
		halt,
	)
}

func assertFreshHalt(
	t *testing.T,
	r *testRegistry,
	old <-chan struct{},
) {
	t.Helper()
	next := durableItem(
		"a",
		9,
	)
	if enqueue(
		t,
		r,
		next,
	).Parked {
		t.Fatal("a durable job was refused after the lift")
	}

	got, halt := pop(
		t,
		r,
		"a",
	)
	notFresh := got != next || halt == old || closed(halt)
	if notFresh {
		t.Fatal("after the lift the job is not delivered under a fresh open halt")
	}
}

func TestResumeUndelivered_ReturnsTheVersionsAscending(
	t *testing.T,
) {
	r, _ := newRegistry()
	enqueue(
		t,
		r,
		durableItem(
			"a",
			4,
		),
	)
	r.Park(
		"a",
		errHalted,
		9,
		2,
	)
	enqueue(
		t,
		r,
		durableItem(
			"a",
			3,
		),
	)
	r.Park(
		"a",
		errHalted,
		5,
	)
	_, undelivered := r.Lift(
		"a",
		generation(
			t,
			r,
			"a",
		),
	)
	if !slices.Equal(
		undelivered,
		[]int64{2, 3, 4, 5, 9},
	) {
		t.Fatalf(
			"undelivered = %v, want [2 3 4 5 9]",
			undelivered,
		)
	}
}

func TestResumeUndelivered_LeavesAParkImposedAgainSinceItWasListed(
	t *testing.T,
) {
	r, _ := newRegistry()
	r.Park(
		"a",
		errHalted,
		1,
	)
	stale := generation(
		t,
		r,
		"a",
	)
	r.Lift(
		"a",
		stale,
	)
	r.Park(
		"a",
		errHalted,
		2,
	)
	current := generation(
		t,
		r,
		"a",
	)

	lifted, undelivered := r.Lift(
		"a",
		stale,
	)
	if lifted ||
		undelivered != nil {
		t.Fatalf(
			"Lift with a stale generation = %v, %v, want false, nil",
			lifted,
			undelivered,
		)
	}

	if generation(
		t,
		r,
		"a",
	) != current {
		t.Fatal("a stale Lift changed the generation")
	}

	if r.Unrun(
		"a",
		2,
	) == nil {
		t.Fatal("a stale Lift forgot the undelivered versions")
	}
}

func TestResumeUndelivered_WithAStaleGenerationChangesNothing(
	t *testing.T,
) {
	r, _ := newRegistry()
	enqueue(
		t,
		r,
		liveItem(
			"a",
			1,
		),
	)
	_, halt := pop(
		t,
		r,
		"a",
	)
	r.Park(
		"a",
		errHalted,
		7,
	)
	g := generation(
		t,
		r,
		"a",
	)

	if lifted, undelivered := r.Lift(
		"a",
		g+1,
	); lifted ||
		undelivered != nil {
		t.Fatalf(
			"Lift(g+1) = %v, %v, want false, nil",
			lifted,
			undelivered,
		)
	}

	if generation(
		t,
		r,
		"a",
	) != g ||
		r.Unrun(
			"a",
			7,
		) == nil {
		t.Fatal("a stale Lift changed the park")
	}

	job := durableItem(
		"a",
		8,
	)
	if !enqueue(
		t,
		r,
		job,
	).Parked {
		t.Fatal("a stale Lift let a durable job through")
	}

	if !closed(halt) {
		t.Fatal("a stale Lift reopened the halt channel")
	}
}

func TestResumeUndelivered_OfAnUnparkedAggregateDoesNothing(
	t *testing.T,
) {
	r, _ := newRegistry()
	enqueue(
		t,
		r,
		liveItem(
			"a",
			1,
		),
	)
	if lifted, undelivered := r.Lift(
		"a",
		0,
	); lifted ||
		undelivered != nil {
		t.Fatalf(
			"Lift = %v, %v, want false, nil",
			lifted,
			undelivered,
		)
	}
}

func TestSupersede_DropsQueuedDurableJobsAndLeavesNoPark(
	t *testing.T,
) {
	r, _ := newRegistry()
	running, durable, live := durableItem(
		"a",
		1,
	), durableItem(
		"a",
		2,
	), liveItem(
		"a",
		3,
	)
	enqueue(
		t,
		r,
		running,
	)
	_, halt := pop(
		t,
		r,
		"a",
	)
	enqueue(
		t,
		r,
		durable,
	)
	enqueue(
		t,
		r,
		live,
	)

	r.Supersede("a")

	if !closed(halt) {
		t.Fatal("Supersede left the running delivery's halt open")
	}

	unrun, completions, err := durable.state()
	wrongSupersede := !errors.Is(
		unrun,
		asynxmd.ErrDeliveryParked,
	) || completions != 1 || !errors.Is(
		err,
		asynxmd.ErrDeliveryParked,
	)
	if wrongSupersede {
		t.Fatalf(
			"superseded job: unrun %v, completed %d with %v",
			unrun,
			completions,
			err,
		)
	}

	if len(r.Parks()) != 0 {
		t.Fatalf(
			"Parks = %v after Supersede",
			r.Parks(),
		)
	}

	if r.Unrun(
		"a",
		2,
	) != nil {
		t.Fatal("Supersede kept the undelivered record")
	}

	if got, _ := pop(
		t,
		r,
		"a",
	); got != live {
		t.Fatal("Supersede dropped a live job")
	}

	assertFreshHalt(
		t,
		r,
		halt,
	)
}

func TestSupersede_OfAParkedAggregateClearsTheRecord(
	t *testing.T,
) {
	r, hooks := newRegistry()
	r.Park(
		"a",
		errHalted,
		1,
		2,
	)
	r.Supersede("a")
	if len(r.Parks()) != 0 ||
		r.Unrun(
			"a",
			1,
		) != nil {
		t.Fatal("Supersede left the park or its record")
	}

	if !closed(enqueue(
		t,
		r,
		durableItem(
			"a",
			3,
		),
	).Turn) ||
		hooks.admits() != 1 {
		t.Fatal("a durable job after Supersede was not queued at once")
	}
}

func TestParkIfCurrent_WithAnOpenHaltParksTheAggregate(
	t *testing.T,
) {
	r, _ := newRegistry()
	enqueue(
		t,
		r,
		durableItem(
			"a",
			1,
		),
	)
	_, halt := pop(
		t,
		r,
		"a",
	)
	queued := durableItem(
		"a",
		2,
	)
	enqueue(
		t,
		r,
		queued,
	)

	r.ParkIfCurrent(
		"a",
		halt,
		errHalted,
	)
	generation(
		t,
		r,
		"a",
	)
	if !closed(halt) {
		t.Fatal("ParkIfCurrent left the halt open")
	}

	assertDroppedAsParked(
		t,
		r,
		queued,
	)
}

func TestParkIfCurrent_WithAClosedHaltDoesNothing(
	t *testing.T,
) {
	r, _ := newRegistry()
	enqueue(
		t,
		r,
		durableItem(
			"a",
			1,
		),
	)
	_, halt := pop(
		t,
		r,
		"a",
	)
	r.Supersede("a")
	queued := durableItem(
		"a",
		2,
	)
	enqueue(
		t,
		r,
		queued,
	)

	r.ParkIfCurrent(
		"a",
		halt,
		errHalted,
	)
	if len(r.Parks()) != 0 {
		t.Fatalf(
			"Parks = %v: a superseded delivery parked the aggregate again",
			r.Parks(),
		)
	}

	assertUntouched(
		t,
		queued,
	)
}

func TestParkIfCurrent_AfterALiftDoesNotParkAgain(
	t *testing.T,
) {
	r, _ := newRegistry()
	enqueue(
		t,
		r,
		durableItem(
			"a",
			1,
		),
	)
	_, halt := pop(
		t,
		r,
		"a",
	)
	r.Park(
		"a",
		errHalted,
	)
	r.Lift(
		"a",
		generation(
			t,
			r,
			"a",
		),
	)
	r.ParkIfCurrent(
		"a",
		halt,
		errHalted,
	)
	if len(r.Parks()) != 0 {
		t.Fatal("ParkIfCurrent re-imposed a park that was lifted")
	}
}

func TestSettleIfParked_RecordsAParkStillInForce(
	t *testing.T,
) {
	r, _ := newRegistry()
	job := durableItem(
		"a",
		1,
	)
	enqueue(
		t,
		r,
		job,
	)
	popped, halt := pop(
		t,
		r,
		"a",
	)
	r.Park(
		"a",
		errHalted,
	)
	if !r.SettleIfParked(
		"a",
		halt,
		popped,
	) {
		t.Fatal("SettleIfParked of a parked aggregate refused")
	}

	unrun, completions, _ := job.state()
	if !errors.Is(
		unrun,
		asynxmd.ErrDeliveryParked,
	) ||
		completions != 0 {
		t.Fatalf(
			"unrun = %v, completions = %d, want ErrDeliveryParked and not completed",
			unrun,
			completions,
		)
	}

	if !errors.Is(
		r.Unrun(
			"a",
			1,
		),
		asynxmd.ErrDeliveryParked,
	) {
		t.Fatal("a park in force did not record the version")
	}
}

func TestSettleIfParked_AfterTheLiftSettlesWithoutARecord(
	t *testing.T,
) {
	r, _ := newRegistry()
	job := durableItem(
		"a",
		1,
	)
	enqueue(
		t,
		r,
		job,
	)
	popped, halt := pop(
		t,
		r,
		"a",
	)
	r.Park(
		"a",
		errHalted,
	)
	r.Lift(
		"a",
		generation(
			t,
			r,
			"a",
		),
	)

	if !r.SettleIfParked(
		"a",
		halt,
		popped,
	) {
		t.Fatal("SettleIfParked under a closed halt refused")
	}

	if unrun, _, _ := job.state(); !errors.Is(
		unrun,
		asynxmd.ErrDeliveryParked,
	) {
		t.Fatalf(
			"unrun = %v, want ErrDeliveryParked",
			unrun,
		)
	}

	if r.Unrun(
		"a",
		1,
	) != nil {
		t.Fatal("a lifted park left a record that outlives it")
	}

	if len(r.Parks()) != 0 {
		t.Fatal("SettleIfParked parked the aggregate")
	}
}

func TestSettleIfParked_UnderAnOpenHaltDoesNothing(
	t *testing.T,
) {
	r, _ := newRegistry()
	job := durableItem(
		"a",
		1,
	)
	enqueue(
		t,
		r,
		job,
	)
	popped, halt := pop(
		t,
		r,
		"a",
	)
	if r.SettleIfParked(
		"a",
		halt,
		popped,
	) {
		t.Fatal("SettleIfParked of a running aggregate settled the job")
	}

	assertUntouched(
		t,
		job,
	)
}

func TestFinish_KeepsTheChainWhileOtherJobsAreActive(
	t *testing.T,
) {
	r, _ := newRegistry()
	first := liveItem(
		"a",
		1,
	)
	enqueue(
		t,
		r,
		first,
	)
	second := liveItem(
		"a",
		2,
	)
	enqueue(
		t,
		r,
		second,
	)
	pop(
		t,
		r,
		"a",
	)
	r.finish(first)

	if got, _ := pop(
		t,
		r,
		"a",
	); got != second {
		t.Fatal("Finish lost a queued job")
	}

	if closed(enqueue(
		t,
		r,
		liveItem(
			"a",
			3,
		),
	).Turn) {
		t.Fatal("a Finish with a job still active let a new job overtake it")
	}
}
func TestFinish_ReleasesAnIdleAggregate(
	t *testing.T,
) {
	r, hooks := newRegistry()
	first := liveItem(
		"a",
		1,
	)
	enqueue(
		t,
		r,
		first,
	)
	pop(
		t,
		r,
		"a",
	)
	r.finish(first)

	if !closed(enqueue(
		t,
		r,
		liveItem(
			"a",
			2,
		),
	).Turn) ||
		hooks.admits() != 2 {
		t.Fatal("an Enqueue after the last Finish had to wait")
	}
}
func TestFinish_KeepsTheRecordOfAParkedAggregate(
	t *testing.T,
) {
	r, _ := newRegistry()
	first := durableItem(
		"a",
		1,
	)
	enqueue(
		t,
		r,
		first,
	)
	pop(
		t,
		r,
		"a",
	)
	r.Park(
		"a",
		errHalted,
		7,
	)
	g := generation(
		t,
		r,
		"a",
	)
	r.finish(first)

	if generation(
		t,
		r,
		"a",
	) != g {
		t.Fatal("Finish changed the park")
	}

	if r.Unrun(
		"a",
		7,
	) == nil {
		t.Fatal("Finish dropped the undelivered versions")
	}

	if !enqueue(
		t,
		r,
		durableItem(
			"a",
			8,
		),
	).Parked {
		t.Fatal("a durable job passed a park that survived Finish")
	}

	if !closed(enqueue(
		t,
		r,
		liveItem(
			"a",
			9,
		),
	).Turn) {
		t.Fatal("a live job after the last Finish had to wait")
	}

	if _, undelivered := r.Lift(
		"a",
		g,
	); !slices.Equal(
		undelivered,
		[]int64{7, 8},
	) {
		t.Fatalf(
			"undelivered = %v, want [7 8]",
			undelivered,
		)
	}
}
func TestFinish_ThenLiftLeavesNoRecord(
	t *testing.T,
) {
	r, _ := newRegistry()
	first := liveItem(
		"a",
		1,
	)
	enqueue(
		t,
		r,
		first,
	)
	pop(
		t,
		r,
		"a",
	)
	r.Park(
		"a",
		errHalted,
		1,
	)
	r.finish(first)
	r.Lift(
		"a",
		generation(
			t,
			r,
			"a",
		),
	)
	if len(r.Parks()) != 0 ||
		r.Unrun(
			"a",
			1,
		) != nil {
		t.Fatal("a lifted park of a finished aggregate left a record behind")
	}
}
func TestEnqueue_RacingLiftNeverQueuesADurableJobWhileParked(
	t *testing.T,
) {
	const jobs = 2000
	r, _ := newRegistry()
	done := make(chan struct{})
	var violations atomic.Int64
	var wg sync.WaitGroup
	wg.Add(2)
	go parkAndLift(
		&wg,
		r,
		done,
		&violations,
	)
	go popAll(
		&wg,
		r,
		done,
	)

	items := make(
		[]*fakeItem,
		jobs,
	)
	for i := range items {
		items[i] = durableItem(
			"a",
			int64(i+1),
		)
		enqueue(
			t,
			r,
			items[i],
		)
	}

	close(done)
	wg.Wait()
	for job, _, ok := r.Pop("a"); ok; job, _, ok = r.Pop("a") {
		job.Complete(nil)
	}

	if violations.Load() != 0 {
		t.Fatalf(
			"%d durable jobs were queued while the aggregate was parked",
			violations.Load(),
		)
	}

	for _, job := range items {
		assertSettledOnce(
			t,
			job,
		)
	}
}

func TestInspect_RacingParkAndLiftNeverReportsAnInconsistentStatus(
	t *testing.T,
) {
	const reads = 20000
	r, _ := newRegistry()
	done := make(chan struct{})
	var violations atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go parkAndLift(
		&wg,
		r,
		done,
		&violations,
	)

	statuses := make(
		[]asynxmd.DeliveryStatus,
		reads,
	)
	for i := range statuses {
		statuses[i] = r.Inspect("a")
	}

	inconsistent := len(slices.DeleteFunc(
		statuses,
		consistent,
	))
	close(done)
	wg.Wait()

	if inconsistent != 0 {
		t.Fatalf(
			"%d of %d statuses were inconsistent while the park raced its lift",
			inconsistent,
			reads,
		)
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
	r *testRegistry,
	done <-chan struct{},
	violations *atomic.Int64,
) {
	defer wg.Done()
	for !closed(done) {
		r.Park(
			"a",
			errHalted,
		)
		violations.Add(popWhileParked(r))
		r.Lift(
			"a",
			r.Parks()["a"],
		)
	}
}

func popWhileParked(
	r *testRegistry,
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
	r *testRegistry,
	done <-chan struct{},
) {
	defer wg.Done()
	for !closed(done) {
		completePopped(r)
	}
}

func completePopped(
	r *testRegistry,
) {
	job, halt, ok := r.Pop("a")
	if !ok {
		return
	}

	if r.SettleIfParked(
		"a",
		halt,
		job,
	) {
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
		t.Fatalf(
			"%d: completed %d times, want once",
			job.version,
			completions,
		)
	}

	if errors.Is(
		err,
		asynxmd.ErrDeliveryParked,
	) != errors.Is(
		unrun,
		asynxmd.ErrDeliveryParked,
	) {
		t.Fatalf(
			"%d: completed with %v but unrun = %v",
			job.version,
			err,
			unrun,
		)
	}
}

var (
	errHalted = errors.New("test: halted")
	errLater  = errors.New("test: a later cause")
)

func TestInspect_UnknownAggregateIsTheZeroStatus(
	t *testing.T,
) {
	r, _ := newRegistry()
	enqueue(
		t,
		r,
		durableItem(
			"busy",
			1,
		),
	)
	if got := r.Inspect("ghost"); !reflect.DeepEqual(
		got,
		asynxmd.DeliveryStatus{},
	) {
		t.Fatalf(
			"Inspect(ghost) = %+v, want the zero status",
			got,
		)
	}

	if got := r.Inspect("busy"); !reflect.DeepEqual(
		got,
		asynxmd.DeliveryStatus{},
	) {
		t.Fatalf(
			"Inspect(busy) = %+v, want the zero status for a running aggregate",
			got,
		)
	}
}

func TestInspect_ReportsParkGenerationCauseAndUndelivered(
	t *testing.T,
) {
	r, _ := newRegistry()
	enqueue(
		t,
		r,
		durableItem(
			"a",
			4,
		),
	)
	r.Park(
		"a",
		errHalted,
		9,
		2,
	)
	got := r.Inspect("a")
	if !got.Parked ||
		got.Generation != generation(
			t,
			r,
			"a",
		) {
		t.Fatalf(
			"Inspect = %+v, want parked with the generation Parks lists",
			got,
		)
	}

	if !errors.Is(
		got.Cause,
		errHalted,
	) {
		t.Fatalf(
			"Cause = %v, want %v",
			got.Cause,
			errHalted,
		)
	}

	if !slices.Equal(
		got.Undelivered,
		[]int64{2, 4, 9},
	) {
		t.Fatalf(
			"Undelivered = %v, want [2 4 9] ascending",
			got.Undelivered,
		)
	}
}

func TestPark_KeepsTheFirstNonNilCause(
	t *testing.T,
) {
	r, _ := newRegistry()
	r.Park(
		"a",
		errHalted,
		1,
	)
	r.Park(
		"a",
		errLater,
		2,
	)
	r.Park(
		"a",
		nil,
		3,
	)
	got := r.Inspect("a")
	if !errors.Is(
		got.Cause,
		errHalted,
	) ||
		errors.Is(
			got.Cause,
			errLater,
		) {
		t.Fatalf(
			"Cause = %v, want the first one, %v",
			got.Cause,
			errHalted,
		)
	}

	if !slices.Equal(
		got.Undelivered,
		[]int64{1, 2, 3},
	) {
		t.Fatalf(
			"Undelivered = %v, want [1 2 3]",
			got.Undelivered,
		)
	}
}

func TestPark_ALaterCauseReplacesANilOne(
	t *testing.T,
) {
	r, _ := newRegistry()
	r.Park(
		"a",
		nil,
	)
	if got := r.Inspect("a"); !got.Parked ||
		got.Cause != nil {
		t.Fatalf(
			"Inspect = %+v, want parked with no cause",
			got,
		)
	}

	r.Park(
		"a",
		errLater,
	)
	if got := r.Inspect("a"); !errors.Is(
		got.Cause,
		errLater,
	) {
		t.Fatalf(
			"Cause = %v, want %v",
			got.Cause,
			errLater,
		)
	}
}

func TestParkIfCurrent_RecordsTheCause(
	t *testing.T,
) {
	r, _ := newRegistry()
	enqueue(
		t,
		r,
		durableItem(
			"a",
			1,
		),
	)
	_, halt := pop(
		t,
		r,
		"a",
	)
	r.ParkIfCurrent(
		"a",
		halt,
		errHalted,
	)
	if got := r.Inspect("a"); !got.Parked ||
		!errors.Is(
			got.Cause,
			errHalted,
		) {
		t.Fatalf(
			"Inspect = %+v, want parked with %v",
			got,
			errHalted,
		)
	}
}

func TestInspect_GenerationChangesWhenAParkIsImposedAgain(
	t *testing.T,
) {
	r, _ := newRegistry()
	r.Park(
		"a",
		errHalted,
	)
	first := r.Inspect("a").Generation
	r.Park(
		"a",
		errLater,
	)
	if again := r.Inspect("a").Generation; again != first {
		t.Fatalf(
			"generation %d became %d while still parked",
			first,
			again,
		)
	}

	r.Lift(
		"a",
		first,
	)
	r.Park(
		"a",
		errLater,
	)
	got := r.Inspect("a")
	notReimposed := !got.Parked || got.Generation == first || got.Generation == 0
	if notReimposed {
		t.Fatalf(
			"Inspect = %+v, want parked with a generation other than %d",
			got,
			first,
		)
	}

	if !errors.Is(
		got.Cause,
		errLater,
	) {
		t.Fatalf(
			"Cause = %v, want the new park's %v",
			got.Cause,
			errLater,
		)
	}
}

func TestInspect_IsClearAfterTheParkLifts(
	t *testing.T,
) {
	r, _ := newRegistry()
	enqueue(
		t,
		r,
		liveItem(
			"a",
			1,
		),
	)
	r.Park(
		"a",
		errHalted,
		3,
	)
	r.Lift(
		"a",
		generation(
			t,
			r,
			"a",
		),
	)
	if got := r.Inspect("a"); !reflect.DeepEqual(
		got,
		asynxmd.DeliveryStatus{},
	) {
		t.Fatalf(
			"Inspect after the lift = %+v, want the zero status",
			got,
		)
	}

	r.Park(
		"a",
		nil,
	)
	if got := r.Inspect("a"); got.Cause != nil {
		t.Fatalf(
			"Cause = %v after a lift and a nil-cause park, want nil",
			got.Cause,
		)
	}
}

func TestBegin_RefusesAJobAParkDropped(
	t *testing.T,
) {
	r, _ := newRegistry()
	running, dropped, kept := durableItem(
		"a",
		1,
	), durableItem(
		"a",
		2,
	), liveItem(
		"a",
		3,
	)
	enqueue(
		t,
		r,
		running,
	)
	enqueue(
		t,
		r,
		dropped,
	)
	enqueue(
		t,
		r,
		kept,
	)
	if _, ok := r.Begin(running); !ok {
		t.Fatal("Begin refused the head of the queue")
	}

	r.Park(
		"a",
		errHalted,
	)
	if _, ok := r.Begin(dropped); ok {
		t.Fatal("Begin let a job the park dropped run")
	}

	if _, ok := r.Begin(kept); !ok {
		t.Fatal("Begin refused a live job a park kept queued")
	}
}

func TestBegin_RefusesAJobThatIsNotAtTheHead(
	t *testing.T,
) {
	r, _ := newRegistry()
	head, behind := liveItem(
		"a",
		1,
	), liveItem(
		"a",
		2,
	)
	enqueue(
		t,
		r,
		head,
	)
	enqueue(
		t,
		r,
		behind,
	)
	if _, ok := r.Begin(behind); ok {
		t.Fatal("Begin let a job overtake the head of its queue")
	}
}
