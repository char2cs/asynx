package aggregates

import (
	"maps"
	"slices"
	"sync"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/aggregates/contracts"
	"github.com/char2cs/asynx/models/dispatch"
	"github.com/char2cs/asynx/models/failure"
)

type aggregateRegistry[J Item] struct {
	mu         sync.Mutex
	records    map[string]*record[J]
	generation uint64
	closed     bool
	hooks      Hooks[J]
}

// Enqueue appends job to its aggregate's FIFO, or refuses it as described on
// Registry.Enqueue, and writes what became of it into admission. A refused
// durable job is completed after the mutex is released.
func (r *aggregateRegistry[J]) Enqueue(
	job J,
	admission *contracts.Admission,
) {
	r.queueUnderLock(
		job,
		admission,
	)
	if admission.Parked {
		job.Complete(failure.ErrDeliveryParked)
	}
}

func (r *aggregateRegistry[J]) queueUnderLock(
	job J,
	admission *contracts.Admission,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		*admission = contracts.Admission{Err: failure.ErrDispatcherClosed}
		return
	}

	agg := job.AggregateID()
	current, exists := r.records[agg]
	parked := exists && current.parked
	if parked &&
		job.Acked() {
		r.markParkedUnrun(
			current,
			job,
		)
		*admission = contracts.Admission{Parked: true}
		return
	}

	r.open(agg)
	rec := r.records[agg]
	done := make(chan struct{})
	*admission = contracts.Admission{
		Turn: rec.tail,
		Done: done,
	}
	rec.tail = done
	rec.active++
	r.admit(job)
	rec.jobs = append(
		rec.jobs,
		job,
	)
}

// Begin removes job from its aggregate's queue and returns the halt channel
// it is delivered under (see Registry.Begin).
func (r *aggregateRegistry[J]) Begin(
	job J,
) (halt <-chan struct{}, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, found := r.records[job.AggregateID()]
	if !found ||
		!rec.isHead(job) {
		return nil,
			false
	}

	clear(rec.jobs[:1])
	rec.jobs = rec.jobs[1:]
	return rec.halt,
		true
}

// Finish ends the turn of the aggregate's job that holds done (see
// Registry.Finish).
func (r *aggregateRegistry[J]) Finish(
	aggregateID string,
	done chan<- struct{},
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	close(done)
	rec, ok := r.records[aggregateID]
	if !ok {
		return
	}

	rec.active--
	r.discardIfEmpty(
		aggregateID,
		rec,
	)
}

// Park parks the aggregate for cause and records the given versions as
// undelivered (see Registry.Park).
func (r *aggregateRegistry[J]) Park(
	aggregateID string,
	cause error,
	undelivered ...int64,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.park(
		aggregateID,
		cause,
	)
	rec := r.records[aggregateID]
	for _, v := range undelivered {
		rec.undelivered[v] = failure.ErrDeliveryParked
	}
}

// ParkIfCurrent parks the aggregate unless halt has been closed
// since the job was popped (see Registry.ParkIfCurrent).
func (r *aggregateRegistry[J]) ParkIfCurrent(
	aggregateID string,
	halt <-chan struct{},
	cause error,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if isClosed(halt) {
		return
	}

	r.park(
		aggregateID,
		cause,
	)
}

// SettleIfParked settles job as unrun if its aggregate is parked, or was
// parked since halt was taken (see Registry.SettleIfParked).
func (r *aggregateRegistry[J]) SettleIfParked(
	aggregateID string,
	halt <-chan struct{},
	job J,
) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.records[aggregateID]
	if ok &&
		rec.parked {
		r.markParkedUnrun(
			rec,
			job,
		)
		return true
	}

	if !isClosed(halt) {
		return false
	}

	job.MarkUnrun(failure.ErrDeliveryParked)
	return true
}

// Lift lifts the park of the given generation and writes whether it did, with
// the undelivered versions in ascending order, into outcome (see
// Registry.Lift).
func (r *aggregateRegistry[J]) Lift(
	aggregateID string,
	generation uint64,
	outcome *contracts.LiftOutcome,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.records[aggregateID]
	current := ok && rec.parked && rec.generation == generation
	if !current {
		*outcome = contracts.LiftOutcome{}
		return
	}

	*outcome = contracts.LiftOutcome{
		Lifted:      true,
		Undelivered: slices.Sorted(maps.Keys(rec.undelivered)),
	}
	r.lift(
		aggregateID,
		rec,
	)
}

// Supersede parks the aggregate and lifts the park at once (see
// Registry.Supersede).
func (r *aggregateRegistry[J]) Supersede(
	aggregateID string,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.park(
		aggregateID,
		nil,
	)
	r.lift(
		aggregateID,
		r.records[aggregateID],
	)
}

// Parks returns a copy of the parked aggregates and their generations.
func (r *aggregateRegistry[J]) Parks() map[string]uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	parks := make(map[string]uint64)
	for agg, rec := range r.records {
		parks[agg] = rec.generation
	}

	maps.DeleteFunc(
		parks,
		isNotParked,
	)

	return parks
}

// Inspect reports the aggregate's park, its cause and its undelivered versions,
// read under one hold of the mutex (see Registry.Inspect).
func (r *aggregateRegistry[J]) Inspect(
	aggregateID string,
) dispatch.Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.records[aggregateID]
	if !ok ||
		!rec.parked {
		return dispatch.Status{}
	}

	return dispatch.Status{
		Parked:      true,
		Generation:  rec.generation,
		Cause:       rec.cause,
		Undelivered: undeliveredOf(rec),
	}
}

// Unrun reports ErrDeliveryParked for a version the aggregate's park left
// undelivered, nil otherwise.
func (r *aggregateRegistry[J]) Unrun(
	aggregateID string,
	version int64,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.records[aggregateID]
	if !ok {
		return nil
	}

	return rec.undelivered[version]
}

// Close refuses every later Enqueue; closing a closed registry changes
// nothing.
func (r *aggregateRegistry[J]) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
}

func (r *aggregateRegistry[J]) open(
	agg string,
) {
	if _, ok := r.records[agg]; ok {
		return
	}

	r.records[agg] = &record[J]{
		tail:        turnReady(),
		halt:        make(chan struct{}),
		undelivered: make(map[int64]error),
	}
}

func (r *aggregateRegistry[J]) admit(
	job J,
) {
	if r.hooks.Admit == nil {
		return
	}

	r.hooks.Admit(job)
}

func (r *aggregateRegistry[J]) park(
	agg string,
	cause error,
) {
	r.open(agg)
	rec := r.records[agg]
	if rec.cause == nil {
		rec.cause = cause
	}

	if rec.parked {
		return
	}

	r.generation++
	rec.parked = true
	rec.generation = r.generation
	close(rec.halt)
	r.dropAcked(rec)
}

func (r *aggregateRegistry[J]) dropAcked(
	rec *record[J],
) {
	queued := slices.DeleteFunc(
		slices.Clone(rec.jobs),
		J.Acked,
	)
	dropped := slices.DeleteFunc(
		slices.Clone(rec.jobs),
		isLive[J],
	)
	rec.jobs = queued
	for _, job := range dropped {
		r.markParkedUnrun(
			rec,
			job,
		)
		job.Complete(failure.ErrDeliveryParked)
	}
}

func (r *aggregateRegistry[J]) lift(
	agg string,
	rec *record[J],
) {
	rec.parked = false
	rec.generation = noPark
	rec.cause = nil
	rec.undelivered = make(map[int64]error)
	rec.halt = make(chan struct{})
	r.discardIfEmpty(
		agg,
		rec,
	)
}

func (r *aggregateRegistry[J]) markParkedUnrun(
	rec *record[J],
	job J,
) {
	job.MarkUnrun(failure.ErrDeliveryParked)
	rec.undelivered[job.Version()] = failure.ErrDeliveryParked
}

func (r *aggregateRegistry[J]) discardIfEmpty(
	agg string,
	rec *record[J],
) {
	busy := rec.active > 0
	held := rec.parked || len(rec.undelivered) > 0
	if busy ||
		held {
		return
	}

	delete(
		r.records,
		agg,
	)
}

func undeliveredOf[J Item](
	rec *record[J],
) []int64 {
	if len(rec.undelivered) == 0 {
		return nil
	}

	return slices.Sorted(maps.Keys(rec.undelivered))
}

func turnReady() <-chan struct{} {
	ready := make(chan struct{})
	close(ready)
	return ready
}

func isClosed(
	ch <-chan struct{},
) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func isNotParked(
	_ string,
	generation uint64,
) bool {
	return generation == noPark
}

func isLive[J Item](
	job J,
) bool {
	return !job.Acked()
}
