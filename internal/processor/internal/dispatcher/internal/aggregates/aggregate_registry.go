package aggregates

import (
	"maps"
	"slices"
	"sync"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/aggregates/contracts"
	asynxmd "github.com/char2cs/asynx/models"
)

type aggregateRegistry[J Item] struct {
	mu         sync.Mutex
	records    map[string]*record[J]
	generation uint64
	closed     bool
	hooks      Hooks[J]
}

// Enqueue appends job to its aggregate's FIFO, or refuses it as described on
// Registry.Enqueue. A refused durable job is completed after the mutex is
// released.
func (r *aggregateRegistry[J]) Enqueue(
	job J,
) (contracts.Admission, error) {
	admission, err := r.queueUnderLock(job)
	if admission.Parked {
		job.Complete(asynxmd.ErrDeliveryParked)
	}

	return admission,
		err
}

func (r *aggregateRegistry[J]) queueUnderLock(
	job J,
) (contracts.Admission, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return contracts.Admission{},
			asynxmd.ErrDispatcherClosed
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
		return contracts.Admission{Parked: true},
			nil
	}

	rec := r.recordOf(agg)
	done := make(chan struct{})
	admission := contracts.Admission{
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
	return admission,
		nil
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
	rec := r.park(
		aggregateID,
		cause,
	)
	for _, v := range undelivered {
		rec.undelivered[v] = asynxmd.ErrDeliveryParked
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

	job.MarkUnrun(asynxmd.ErrDeliveryParked)
	return true
}

// Lift lifts the park of the given generation and returns its undelivered
// versions in ascending order (see Registry.Lift).
func (r *aggregateRegistry[J]) Lift(
	aggregateID string,
	generation uint64,
) (bool, []int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.records[aggregateID]
	current := ok && rec.parked && rec.generation == generation
	if !current {
		return false,
			nil
	}

	undelivered := slices.Sorted(maps.Keys(rec.undelivered))
	r.lift(
		aggregateID,
		rec,
	)
	return true,
		undelivered
}

// Supersede parks the aggregate and lifts the park at once (see
// Registry.Supersede).
func (r *aggregateRegistry[J]) Supersede(
	aggregateID string,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.park(
		aggregateID,
		nil,
	)
	r.lift(
		aggregateID,
		rec,
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
) asynxmd.DeliveryStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.records[aggregateID]
	if !ok ||
		!rec.parked {
		return asynxmd.DeliveryStatus{}
	}

	return asynxmd.DeliveryStatus{
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

func (r *aggregateRegistry[J]) recordOf(
	agg string,
) *record[J] {
	rec, ok := r.records[agg]
	if ok {
		return rec
	}

	rec = &record[J]{
		tail:        turnReady(),
		halt:        make(chan struct{}),
		undelivered: make(map[int64]error),
	}
	r.records[agg] = rec
	return rec
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
) *record[J] {
	rec := r.recordOf(agg)
	if rec.cause == nil {
		rec.cause = cause
	}

	if rec.parked {
		return rec
	}

	r.generation++
	rec.parked = true
	rec.generation = r.generation
	close(rec.halt)
	r.dropAcked(rec)
	return rec
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
		job.Complete(asynxmd.ErrDeliveryParked)
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
	job.MarkUnrun(asynxmd.ErrDeliveryParked)
	rec.undelivered[job.Version()] = asynxmd.ErrDeliveryParked
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
