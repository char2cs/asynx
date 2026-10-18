package aggregates

import (
	"maps"
	"slices"
	"sync"

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
) (Admission, error) {
	admission, err := r.enqueue(job)
	if admission.Parked {
		job.Complete(asynxmd.ErrDeliveryParked)
	}
	return admission, err
}

func (r *aggregateRegistry[J]) enqueue(
	job J,
) (Admission, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return Admission{}, asynxmd.ErrDispatcherClosed
	}
	agg := job.AggregateID()
	current, exists := r.records[agg]
	if exists && current.parked && job.Durable() {
		r.markParkedUnrun(current, job)
		return Admission{Parked: true}, nil
	}
	rec := r.recordOf(agg)
	spawned := r.ensureWorker(agg, rec)
	r.admit(job)
	rec.jobs = append(rec.jobs, job)
	signal(rec.wake)
	return Admission{Spawned: spawned}, nil
}

// Pop removes and returns the aggregate's oldest queued job with the halt
// channel it is delivered under (see Registry.Pop).
func (r *aggregateRegistry[J]) Pop(
	aggregateID string,
) (J, <-chan struct{}, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var zero J
	rec, ok := r.records[aggregateID]
	if !ok || len(rec.jobs) == 0 {
		return zero, nil, false
	}
	job := rec.jobs[0]
	rec.jobs[0] = zero
	rec.jobs = rec.jobs[1:]
	return job, rec.halt, true
}

// Wake returns the aggregate's wake channel, nil when it has no record (see
// Registry.Wake).
func (r *aggregateRegistry[J]) Wake(
	aggregateID string,
) <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.records[aggregateID]
	if !ok {
		return nil
	}
	return rec.wake
}

// Retire releases the aggregate's idle worker, keeping a record that holds a
// park or undelivered versions (see Registry.Retire).
func (r *aggregateRegistry[J]) Retire(
	aggregateID string,
) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.records[aggregateID]
	if !ok {
		return true
	}
	if len(rec.jobs) > 0 {
		return false
	}
	rec.worker = false
	r.discardIfEmpty(aggregateID, rec)
	return true
}

// Park parks the aggregate and records the given versions as undelivered (see
// Registry.Park).
func (r *aggregateRegistry[J]) Park(
	aggregateID string,
	undelivered ...int64,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.park(aggregateID)
	for _, v := range undelivered {
		rec.undelivered[v] = asynxmd.ErrDeliveryParked
	}
}

// ParkIfCurrent parks a durable job's aggregate unless halt has been closed
// since the job was popped (see Registry.ParkIfCurrent).
func (r *aggregateRegistry[J]) ParkIfCurrent(
	aggregateID string,
	halt <-chan struct{},
	durable bool,
) bool {
	if !durable {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if isClosed(halt) {
		return false
	}
	r.park(aggregateID)
	return true
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
	if ok && rec.parked {
		r.markParkedUnrun(rec, job)
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
	if !ok || !rec.parked || rec.generation != generation {
		return false, nil
	}
	undelivered := slices.Sorted(maps.Keys(rec.undelivered))
	r.lift(aggregateID, rec)
	return true, undelivered
}

// Supersede parks the aggregate and lifts the park at once (see
// Registry.Supersede).
func (r *aggregateRegistry[J]) Supersede(
	aggregateID string,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.park(aggregateID)
	r.lift(aggregateID, rec)
}

// Parks returns a copy of the parked aggregates and their generations.
func (r *aggregateRegistry[J]) Parks() map[string]uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	parks := make(map[string]uint64)
	for agg, rec := range r.records {
		addPark(parks, agg, rec)
	}
	return parks
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

// Close refuses every later Enqueue and reports whether this call closed the
// registry.
func (r *aggregateRegistry[J]) Close() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	first := !r.closed
	r.closed = true
	return first
}

func (r *aggregateRegistry[J]) recordOf(
	agg string,
) *record[J] {
	rec, ok := r.records[agg]
	if ok {
		return rec
	}
	rec = &record[J]{
		halt:        make(chan struct{}),
		undelivered: make(map[int64]error),
	}
	r.records[agg] = rec
	return rec
}

func (r *aggregateRegistry[J]) ensureWorker(
	agg string,
	rec *record[J],
) bool {
	if rec.worker {
		return false
	}
	rec.worker = true
	rec.wake = make(chan struct{}, 1)
	rec.halt = make(chan struct{})
	r.spawn(agg)
	return true
}

func (r *aggregateRegistry[J]) spawn(
	agg string,
) {
	if r.hooks.Spawn == nil {
		return
	}
	r.hooks.Spawn(agg)
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
) *record[J] {
	rec := r.recordOf(agg)
	if rec.parked {
		return rec
	}
	r.generation++
	rec.parked = true
	rec.generation = r.generation
	close(rec.halt)
	kept := make([]J, 0, len(rec.jobs))
	for _, job := range rec.jobs {
		kept = r.keepLive(rec, job, kept)
	}
	rec.jobs = kept
	return rec
}

func (r *aggregateRegistry[J]) keepLive(
	rec *record[J],
	job J,
	kept []J,
) []J {
	if !job.Durable() {
		return append(kept, job)
	}
	r.markParkedUnrun(rec, job)
	job.Complete(asynxmd.ErrDeliveryParked)
	return kept
}

func (r *aggregateRegistry[J]) lift(
	agg string,
	rec *record[J],
) {
	rec.parked = false
	rec.generation = 0
	rec.undelivered = make(map[int64]error)
	rec.halt = make(chan struct{})
	r.discardIfEmpty(agg, rec)
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
	if rec.worker || len(rec.jobs) > 0 || rec.parked || len(rec.undelivered) > 0 {
		return
	}
	delete(r.records, agg)
}

func addPark[J Item](
	parks map[string]uint64,
	agg string,
	rec *record[J],
) {
	if !rec.parked {
		return
	}
	parks[agg] = rec.generation
}

func signal(
	ch chan struct{},
) {
	select {
	case ch <- struct{}{}:
	default:
	}
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
