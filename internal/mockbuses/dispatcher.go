package mockbuses

import (
	"context"
	"maps"
	"slices"
	"sync"

	"github.com/char2cs/asynx/v2/models/dispatch"
	"github.com/char2cs/asynx/v2/models/dispatcher"
)

// Dispatcher is a recording fake of dispatcher.Dispatcher. It records every
// command it receives (the factory call, Enqueue, Unrun, Park, Supersede,
// ResumeUndelivered, Interrupt, Close and WaitIdle) in order, and every request
// it is handed, settles each ticket at once, and lets a test script an Enqueue
// or Close failure. It delivers nothing. It keeps the parks it is told of, with
// their first non-nil cause, for Parks and Inspect. The read-only methods
// (TicketFor, Inspect, Parks, InHandler) change nothing and are not recorded,
// so Calls shows only what altered or drove the dispatcher. It is safe for
// concurrent use.
type Dispatcher[T any] struct {
	mu          sync.Mutex
	config      dispatcher.Config[T]
	calls       []string
	requests    []dispatch.Request[T]
	parkCalls   []ParkCall
	parks       map[string]uint64
	causes      map[string]error
	nextPark    uint64
	enqueueErr  error
	closeErr    error
	interrupted chan struct{}
	interrupt   sync.Once
}

// NewDispatcher returns a fake with no parks, no scripted failure and an open
// Interrupted channel, ready to be handed to a Builder through Factory.
func NewDispatcher[T any]() *Dispatcher[T] {
	return &Dispatcher[T]{
		parks:       make(map[string]uint64),
		causes:      make(map[string]error),
		interrupted: make(chan struct{}),
	}
}

// Factory returns a dispatcher.Factory that hands out d and keeps the
// configuration it was called with, which Config returns.
func (d *Dispatcher[T]) Factory() dispatcher.Factory[T] {
	return func(
		config dispatcher.Config[T],
	) (dispatcher.Dispatcher[T], error) {
		d.configure(config)
		return d,
			nil
	}
}

// FailEnqueue makes every later Enqueue return err instead of accepting the
// request; nil restores success. The failed Enqueue is still recorded in Calls
// but its request is not kept in Requests.
func (d *Dispatcher[T]) FailEnqueue(
	err error,
) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.enqueueErr = err
}

// FailClose makes Close return err; nil restores success.
func (d *Dispatcher[T]) FailClose(
	err error,
) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closeErr = err
}

// Config returns the configuration the factory was called with.
func (d *Dispatcher[T]) Config() dispatcher.Config[T] {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.config
}

// Calls returns a copy of the names of the commands received, in order. The
// read-only methods never appear in it.
func (d *Dispatcher[T]) Calls() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.calls)
}

// Requests returns the requests Enqueue accepted, in order.
func (d *Dispatcher[T]) Requests() []dispatch.Request[T] {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.requests)
}

// ParkCalls returns the calls to Park, in order.
func (d *Dispatcher[T]) ParkCalls() []ParkCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.parkCalls)
}

// Enqueue records the request and returns a ticket that is already settled, or the error set by FailEnqueue.
func (d *Dispatcher[T]) Enqueue(
	_ context.Context,
	request dispatch.Request[T],
) (dispatch.Ticket, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.recordLocked("Enqueue")
	if d.enqueueErr != nil {
		return dispatch.Ticket{},
			d.enqueueErr
	}

	d.addRequestLocked(request)
	return settledTicket(),
		nil
}

// TicketFor reports that no ticket is held: the fake settles every ticket at
// Enqueue and keeps none.
func (d *Dispatcher[T]) TicketFor(
	_ string,
	_ int64,
) (dispatch.Ticket, bool) {
	return dispatch.Ticket{},
		false
}

// Unrun records the call and succeeds.
func (d *Dispatcher[T]) Unrun(
	_ string,
	_ int64,
) error {
	d.record("Unrun")
	return nil
}

// Park records the call and parks the aggregate, keeping the first non-nil cause.
func (d *Dispatcher[T]) Park(
	aggregateID string,
	cause error,
	undelivered ...int64,
) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.recordLocked("Park")
	d.parkCalls = append(
		d.parkCalls,
		ParkCall{AggregateID: aggregateID, Cause: cause, Undelivered: undelivered},
	)
	if d.causes[aggregateID] == nil {
		d.causes[aggregateID] = cause
	}

	if _, parked := d.parks[aggregateID]; parked {
		return
	}

	d.nextPark++
	d.parks[aggregateID] = d.nextPark
}

// Inspect reports whether the aggregate is parked, the park's generation and
// the first non-nil cause it was parked with. The fake keeps no undelivered
// versions, so Undelivered is always nil.
func (d *Dispatcher[T]) Inspect(
	aggregateID string,
) dispatch.Status {
	d.mu.Lock()
	defer d.mu.Unlock()
	generation, parked := d.parks[aggregateID]
	if !parked {
		return dispatch.Status{}
	}

	return dispatch.Status{Parked: true, Generation: generation, Cause: d.causes[aggregateID]}
}

// Supersede records the call and forgets the aggregate's park.
func (d *Dispatcher[T]) Supersede(
	aggregateID string,
) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.recordLocked("Supersede")
	d.liftLocked(aggregateID)
}

// Parks returns a copy of the parked aggregates with their generations.
func (d *Dispatcher[T]) Parks() map[string]uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return maps.Clone(d.parks)
}

// ResumeUndelivered records the call and lifts the aggregate's park when
// generation is the one Park imposed, reporting true. It reports false and
// changes nothing for an unknown aggregate or a stale generation. The undelivered
// versions are always nil: the fake keeps none. Lifting and reporting happen in
// one call because dispatcher.Dispatcher defines this method that way, so the fake
// cannot split them.
func (d *Dispatcher[T]) ResumeUndelivered(
	aggregateID string,
	generation uint64,
) (bool, []int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.recordLocked("ResumeUndelivered")
	if d.parks[aggregateID] != generation {
		return false,
			nil
	}

	d.liftLocked(aggregateID)
	return true,
		nil
}

// InHandler reports false: the fake runs no handler.
func (d *Dispatcher[T]) InHandler(
	_ context.Context,
	_ string,
) bool {
	return false
}

// Interrupt records the call and closes the Interrupted channel.
func (d *Dispatcher[T]) Interrupt() {
	d.record("Interrupt")
	d.interrupt.Do(func() { close(d.interrupted) })
}

// Interrupted returns the channel Interrupt and Close close.
func (d *Dispatcher[T]) Interrupted() <-chan struct{} {
	return d.interrupted
}

// Close records the call, closes the Interrupted channel and returns the error set by FailClose.
func (d *Dispatcher[T]) Close(
	_ context.Context,
) error {
	d.record("Close")
	d.interrupt.Do(func() { close(d.interrupted) })
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.closeErr
}

// WaitIdle records the call and returns at once.
func (d *Dispatcher[T]) WaitIdle() {
	d.record("WaitIdle")
}

func (d *Dispatcher[T]) record(
	call string,
) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.recordLocked(call)
}

func (d *Dispatcher[T]) recordLocked(
	call string,
) {
	d.calls = append(
		d.calls,
		call,
	)
}

func (d *Dispatcher[T]) configure(
	config dispatcher.Config[T],
) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.config = config
	d.recordLocked("Factory")
}

func (d *Dispatcher[T]) addRequestLocked(
	request dispatch.Request[T],
) {
	d.requests = append(
		d.requests,
		request,
	)
}

func (d *Dispatcher[T]) liftLocked(
	aggregateID string,
) {
	delete(
		d.parks,
		aggregateID,
	)
	delete(
		d.causes,
		aggregateID,
	)
}

func settledTicket() dispatch.Ticket {
	done := make(chan struct{})
	close(done)
	return dispatch.Ticket{
		Settled: done,
		Final:   done,
		Err:     func() error { return nil },
		Unrun:   func() error { return nil },
	}
}
