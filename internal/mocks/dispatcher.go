package mocks

import (
	"context"
	"maps"
	"slices"
	"sync"

	"github.com/char2cs/asynx/models"
)

// Dispatcher is a recording fake of models.Dispatcher. It records every call
// in order and every request it is handed, settles each ticket at once, and
// lets a test script an Enqueue or Close failure. It delivers nothing. It keeps
// the parks it is told of, with their first non-nil cause, for Parks and
// Inspect.
type Dispatcher[T any] struct {
	mu          sync.Mutex
	config      models.DispatcherConfig[T]
	calls       []string
	requests    []models.DispatchRequest[T]
	parkCalls   []ParkCall
	parks       map[string]uint64
	causes      map[string]error
	nextPark    uint64
	enqueueErr  error
	closeErr    error
	interrupted chan struct{}
	interrupt   sync.Once
}

// NewDispatcher returns a fake that records and settles.
func NewDispatcher[T any]() *Dispatcher[T] {
	return &Dispatcher[T]{
		parks:       make(map[string]uint64),
		causes:      make(map[string]error),
		interrupted: make(chan struct{}),
	}
}

// Factory returns a DispatcherFactory that hands out d and keeps the
// configuration it was called with, which Config returns.
func (d *Dispatcher[T]) Factory() models.DispatcherFactory[T] {
	return func(
		config models.DispatcherConfig[T],
	) (models.Dispatcher[T], error) {
		d.mu.Lock()
		defer d.mu.Unlock()
		d.config = config
		d.calls = append(d.calls, "Factory")
		return d, nil
	}
}

// FailEnqueue makes every later Enqueue return err; nil restores success.
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
func (d *Dispatcher[T]) Config() models.DispatcherConfig[T] {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.config
}

// Calls returns the names of the methods called, in order.
func (d *Dispatcher[T]) Calls() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.calls)
}

// Requests returns the requests Enqueue accepted, in order.
func (d *Dispatcher[T]) Requests() []models.DispatchRequest[T] {
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

func (d *Dispatcher[T]) Enqueue(
	_ context.Context,
	request models.DispatchRequest[T],
) (models.Ticket, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, "Enqueue")
	if d.enqueueErr != nil {
		return models.Ticket{}, d.enqueueErr
	}
	d.requests = append(d.requests, request)
	return settledTicket(), nil
}

func (d *Dispatcher[T]) TicketFor(
	_ string,
	_ int64,
) (models.Ticket, bool) {
	d.record("TicketFor")
	return models.Ticket{}, false
}

func (d *Dispatcher[T]) Unrun(
	_ string,
	_ int64,
) error {
	d.record("Unrun")
	return nil
}

func (d *Dispatcher[T]) Park(
	aggregateID string,
	cause error,
	undelivered ...int64,
) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, "Park")
	d.parkCalls = append(d.parkCalls, ParkCall{AggregateID: aggregateID, Cause: cause, Undelivered: undelivered})
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
) models.DeliveryStatus {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, "Inspect")
	generation, parked := d.parks[aggregateID]
	if !parked {
		return models.DeliveryStatus{}
	}
	return models.DeliveryStatus{Parked: true, Generation: generation, Cause: d.causes[aggregateID]}
}

func (d *Dispatcher[T]) Supersede(
	aggregateID string,
) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, "Supersede")
	delete(d.parks, aggregateID)
	delete(d.causes, aggregateID)
}

func (d *Dispatcher[T]) Parks() map[string]uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, "Parks")
	return maps.Clone(d.parks)
}

func (d *Dispatcher[T]) ResumeUndelivered(
	aggregateID string,
	generation uint64,
) (bool, []int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, "ResumeUndelivered")
	if d.parks[aggregateID] != generation {
		return false, nil
	}
	delete(d.parks, aggregateID)
	delete(d.causes, aggregateID)
	return true, nil
}

func (d *Dispatcher[T]) InHandler(
	_ context.Context,
	_ string,
) bool {
	return false
}

func (d *Dispatcher[T]) Interrupt() {
	d.record("Interrupt")
	d.interrupt.Do(func() { close(d.interrupted) })
}

func (d *Dispatcher[T]) Interrupted() <-chan struct{} {
	return d.interrupted
}

func (d *Dispatcher[T]) Close(
	_ context.Context,
) error {
	d.record("Close")
	d.interrupt.Do(func() { close(d.interrupted) })
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.closeErr
}

func (d *Dispatcher[T]) WaitIdle() {
	d.record("WaitIdle")
}

func (d *Dispatcher[T]) record(
	call string,
) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, call)
}

func settledTicket() models.Ticket {
	done := make(chan struct{})
	close(done)
	return models.Ticket{
		Settled: done,
		Final:   done,
		Err:     func() error { return nil },
		Unrun:   func() error { return nil },
	}
}
