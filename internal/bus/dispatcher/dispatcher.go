package dispatcher

import (
	"context"
	"sync"
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

const (
	defaultBufferSize = 16
	defaultIdleTimeout = 5 * time.Second
)

// Opt is a functional option for configuring a Dispatcher.
type Opt[T any] func(*Dispatcher[T])

// WithPublishErrorHandler sets the callback invoked when bus.PublishSync
// returns a non-nil error. When nil (the default), errors are silently dropped.
func WithPublishErrorHandler[T any](fn asynxmd.PublishErrorHandler[T]) Opt[T] {
	return func(d *Dispatcher[T]) { d.onPublishError = fn }
}

// WithIdleTimeout sets how long a per-aggregate worker goroutine waits for new
// events before cleaning itself up. Zero or negative values are ignored.
func WithIdleTimeout[T any](t time.Duration) Opt[T] {
	return func(d *Dispatcher[T]) {
		if t > 0 {
			d.idleTimeout = t
		}
	}
}

// Dispatcher provides per-aggregate ordered event delivery by maintaining a
// FIFO channel and dedicated worker goroutine for each aggregate that has
// in-flight events. Events for the same aggregate are delivered sequentially
// via bus.PublishSync; different aggregates are delivered concurrently.
type Dispatcher[T any] struct {
	bus            asynxmd.Bus[T]
	mu             sync.Mutex
	queues         map[string]*aggregateQueue[T]
	closed         bool
	stopCh         chan struct{} // closed on Close to signal workers to drain and exit
	wg             sync.WaitGroup // tracks goroutine lifetime (for Close)
	jobsWg         sync.WaitGroup // tracks in-flight jobs (for WaitIdle)
	onPublishError asynxmd.PublishErrorHandler[T]
	idleTimeout    time.Duration
}

type aggregateQueue[T any] struct {
	ch chan *dispatchJob[T]
}

type dispatchJob[T any] struct {
	event asynxmd.Event[T]
	ctx   context.Context
	done  chan struct{}
}

// New creates a Dispatcher that publishes events through bus.
func New[T any](bus asynxmd.Bus[T], opts ...Opt[T]) *Dispatcher[T] {
	d := &Dispatcher[T]{
		bus:         bus,
		queues:      make(map[string]*aggregateQueue[T]),
		stopCh:      make(chan struct{}),
		idleTimeout: defaultIdleTimeout,
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Dispatch enqueues an event for ordered delivery. The enqueue itself is
// synchronous (establishes ordering). If waitHandlers is true the call blocks
// until the event's handlers have completed.
func (d *Dispatcher[T]) Dispatch(ctx context.Context, event asynxmd.Event[T], waitHandlers bool) error {
	job := &dispatchJob[T]{
		event: event,
		ctx:   context.WithoutCancel(ctx),
		done:  make(chan struct{}),
	}

	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return asynxmd.ErrDispatcherClosed
	}

	q, exists := d.queues[event.AggregateID]
	if !exists {
		q = &aggregateQueue[T]{ch: make(chan *dispatchJob[T], defaultBufferSize)}
		d.queues[event.AggregateID] = q
		d.wg.Add(1)
		go d.worker(event.AggregateID, q)
	}

	d.jobsWg.Add(1)
	d.mu.Unlock()

	q.ch <- job

	if waitHandlers {
		<-job.done
	}
	return nil
}

// Close marks the dispatcher as closed, closes all per-aggregate channels, and
// waits for every worker to drain its queue. If ctx expires before all workers
// finish, the context error is returned.
func (d *Dispatcher[T]) Close(ctx context.Context) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	close(d.stopCh) // signal all workers to drain and exit
	d.mu.Unlock()

	done := make(chan struct{})
	go func() { d.wg.Wait(); close(done) }()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitIdle blocks until all in-flight jobs have been handled. The per-aggregate
// worker goroutines may still be alive (waiting for their idle timeout), but
// every dispatched event has been delivered to the bus.
func (d *Dispatcher[T]) WaitIdle() { d.jobsWg.Wait() }

// waitWorkers blocks until all per-aggregate worker goroutines have exited
// (i.e. after their idle timeout elapses). Only for use in tests.
func (d *Dispatcher[T]) waitWorkers() { d.wg.Wait() }

func (d *Dispatcher[T]) worker(aggregateID string, q *aggregateQueue[T]) {
	defer d.wg.Done()
	for {
		select {
		case job := <-q.ch:
			d.handle(job)
		case <-d.stopCh:
			// Drain remaining jobs before exiting.
			for {
				select {
				case job := <-q.ch:
					d.handle(job)
				default:
					return
				}
			}
		case <-time.After(d.idleTimeout):
			d.mu.Lock()
			if len(q.ch) > 0 {
				d.mu.Unlock()
				continue
			}
			delete(d.queues, aggregateID)
			d.mu.Unlock()
			return
		}
	}
}

// handle runs bus.PublishSync with panic recovery for resilience. A panicking
// handler will not crash the worker goroutine — subsequent events continue to
// be delivered.
func (d *Dispatcher[T]) handle(job *dispatchJob[T]) {
	defer d.jobsWg.Done()
	defer close(job.done)
	defer func() {
		if r := recover(); r != nil {
			// Worker survives — recovered defensively so subsequent events
			// still deliver.
		}
	}()
	if d.bus == nil {
		return
	}
	err := d.bus.PublishSync(job.ctx, job.event)
	if err != nil && d.onPublishError != nil {
		d.onPublishError(job.ctx, job.event, err)
	}
}
