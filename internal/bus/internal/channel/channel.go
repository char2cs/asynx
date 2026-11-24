package channel

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/char2cs/asynx/v2/internal/bus/internal/exec"
	"github.com/char2cs/asynx/v2/internal/bus/internal/routing"
	"github.com/char2cs/asynx/v2/internal/bus/models"
	"github.com/char2cs/asynx/v2/internal/ids"
	"github.com/char2cs/asynx/v2/models/event"
	"github.com/char2cs/asynx/v2/models/failure"
	"github.com/char2cs/asynx/v2/models/handler"
	"github.com/char2cs/asynx/v2/models/subscription"
)

// Bus is the in-process event bus: it routes each published event to every
// handler whose subscription pattern matches the event name and runs each
// handler on its own goroutine.
//
// Subscriptions are held in an immutable index swapped in atomically, so
// Publish never takes a lock on the subscription path; Subscribe and
// Unsubscribe serialise among themselves and build a new index each time.
// Patterns without regexp metacharacters match by equality, the rest are
// compiled once when they are subscribed and kept for the life of the bus (a
// pattern that does not compile never matches; its compile error is kept with
// it).
//
// Close stops new work and drains in-flight handlers; after it a bus rejects
// Subscribe, Publish and PublishSync with ErrBusClosed. A Bus is safe for
// concurrent use by any number of goroutines and must be built with New.
type Bus[T any] struct {
	subMu         sync.Mutex
	subscriptions map[string]*models.Subscription[T]
	idx           atomic.Pointer[models.PublishIndex[T]]

	publishMu  sync.RWMutex
	closed     bool
	inFlightWg sync.WaitGroup

	patternMu sync.RWMutex
	patterns  map[string]compiledPattern

	onPanic   handler.Panic[T]
	onTimeout handler.Timeout[T]
}

// New returns a Bus with no subscriptions, ready for use. opts adjust the
// callbacks invoked when a handler panics or times out; an option that leaves
// one unset selects the package-level default reporter for it. The returned bus
// owns no goroutines until something is published.
func New[T any](
	opts ...models.Opt[T],
) *Bus[T] {
	b := &Bus[T]{
		subscriptions: make(map[string]*models.Subscription[T]),
		patterns:      make(map[string]compiledPattern),
	}
	b.idx.Store(&models.PublishIndex[T]{
		ExactSubs: make(map[string][]*models.Subscription[T]),
	})
	var cfg models.Config[T]
	for _, opt := range opts {
		opt(&cfg)
	}

	b.onPanic = cfg.OnPanic
	b.onTimeout = cfg.OnTimeout
	return b
}

// Subscribe registers handler for every event whose name matches pattern and
// returns the id to pass to Unsubscribe. A pattern without regexp
// metacharacters matches the event name exactly; any other pattern is a regular
// expression matched anywhere in the name (anchor it to match the whole name).
// opts may set a per-handler timeout and a fallback handler.
//
// It returns ErrEmptyPattern for an empty pattern, ErrNilHandler for a nil
// handler and ErrBusClosed once the bus is closed. A pattern that is not a
// valid regular expression is accepted but never matches. A handler added
// while an event is being published sees that event only if the publish had not
// yet taken its snapshot of the subscriptions.
func (b *Bus[T]) Subscribe(
	pattern string,
	callback handler.Projection[T],
	opts ...subscription.Opt[T],
) (subID string, err error) {
	if pattern == "" {
		return "",
			failure.ErrEmptyPattern
	}

	if callback == nil {
		return "",
			failure.ErrNilHandler
	}

	cfg := &subscription.Config[T]{}
	for _, opt := range opts {
		opt(cfg)
	}

	b.subMu.Lock()
	defer b.subMu.Unlock()

	if b.isClosed() {
		return "",
			failure.ErrBusClosed
	}

	subID = ids.New()
	b.register(
		subID,
		pattern,
		callback,
		cfg,
	)

	return subID,
		nil
}

func (b *Bus[T]) isClosed() bool {
	b.publishMu.RLock()
	defer b.publishMu.RUnlock()

	return b.closed
}

func (b *Bus[T]) register(
	subID string,
	pattern string,
	callback handler.Projection[T],
	cfg *subscription.Config[T],
) {
	sub := &models.Subscription[T]{
		Pattern:         pattern,
		Handler:         callback,
		FallbackHandler: cfg.Fallback,
		Timeout:         cfg.Timeout,
	}

	if !routing.IsExactPattern(pattern) {
		b.rememberPattern(pattern)
	}

	b.subscriptions[subID] = sub
	b.idx.Store(routing.IndexWith(
		b.idx.Load(),
		sub,
	))
}

// Unsubscribe removes the subscription with the given id, so events published
// afterwards no longer reach its handler. Handlers already started for earlier
// events keep running to completion. An unknown or already removed id is not an
// error, which makes the call idempotent; the error result is always nil and
// exists to satisfy the Bus contract.
func (b *Bus[T]) Unsubscribe(
	id string,
) error {
	b.subMu.Lock()
	defer b.subMu.Unlock()

	sub, ok := b.subscriptions[id]
	if !ok {
		return nil
	}

	delete(
		b.subscriptions,
		id,
	)
	b.idx.Store(routing.IndexWithout(
		b.idx.Load(),
		sub,
	))

	return nil
}

// Publish starts every handler whose subscription matches event.EventName, each
// on its own goroutine, and returns without waiting for them. The context
// reaches each handler as given, so cancelling it is the handler's signal to
// stop; it does not abort handlers already started.
//
// It returns ctx.Err() when ctx is already done and ErrBusClosed once the bus is
// closed; in both cases no handler is started. Publishing an event nothing
// subscribes to succeeds. Handlers of one event run concurrently and their
// order is unspecified. Handler panics and timeouts never reach the caller.
// Close waits for the handlers Publish started.
func (b *Bus[T]) Publish(
	ctx context.Context,
	evt event.Event[T],
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	matched := b.matching(evt.EventName)
	err := b.reserve(
		&b.inFlightWg,
		len(matched),
	)
	if err != nil {
		return err
	}

	b.launch(
		ctx,
		&b.inFlightWg,
		evt,
		matched,
	)

	return nil
}

// PublishSync starts the matching handlers like Publish and blocks until every
// one of them has finished, including a handler that exceeded its timeout:
// the timeout is still reported, but the call keeps waiting for the handler. The
// dispatcher relies on this so that the next event of an aggregate never starts
// while the previous one is still being handled.
//
// Errors are those of Publish and are returned before anything is started.
// Handler panics and timeouts go to the panic and timeout handlers and do not
// affect the returned error. Calling PublishSync from inside a handler of the
// same bus is allowed, but waiting for a handler that is itself waiting on the
// caller deadlocks.
func (b *Bus[T]) PublishSync(
	ctx context.Context,
	evt event.Event[T],
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	matched := b.matching(evt.EventName)

	var localWg sync.WaitGroup
	err := b.reserve(
		&localWg,
		len(matched),
	)
	if err != nil {
		return err
	}

	b.launchSync(
		ctx,
		&localWg,
		evt,
		matched,
	)

	localWg.Wait()
	return nil
}

func (b *Bus[T]) matching(
	eventName string,
) []*models.Subscription[T] {
	idx := b.idx.Load()
	exact := idx.ExactSubs[eventName]

	return append(
		slices.Clone(exact),
		b.regexMatching(
			idx.RegexSubs,
			eventName,
		)...,
	)
}

func (b *Bus[T]) regexMatching(
	subs []*models.Subscription[T],
	eventName string,
) []*models.Subscription[T] {
	return slices.DeleteFunc(
		slices.Clone(subs),
		b.patternRejects(eventName),
	)
}

func (b *Bus[T]) patternRejects(
	eventName string,
) func(*models.Subscription[T]) bool {
	return func(
		sub *models.Subscription[T],
	) bool {
		return !b.matchesRegex(
			eventName,
			sub.Pattern,
		)
	}
}

func (b *Bus[T]) reserve(
	wg *sync.WaitGroup,
	total int,
) error {
	b.publishMu.RLock()
	defer b.publishMu.RUnlock()

	if b.closed {
		return failure.ErrBusClosed
	}

	if total > 0 {
		wg.Add(total)
	}

	return nil
}

func (b *Bus[T]) launch(
	ctx context.Context,
	wg *sync.WaitGroup,
	evt event.Event[T],
	subs []*models.Subscription[T],
) {
	for _, sub := range subs {
		go exec.Handler(
			ctx,
			wg,
			exec.MakeJob(
				sub,
				evt,
				b.onPanic,
				b.onTimeout,
			),
		)
	}
}

func (b *Bus[T]) launchSync(
	ctx context.Context,
	wg *sync.WaitGroup,
	evt event.Event[T],
	subs []*models.Subscription[T],
) {
	for _, sub := range subs {
		job := exec.MakeJob(
			sub,
			evt,
			b.onPanic,
			b.onTimeout,
		)
		job.WaitAfterTimeout = true
		go exec.Handler(
			ctx,
			wg,
			job,
		)
	}
}

// Close marks the bus closed, so every later Subscribe, Publish and PublishSync
// returns ErrBusClosed, and waits for the handlers already in flight. It
// returns nil once they have all finished and ctx.Err() if ctx ends first; the
// bus stays closed and the remaining handlers keep running in that case.
// Calling Close again is safe and waits again.
func (b *Bus[T]) Close(
	ctx context.Context,
) error {
	b.publishMu.Lock()
	b.closed = true
	b.publishMu.Unlock()

	done := make(chan struct{})
	go func() {
		b.inFlightWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Bus[T]) matchesRegex(
	eventName,
	pattern string,
) bool {
	b.patternMu.RLock()
	defer b.patternMu.RUnlock()

	return b.patterns[pattern].matches(eventName)
}

func (b *Bus[T]) rememberPattern(
	pattern string,
) {
	compiled := compilePattern(pattern)

	b.patternMu.Lock()
	defer b.patternMu.Unlock()

	b.patterns[pattern] = compiled
}

// WaitForHandlers blocks until every handler started by Publish has finished,
// without closing the bus or bounding the wait. Handlers started by PublishSync
// are not counted: that call waits for its own. It exists so tests and
// shutdown paths that cannot pass a context can drain the bus; code that can
// should use Close with a deadline instead.
func (b *Bus[T]) WaitForHandlers() {
	b.inFlightWg.Wait()
}
