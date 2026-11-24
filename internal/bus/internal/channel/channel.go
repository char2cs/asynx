package channel

import (
	"context"
	"regexp"
	"sync"
	"sync/atomic"

	"github.com/char2cs/asynx/internal/bus/internal/exec"
	"github.com/char2cs/asynx/internal/bus/internal/routing"
	"github.com/char2cs/asynx/internal/bus/models"
	"github.com/char2cs/asynx/internal/ids"
	asynxmd "github.com/char2cs/asynx/models"
)

// Bus is the in-process channel-based event bus.
type Bus[T any] struct {
	subMu         sync.Mutex
	subscriptions map[string]*models.Subscription[T]
	idx           atomic.Pointer[models.PublishIndex[T]]

	publishMu  sync.RWMutex
	closed     bool
	inFlightWg sync.WaitGroup

	patternMu        sync.RWMutex
	compiledPatterns map[string]*regexp.Regexp
	patternErrors    map[string]error

	onPanic   asynxmd.PanicHandler[T]
	onTimeout asynxmd.TimeoutHandler[T]
}

// New returns an initialized Bus ready for use.
func New[T any](
	opts ...models.Opt[T],
) *Bus[T] {
	b := &Bus[T]{
		subscriptions:    make(map[string]*models.Subscription[T]),
		compiledPatterns: make(map[string]*regexp.Regexp),
		patternErrors:    make(map[string]error),
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
// returns the subscription id.
func (b *Bus[T]) Subscribe(
	pattern string,
	handler asynxmd.ProjectionHandler[T],
	opts ...asynxmd.SubscriptionOpt[T],
) (subID string, err error) {
	if pattern == "" {
		return "",
			asynxmd.ErrEmptyPattern
	}

	if handler == nil {
		return "",
			asynxmd.ErrNilHandler
	}

	cfg := &asynxmd.SubscriptionConfig[T]{}
	for _, opt := range opts {
		opt(cfg)
	}

	b.subMu.Lock()
	defer b.subMu.Unlock()

	if b.isClosed() {
		return "",
			asynxmd.ErrBusClosed
	}

	return b.register(
			pattern,
			handler,
			cfg,
		),
		nil
}

func (b *Bus[T]) isClosed() bool {
	b.publishMu.RLock()
	defer b.publishMu.RUnlock()

	return b.closed
}

func (b *Bus[T]) register(
	pattern string,
	handler asynxmd.ProjectionHandler[T],
	cfg *asynxmd.SubscriptionConfig[T],
) string {
	subID := ids.New()
	sub := &models.Subscription[T]{
		Pattern:         pattern,
		Handler:         handler,
		FallbackHandler: cfg.Fallback,
		Timeout:         cfg.Timeout,
	}

	b.subscriptions[subID] = sub
	b.idx.Store(routing.IndexWith(
		b.idx.Load(),
		sub,
	))

	return subID
}

// Unsubscribe removes the subscription with the given id. An unknown id is a
// no-op.
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

// Publish fires all matching handlers asynchronously and returns once they
// are started.
func (b *Bus[T]) Publish(
	ctx context.Context,
	event asynxmd.Event[T],
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	matched := b.matching(event.EventName)
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
		event,
		matched,
	)

	return nil
}

// PublishSync fires all matching handlers and blocks until every handler
// goroutine triggered by this event has completed. Handler panics and timeouts
// are still handled by PanicHandler/TimeoutHandler — they do not affect the
// returned error.
func (b *Bus[T]) PublishSync(
	ctx context.Context,
	event asynxmd.Event[T],
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	matched := b.matching(event.EventName)

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
		event,
		matched,
	)

	localWg.Wait()
	return nil
}

func (b *Bus[T]) matching(
	eventName string,
) []*models.Subscription[T] {
	idx := b.idx.Load()
	matched := append(
		[]*models.Subscription[T](nil),
		idx.ExactSubs[eventName]...,
	)

	for _, sub := range idx.RegexSubs {
		matched = b.appendIfMatch(
			matched,
			eventName,
			sub,
		)
	}

	return matched
}

func (b *Bus[T]) appendIfMatch(
	matched []*models.Subscription[T],
	eventName string,
	sub *models.Subscription[T],
) []*models.Subscription[T] {
	if !b.matchesRegex(
		eventName,
		sub.Pattern,
	) {
		return matched
	}

	return append(
		matched,
		sub,
	)
}

func (b *Bus[T]) reserve(
	wg *sync.WaitGroup,
	total int,
) error {
	b.publishMu.RLock()
	defer b.publishMu.RUnlock()

	if b.closed {
		return asynxmd.ErrBusClosed
	}

	if total > 0 {
		wg.Add(total)
	}

	return nil
}

func (b *Bus[T]) launch(
	ctx context.Context,
	wg *sync.WaitGroup,
	event asynxmd.Event[T],
	subs []*models.Subscription[T],
) {
	for _, sub := range subs {
		go exec.Handler(
			ctx,
			wg,
			exec.MakeJob(
				sub,
				event,
				b.onPanic,
				b.onTimeout,
			),
		)
	}
}

func (b *Bus[T]) launchSync(
	ctx context.Context,
	wg *sync.WaitGroup,
	event asynxmd.Event[T],
	subs []*models.Subscription[T],
) {
	for _, sub := range subs {
		job := exec.MakeJob(
			sub,
			event,
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

// Close stops accepting new work and waits for in-flight handlers until ctx
// is done.
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
	compiled, err := b.getCompiledPattern(pattern)
	if err != nil {
		return false
	}

	return compiled.MatchString(eventName)
}

func (b *Bus[T]) getCompiledPattern(
	pattern string,
) (*regexp.Regexp, error) {
	b.patternMu.RLock()

	if compiled, ok := b.compiledPatterns[pattern]; ok {
		b.patternMu.RUnlock()
		return compiled,
			nil
	}

	if err, cached := b.patternErrors[pattern]; cached {
		b.patternMu.RUnlock()
		return nil,
			err
	}

	b.patternMu.RUnlock()

	compiled, err := regexp.Compile(pattern)
	if err != nil {
		b.patternMu.Lock()
		b.patternErrors[pattern] = err
		b.patternMu.Unlock()
		return nil,
			err
	}

	b.patternMu.Lock()
	defer b.patternMu.Unlock()

	if existing, ok := b.compiledPatterns[pattern]; ok {
		return existing,
			nil
	}

	b.compiledPatterns[pattern] = compiled
	return compiled,
		nil
}

// WaitForHandlers blocks until all in-flight handler executions complete.
// Only for use in tests; do not call in production code.
func (b *Bus[T]) WaitForHandlers() {
	b.inFlightWg.Wait()
}
