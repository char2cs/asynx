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

	b.publishMu.RLock()
	closed := b.closed
	b.publishMu.RUnlock()

	if closed {
		return "",
			asynxmd.ErrBusClosed
	}

	subID = ids.New()
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

	return subID,
		nil
}

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

func (b *Bus[T]) Publish(
	ctx context.Context,
	event asynxmd.Event[T],
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	idx := b.idx.Load()

	exactMatches := idx.ExactSubs[event.EventName]

	var regexMatches []*models.Subscription[T]
	for _, sub := range idx.RegexSubs {
		if b.matchesRegex(
			event.EventName,
			sub.Pattern,
		) {
			regexMatches = append(
				regexMatches,
				sub,
			)
		}
	}

	total := len(exactMatches) + len(regexMatches)

	b.publishMu.RLock()
	if b.closed {
		b.publishMu.RUnlock()
		return asynxmd.ErrBusClosed
	}

	if total > 0 {
		b.inFlightWg.Add(total)
	}

	b.publishMu.RUnlock()

	for _, sub := range exactMatches {
		go exec.Handler(
			&b.inFlightWg,
			exec.MakeJob(
				sub,
				event,
				ctx,
				b.onPanic,
				b.onTimeout,
			),
		)
	}

	for _, sub := range regexMatches {
		go exec.Handler(
			&b.inFlightWg,
			exec.MakeJob(
				sub,
				event,
				ctx,
				b.onPanic,
				b.onTimeout,
			),
		)
	}

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

	idx := b.idx.Load()

	exactMatches := idx.ExactSubs[event.EventName]

	var regexMatches []*models.Subscription[T]
	for _, sub := range idx.RegexSubs {
		if b.matchesRegex(
			event.EventName,
			sub.Pattern,
		) {
			regexMatches = append(
				regexMatches,
				sub,
			)
		}
	}

	total := len(exactMatches) + len(regexMatches)

	b.publishMu.RLock()
	if b.closed {
		b.publishMu.RUnlock()
		return asynxmd.ErrBusClosed
	}

	var localWg sync.WaitGroup
	if total > 0 {
		localWg.Add(total)
	}

	b.publishMu.RUnlock()

	syncJob := func(
		sub *models.Subscription[T],
	) *exec.HandlerJob[T] {
		job := exec.MakeJob(
			sub,
			event,
			ctx,
			b.onPanic,
			b.onTimeout,
		)
		job.WaitAfterTimeout = true
		return job
	}
	for _, sub := range exactMatches {
		go exec.Handler(
			&localWg,
			syncJob(sub),
		)
	}

	for _, sub := range regexMatches {
		go exec.Handler(
			&localWg,
			syncJob(sub),
		)
	}

	localWg.Wait()
	return nil
}

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
